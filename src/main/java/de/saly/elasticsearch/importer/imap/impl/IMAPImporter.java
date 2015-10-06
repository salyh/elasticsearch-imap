/***********************************************************************************************************************
 *
 * Elasticsearch IMAP River - open source IMAP river for Elasticsearch
 * ==========================================
 *
 * Copyright (C) 2014 by Hendrik Saly (http://saly.de) and others.
 * 
 * Contains (partially) copied code from Jörg Prante's Elasticsearch JDBC river (https://github.com/jprante/elasticsearch-river-jdbc)
 *
 ***********************************************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 ***********************************************************************************************************************
 *
 * $Id:$
 *
 **********************************************************************************************************************/
package de.saly.elasticsearch.importer.imap.impl;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.mail.MessagingException;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;

import de.saly.elasticsearch.importer.imap.ldap.ILoginSource;
import de.saly.elasticsearch.importer.imap.ldap.LdapLoginSource;
import de.saly.elasticsearch.importer.imap.maildestination.ElasticsearchBulkMailDestination;
import de.saly.elasticsearch.importer.imap.maildestination.MailDestination;
import de.saly.elasticsearch.importer.imap.mailsource.MailSource;
import de.saly.elasticsearch.importer.imap.mailsource.ParallelPollingIMAPMailSource;
import de.saly.elasticsearch.importer.imap.mailsource.ParallelPollingPOPMailSource;
import de.saly.elasticsearch.importer.imap.state.ElasticsearchStateManager;
import de.saly.elasticsearch.importer.imap.state.StateManager;
import de.saly.elasticsearch.importer.imap.support.MailFlowJob;

public class IMAPImporter {

    private volatile boolean closed;

    private final String folderPattern;

    private final String indexName;
    
    private final String indexNameStrategy;

    private final TimeValue interval;

    private static final ESLogger logger = ESLoggerFactory.getLogger(IMAPImporter.class.getName());

    private final List<MailSource> mailSources = new ArrayList<MailSource>();

    private final List<String> passwords = new ArrayList<String>();

    private final Properties props = new Properties();

    private final Client client;

    private Scheduler sched;

    private final String schedule;

    private final String typeName;

    private final List<String> indices = new ArrayList<String>();
    
    private final List<String> users = new ArrayList<String>();

    private final List<String> headersToFields;

    public IMAPImporter(final Map<String, Object> imapSettings, final Client client) {
        
        this.client = client;

        getUserLogins(imapSettings);

        folderPattern = XContentMapValues.nodeStringValue(imapSettings.get("folderpattern"), null);

        indexName = XContentMapValues.nodeStringValue(imapSettings.get("mail_index_name"), "imapriverdata");
        
        indexNameStrategy = XContentMapValues.nodeStringValue(imapSettings.get("mail_index_name_strategy"), "all_in_one");
        
        typeName = XContentMapValues.nodeStringValue(imapSettings.get("mail_type_name"), "mail");

        schedule = imapSettings.containsKey("schedule") ? XContentMapValues.nodeStringValue(imapSettings.get("schedule"), null) : null;

        interval = XContentMapValues.nodeTimeValue(imapSettings.get("interval"), TimeValue.timeValueMinutes(1));

        headersToFields = arrayNodeToList(imapSettings.get("headers_to_fields"));

        final int bulkSize = XContentMapValues.nodeIntegerValue(imapSettings.get("bulk_size"), 100);
        final int maxBulkRequests = XContentMapValues.nodeIntegerValue(imapSettings.get("max_bulk_requests"), 30);
        // flush interval for bulk indexer
        final TimeValue flushInterval = XContentMapValues.nodeTimeValue(imapSettings.get("bulk_flush_interval"),
                TimeValue.timeValueSeconds(5));

        final int threads = XContentMapValues.nodeIntegerValue(imapSettings.get("threads"), 5);

        final boolean withTextContent = XContentMapValues.nodeBooleanValue(imapSettings.get("with_text_content"), true);

        final boolean withHtmlContent = XContentMapValues.nodeBooleanValue(imapSettings.get("with_html_content"), false);

        final boolean preferHtmlContent = XContentMapValues.nodeBooleanValue(imapSettings.get("prefer_html_content"), false);

        final boolean withFlagSync = XContentMapValues.nodeBooleanValue(imapSettings.get("with_flag_sync"), true);

        final boolean withAttachments = XContentMapValues.nodeBooleanValue(imapSettings.get("with_attachments"), false);

        final boolean stripTagsFromTextContent = XContentMapValues.nodeBooleanValue(imapSettings.get("with_striptags_from_textcontent"),
                true);
        
        final boolean keepExpungedMessages = XContentMapValues.nodeBooleanValue(imapSettings.get("keep_expunged_messages"), false);

        // get two maps from the river settings to improve index creation
        final Map<String, Object> indexSettings = imapSettings.get("index_settings") != null ? XContentMapValues.nodeMapValue(
                imapSettings.get("index_settings"), null) : null;

        final Map<String, Object> typeMapping = imapSettings.get("type_mapping") != null ? XContentMapValues.nodeMapValue(
                imapSettings.get("type_mapping"), null) : null;

        for (final Map.Entry<String, Object> entry : imapSettings.entrySet()) {

            if (entry != null && entry.getKey().startsWith("mail.")) {
                props.setProperty(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }

        if (StringUtils.isEmpty(props.getProperty("mail.store.protocol"))) {
            logger.warn("mail.store.protocol not set, assume its 'imaps'");
            props.setProperty("mail.store.protocol", "imaps");
        }

        logger.debug("river settings " + imapSettings);
        logger.debug("mail settings " + props);

        for(int i=0; i<users.size();i++) {
        
            String user = users.get(i);
            String password = passwords.get(i);
            
            String _indexName = null;
            
            if("all_in_one".equalsIgnoreCase(indexNameStrategy)) {
                
                _indexName = indexName;
            } else if("username".equalsIgnoreCase(indexNameStrategy)) {
                _indexName = user;
            } else if("username_crop".equalsIgnoreCase(indexNameStrategy)) {
                _indexName = user.split("@")[0];
            }else if("prefixed_username".equalsIgnoreCase(indexNameStrategy)) {
                _indexName = indexName+"-"+user;
            }else if("prefixed_username_crop".equalsIgnoreCase(indexNameStrategy)) {
                _indexName = indexName+"-"+user.split("@")[0];
            }
            
            StateManager riverStateManager = new ElasticsearchStateManager().client(client).index(_indexName);
            
            MailSource mailSource = null;
            
            MailDestination mailDestination = new ElasticsearchBulkMailDestination().maxBulkActions(bulkSize).maxConcurrentBulkRequests(maxBulkRequests)
                    .flushInterval(flushInterval).client(client).setMapping(typeMapping).setSettings(indexSettings).setType(typeName) //+user???
                    .setIndex(_indexName).setWithAttachments(withAttachments).setWithTextContent(withTextContent).setWithHtmlContent(withHtmlContent)
                    .setPreferHtmlContent(preferHtmlContent).setStripTagsFromTextContent(stripTagsFromTextContent).setHeadersToFields(headersToFields);

            if (props.getProperty("mail.store.protocol").toLowerCase().contains("imap")) {
                mailSource = new ParallelPollingIMAPMailSource(props, threads, user, password).setWithFlagSync(withFlagSync);
            } else {
                mailSource = new ParallelPollingPOPMailSource(props, threads, user, password);
            }
    
            mailSource.setDeleteExpungedMessages(!keepExpungedMessages);
            mailSource.setMailDestination(mailDestination);
            mailSource.setStateManager(riverStateManager);
            mailSources.add(mailSource);
            indices.add(_indexName);
        }
        logger.info("IMAPRiver created");
    }

    public void close() {

        if (closed) {
            return;
        }
        logger.info("Closing IMAPRiver ...");

        closed = true;

        try {
            if (sched != null && sched.isStarted()) {
                sched.shutdown();
                logger.info("Scheduler shutted down");
            }
        } catch (final SchedulerException e) {
            logger.warn("Unable to shutdown scheduler due to " + e, e);

        }
        
        for(int i=0;i<mailSources.size();i++) {
                    
            MailSource mailSource = mailSources.get(i);
            mailSource.getMailDestination().close();
            mailSource.close();
                    
        }

        logger.info("IMAPRiver closed");
    }

    public List<String> getIndexNames() {
        return Collections.unmodifiableList(indices);
    }

    public String getIndexNameStrategy() {
        return indexNameStrategy;
    }

    public String getTypeName() {
        return typeName;
    }

    public void once() throws MessagingException, IOException {

        for(int i=0;i<mailSources.size();i++) {
            
            MailSource mailSource = mailSources.get(i);
            mailSource.getMailDestination().startup();
            logger.debug("once() start");
            final MailFlowJob mfj = new MailFlowJob();
            try {
                mfj.setPattern(folderPattern == null ? null : Pattern.compile(folderPattern));
            } catch (final PatternSyntaxException e) {
                logger.error("folderpattern is invalid due to {}", e, e.toString());
            }
            mfj.setMailSource(mailSource);
            mfj.execute();
            logger.debug("once() end");
        }
    }

    public void start() {
        logger.info("Start IMAPRiver ...");

        try {

            sched = StdSchedulerFactory.getDefaultScheduler();

            if (sched.isShutdown()) {
                throw new Exception("Scheduler already down");
            }
            if (sched.isStarted()) {
                logger.debug("Scheduler already running");
            }

            
            for(int i=0;i<mailSources.size();i++) {
            
                MailSource mailSource = mailSources.get(i);
                mailSource.getMailDestination().startup();
                final JobDataMap jdm = new JobDataMap();
                jdm.put("mailSource", mailSource);
                jdm.put("client", client);
                
                try {
                    if (folderPattern != null) {
                        jdm.put("pattern", Pattern.compile(folderPattern));
                    }
                } catch (final PatternSyntaxException e) {
                    throw new Exception("folderpattern is invalid due to " + e, e);
                }
    
                final JobDetail job = newJob(MailFlowJob.class)
                        .usingJobData(jdm)
                        .build();
    
                Trigger trigger = null;
    
                if (StringUtils.isEmpty(schedule)) {
                    logger.info("Trigger interval is every {} seconds", interval.seconds());
    
                    trigger = newTrigger()
                            .startNow()
                            .withSchedule(simpleSchedule().withIntervalInSeconds((int) interval.seconds()).repeatForever()).build();
                } else {
    
                    logger.info("Trigger follows cron pattern {}", schedule);
    
                    trigger = newTrigger()
                            .withSchedule(cronSchedule(schedule)).build();
                }
    
                sched.scheduleJob(job, trigger);
            }

            sched.start();
            logger.info("IMAPRiver started");

        } catch (final Exception e) {
            logger.error("Unable to start IMAPRiver due to " + e, e);
        }

    }

    @SuppressWarnings("unchecked")
    private List<String> arrayNodeToList(Object arrayNode) {
        ArrayList<String> list = new ArrayList<>();
        if(XContentMapValues.isArray(arrayNode)) {
            for(Object node : (List<Object>) arrayNode) {
                String value = XContentMapValues.nodeStringValue(node, null);
                if(value != null) {
                    list.add(value);
                }
            }
        }
        return list;
    }
    
    private void getUserLogins(final Map<String, Object> imapSettings) {
        String userSource = XContentMapValues.nodeStringValue(imapSettings.get("user_source"), null);
        ILoginSource source = null;

        if ("ldap".equals(userSource)) {
            //master user credentials for Dovecot
            String masterUser = XContentMapValues.nodeStringValue(imapSettings.get("master_user"), null);
            String masterPassword = XContentMapValues.nodeStringValue(imapSettings.get("master_password"), null);
            source = new LdapLoginSource(imapSettings, masterUser, masterPassword);
        } else {
            //read logins directly
            String _user = XContentMapValues.nodeStringValue(imapSettings.get("user"), null);
            String _password = XContentMapValues.nodeStringValue(imapSettings.get("password"), null);

            if (_user != null && !_user.isEmpty()) {
                users.add(_user);
                passwords.add(_password);
            }

            List<String> _users = arrayNodeToList(imapSettings.get("users"));
            List<String> _passwords = arrayNodeToList(imapSettings.get("passwords"));

            //TODO: inject master user credentials?
            if (_users != null && !_users.isEmpty()) {
                users.addAll(_users);
                passwords.addAll(_passwords);
            }
        }

        //read from generic source
        if (source != null) {
            users.addAll(source.getUserNames());
            passwords.addAll(source.getUserPasswords());
        }
    }
    
    public static void waitForYellowCluster(Client client) throws IOException {

        ClusterHealthStatus status = ClusterHealthStatus.YELLOW;
        
        try {
            logger.debug("waiting for cluster state {}", status.name());
            final ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForStatus(status)
                    .setTimeout(TimeValue.timeValueSeconds(30)).execute().actionGet();
            if (healthResponse.isTimedOut()) {
                logger.error("Timeout while waiting for cluster state: {}, current cluster state is: {}", status.name(), healthResponse.getStatus().name());
                throw new IOException("cluster state is " + healthResponse.getStatus().name() + " and not " + status.name()
                       + ", cowardly refusing to continue with operations");
            } else {
                logger.debug("... cluster state ok");
            }
        } catch (final Exception e) {
            logger.error("Exception while waiting for cluster state: {} due to ", e, status.name(), e.toString());
            throw new IOException("timeout, cluster does not respond to health request, cowardly refusing to continue with operations", e);
        }
    }
}
