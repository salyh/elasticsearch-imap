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
package de.saly.elasticsearch.river.imap;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import javax.mail.MessagingException;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;

import de.saly.elasticsearch.maildestination.ElasticsearchBulkMailDestination;
import de.saly.elasticsearch.maildestination.MailDestination;
import de.saly.elasticsearch.mailsource.MailSource;
import de.saly.elasticsearch.mailsource.ParallelPollingIMAPMailSource;
import de.saly.elasticsearch.mailsource.ParallelPollingPOPMailSource;
import de.saly.elasticsearch.riverstate.ElasticsearchRiverStateManager;
import de.saly.elasticsearch.riverstate.RiverStateManager;
import de.saly.elasticsearch.support.MailFlowJob;

public class IMAPRiver extends AbstractRiverComponent implements River {

    public final static String NAME = "river-imap";

    public final static String TYPE = "imap";

    private volatile boolean closed;

    private final String folderPattern;

    private final String indexName;

    private final TimeValue interval;

    private final ESLogger logger = ESLoggerFactory.getLogger(IMAPRiver.class.getName());

    private final MailDestination mailDestination;

    private final MailSource mailSource;

    private final String password;

    private final Properties props = new Properties();

    private final RiverStateManager riverStateManager;

    private Scheduler sched;

    private final String schedule;

    private final String typeName;

    private final String user;

    @Inject
    public IMAPRiver(final RiverName riverName, final RiverSettings riverSettings, final Client client) {
        super(riverName, riverSettings);

        final Map<String, Object> imapSettings = settings.settings();

        user = XContentMapValues.nodeStringValue(imapSettings.get("user"), null);
        password = XContentMapValues.nodeStringValue(imapSettings.get("password"), null);

        folderPattern = XContentMapValues.nodeStringValue(imapSettings.get("folderpattern"), null);

        indexName = XContentMapValues.nodeStringValue(imapSettings.get("mail_index_name"), "imapriverdata");
        typeName = XContentMapValues.nodeStringValue(imapSettings.get("mail_type_name"), "mail");

        schedule = imapSettings.containsKey("schedule") ? XContentMapValues.nodeStringValue(imapSettings.get("schedule"), null) : null;

        interval = XContentMapValues.nodeTimeValue(imapSettings.get("interval"), TimeValue.timeValueMinutes(1));

        final int bulkSize = XContentMapValues.nodeIntegerValue(imapSettings.get("bulk_size"), 100);
        final int maxBulkRequests = XContentMapValues.nodeIntegerValue(imapSettings.get("max_bulk_requests"), 30);
        // flush interval for bulk indexer
        final TimeValue flushInterval = XContentMapValues.nodeTimeValue(imapSettings.get("bulk_flush_interval"),
                TimeValue.timeValueSeconds(5));

        final int threads = XContentMapValues.nodeIntegerValue(imapSettings.get("threads"), 5);

        final boolean withTextContent = XContentMapValues.nodeBooleanValue(imapSettings.get("with_text_content"), true);

        final boolean withAttachments = XContentMapValues.nodeBooleanValue(imapSettings.get("with_attachments"), false);

        final boolean stripTagsFromTextContent = XContentMapValues.nodeBooleanValue(imapSettings.get("with_striptags_from_textcontent"),
                true);

        // get two maps from the river settings to improve index creation
        final Map<String, Object> indexSettings = imapSettings.containsKey("index_settings") ? XContentMapValues.nodeMapValue(
                imapSettings.get("index_settings"), null) : null;
        final Map<String, Object> typeMapping = imapSettings.containsKey("type_mapping") ? XContentMapValues.nodeMapValue(
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

        mailDestination = new ElasticsearchBulkMailDestination().maxBulkActions(bulkSize).maxConcurrentBulkRequests(maxBulkRequests)
                .flushInterval(flushInterval).client(client).setMapping(typeMapping).setSettings(indexSettings).setType(typeName)
                .setIndex(indexName).setWithAttachments(withAttachments).setWithTextContent(withTextContent)
                .setStripTagsFromTextContent(stripTagsFromTextContent);

        riverStateManager = new ElasticsearchRiverStateManager().client(client).index(indexName);

        if (props.getProperty("mail.store.protocol").toLowerCase().contains("imap")) {
            mailSource = new ParallelPollingIMAPMailSource(props, threads, user, password);
        } else {
            mailSource = new ParallelPollingPOPMailSource(props, threads, user, password);
        }

        mailSource.setMailDestination(mailDestination);
        mailSource.setStateManager(riverStateManager);
        logger.info("IMAPRiver created, river name: {}", riverName.getName());
    }

    @Override
    public void close() {

        if (closed) {
            return;
        }
        logger.info("Closing IMAPRiver ...");

        closed = true;

        try {
            if (sched != null && sched.isStarted()) {

                sched.shutdown(true);

            }
        } catch (final SchedulerException e) {
            logger.warn("Unable to shutdown scheduler due to " + e, e);

        }

        if (mailSource != null) {
            mailSource.close();
        }

        if (mailDestination != null) {
            mailDestination.close();
        }

        logger.info("IMAPRiver closed");
    }

    public String getIndexName() {
        return indexName;
    }

    public String getTypeName() {
        return typeName;
    }

    public void once() throws MessagingException, IOException {

        mailDestination.startup();

        logger.debug("once() start");
        final MailFlowJob mfj = new MailFlowJob();
        mfj.setPattern(folderPattern == null ? null : Pattern.compile(folderPattern));
        mfj.setMailSource(mailSource);
        mfj.execute();
        logger.debug("once() end");
    }

    @Override
    public void start() {
        logger.info("Start IMAPRiver ...");

        try {

            mailDestination.startup();

            sched = StdSchedulerFactory.getDefaultScheduler();

            if (sched.isShutdown()) {
                logger.error("Scheduler already down");
            }
            if (sched.isStarted()) {
                logger.error("Scheduler already started");
            }

            final JobDataMap jdm = new JobDataMap();
            jdm.put("mailSource", mailSource);

            if (folderPattern != null) {
                jdm.put("pattern", Pattern.compile(folderPattern));
            }

            final JobDetail job = newJob(MailFlowJob.class).withIdentity(riverName + "-" + props.hashCode(), "group1").usingJobData(jdm)
                    .build();

            Trigger trigger = null;

            if (StringUtils.isEmpty(schedule)) {
                logger.info("Trigger interval is every {} seconds", interval.seconds());

                trigger = newTrigger().withIdentity("intervaltrigger", "group1").startNow()
                        .withSchedule(simpleSchedule().withIntervalInSeconds((int) interval.seconds()).repeatForever()).build();
            } else {

                logger.info("Trigger follows cron pattern {}", schedule);

                trigger = newTrigger().withIdentity("crontrigger", "group1").withSchedule(cronSchedule(schedule)).build();
            }

            sched.scheduleJob(job, trigger);
            sched.start();

        } catch (final Exception e) {
            logger.error("Unable to start IMAPRiver due to " + e, e);
        }

        logger.info("IMAPRiver started");
    }
}
