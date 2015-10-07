/***********************************************************************************************************************
 *
 * Elasticsearch IMAP/Pop3 E-Mail Importer
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
package de.saly.elasticsearch.imap;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.mail.Flags.Flag;
import javax.mail.Folder;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Store;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import de.saly.elasticsearch.importer.imap.impl.IMAPImporter;
import de.saly.elasticsearch.importer.imap.support.IMAPUtils;
import de.saly.javamail.mock2.MockMailbox;

public abstract class AbstractIMAPRiverUnitTest {
    
    protected static final ObjectMapper MAPPER = new ObjectMapper();

    private static int SID;
    protected static final String EMAIL_SUBJECT = "SID";
    protected static final String EMAIL_TEXT = "This is a test e-mail.";
    protected static final String EMAIL_TO = "esimaprivertest@localhost.com";
    
    protected static final String USER_NAME = "es_imapriver_unittest";
    protected static final String EMAIL_USER_ADDRESS = USER_NAME+"@localhost";
    protected static final String USER_PASSWORD = USER_NAME;
    
    protected static final String USER_NAME2 = "es_imapriver_unittest2";
    protected static final String EMAIL_USER_ADDRESS2 = USER_NAME2+"@localhost";
    protected static final String USER_PASSWORD2 = USER_NAME2;
    
    protected static final String USER_NAME3 = "es_imapriver_unittest3";
    protected static final String EMAIL_USER_ADDRESS3 = USER_NAME3+"@localhost";
    protected static final String USER_PASSWORD3 = USER_NAME3;

    @Rule
    public TestName name = new TestName();
    protected Node esSetup;
    protected Node esSetup2;
    protected Node esSetup3;
    private IMAPImporter imapRiver;
    
    protected final ESLogger logger = ESLoggerFactory.getLogger(this.getClass().getName());

  

    protected AbstractIMAPRiverUnitTest() {
        super();

        System.setProperty("java.net.preferIPv4Stack","true");
 
    }
    
    
    protected Settings.Builder getSettingsBuilder(boolean dataNode, boolean masterNode) {
        
        boolean local = true;
        
        Settings.Builder builder = Settings
        .settingsBuilder()
        .put("path.home", ".")
        .put("cluster.name", "imapriver_testcluster")
        .put("index.store.fs.memory.enabled", "true")
        .put("path.data", "target/data").put("path.work", "target/work").put("path.logs", "target/logs")
        .put("path.conf", "target/config").put("plugin.types", "org.elasticsearch.plugin.mapper.attachments.MapperAttachmentsPlugin").put("index.number_of_shards", "5")
        .put("index.number_of_replicas", "0")
        .put("node.data", dataNode)
        .put("http.enabled", !local)
        .put("node.local", local)
        .put("http.cors.enabled", !local)
        .put("node.master", masterNode)
        .put(getProperties());
         return builder;
    }

    @Before
    public void setUp() throws Exception {

        System.out.println("--------------------- SETUP " + name.getMethodName() + " -------------------------------------");

        FileUtils.deleteQuietly(new File("target/data/"));
        
        MockMailbox.resetAll();

        // Instantiates a local node & client

        esSetup =  NodeBuilder.nodeBuilder().settings(getSettingsBuilder(false, true)).build().start();
        esSetup2 = NodeBuilder.nodeBuilder().settings(getSettingsBuilder(true, false)).build().start();
        esSetup3 = NodeBuilder.nodeBuilder().settings(getSettingsBuilder(false, true)).build().start();
        
        waitForGreenClusterState(esSetup.client());

    }

    @After
    public void tearDown() throws Exception {

        System.out.println("--------------------- TEARDOWN " + name.getMethodName() + " -------------------------------------");

        if(imapRiver != null) {
            imapRiver.close();
        }
        
        if (esSetup != null) {
            esSetup.close();
        }
        
        if (esSetup2 != null) {
            esSetup2.close();
        }
        
        if (esSetup3 != null) {
            esSetup3.close();
        }

        FileUtils.deleteQuietly(new File("target/data/"));
    }

    protected void checkStoreForTestConnection(final Store store) {
        if (!store.isConnected()) {
            IMAPUtils.close(store);
            throw new RuntimeException("Store not connected");
        }

        if (!store.getURLName().getUsername().toLowerCase().startsWith("es_imapriver_unittest")) {
            IMAPUtils.close(store);
            throw new RuntimeException("User " + store.getURLName().getUsername() + " belongs not to a valid test mail connection");
        }
    }

    protected void createInitialIMAPTestdata(final Properties props, final String user, final String password, final int count,
            final boolean deleteall) throws MessagingException {
        final Session session = Session.getInstance(props);
        final Store store = session.getStore();
        store.connect(user, password);
        checkStoreForTestConnection(store);
        final Folder root = store.getDefaultFolder();
        final Folder testroot = root.getFolder("ES-IMAP-RIVER-TESTS");
        final Folder testrootl2 = testroot.getFolder("Level(2!");

        if (deleteall) {

            deleteMailsFromUserMailbox(props, "INBOX", 0, -1, user, password);

            if (testroot.exists()) {
                testroot.delete(true);
            }

            final Folder testrootenamed = root.getFolder("renamed_from_ES-IMAP-RIVER-TESTS");
            if (testrootenamed.exists()) {
                testrootenamed.delete(true);
            }

        }

        if (!testroot.exists()) {

            testroot.create(Folder.HOLDS_FOLDERS & Folder.HOLDS_MESSAGES);
            testroot.open(Folder.READ_WRITE);

            testrootl2.create(Folder.HOLDS_FOLDERS & Folder.HOLDS_MESSAGES);
            testrootl2.open(Folder.READ_WRITE);

        }

        final Folder inbox = root.getFolder("INBOX");
        inbox.open(Folder.READ_WRITE);

        final Message[] msgs = new Message[count];

        for (int i = 0; i < count; i++) {
            final MimeMessage message = new MimeMessage(session);
            message.setFrom(new InternetAddress(EMAIL_TO));
            message.addRecipient(Message.RecipientType.TO, new InternetAddress(EMAIL_USER_ADDRESS));
            message.setSubject(EMAIL_SUBJECT + "::" + i);
            message.setText(EMAIL_TEXT + "::" + SID++);
            message.setSentDate(new Date());
            msgs[i] = message;

        }

        inbox.appendMessages(msgs);
        testroot.appendMessages(msgs);
        testrootl2.appendMessages(msgs);

        IMAPUtils.close(inbox);
        IMAPUtils.close(testrootl2);
        IMAPUtils.close(testroot);
        IMAPUtils.close(store);

    }

    protected void deleteMailsFromUserMailbox(final Properties props, final String folderName, final int start, final int deleteCount,
            final String user, final String password) throws MessagingException {
        final Store store = Session.getInstance(props).getStore();

        store.connect(user, password);
        checkStoreForTestConnection(store);
        final Folder f = store.getFolder(folderName);
        f.open(Folder.READ_WRITE);

        final int msgCount = f.getMessageCount();

        final Message[] m = deleteCount == -1 ? f.getMessages() : f.getMessages(start, Math.min(msgCount, deleteCount + start - 1));
        int d = 0;

        for (final Message message : m) {
            message.setFlag(Flag.DELETED, true);
            logger.info("Delete msgnum: {} with sid {}", message.getMessageNumber(), message.getSubject());
            d++;
        }

        f.close(true);
        logger.info("Deleted " + d + " messages");
        store.close();

    }

    protected long getCount(final String index, final String type) {
        logger.debug("getCount()");

        esSetup.client().admin().indices().refresh(new RefreshRequest()).actionGet();

        final CountResponse count = esSetup.client().count(new CountRequest(index).types(type)).actionGet();

        return count.getCount();
    }
    
    protected long getCount(final List<String> indices, final String type) {
        logger.debug("getCount() for index {} and type", indices, type);
        
        esSetup.client().admin().indices().refresh(new RefreshRequest()).actionGet();

        long count = 0;
        
        for (Iterator<String> iterator = indices.iterator(); iterator.hasNext();) {
            String index = (String) iterator.next();
             long lcount = esSetup.client().count(new CountRequest(index).types(type)).actionGet().getCount();
             logger.debug("Count for index {} (type {}) is {}", index, type, lcount);
             count += lcount;
        }

        return count;
    }

    protected Properties getProperties() {
        return new Properties();
    }

    protected String loadFile(final String file) throws IOException {

        final StringWriter sw = new StringWriter();
        IOUtils.copy(this.getClass().getResourceAsStream("/" + file), sw);
        return sw.toString();

    }

    protected void putMailInMailbox(final int messages) throws MessagingException {

        for (int i = 0; i < messages; i++) {
            final MimeMessage message = new MimeMessage((Session) null);
            message.setFrom(new InternetAddress(EMAIL_TO));
            message.addRecipient(Message.RecipientType.TO, new InternetAddress(EMAIL_USER_ADDRESS));
            message.setSubject(EMAIL_SUBJECT + "::" + i);
            message.setText(EMAIL_TEXT + "::" + SID++);
            message.setSentDate(new Date());
            MockMailbox.get(EMAIL_USER_ADDRESS).getInbox().add(message);
        }
        
        logger.info("Putted " + messages + " into mailbox "+EMAIL_USER_ADDRESS);
    }
    
    protected void putMailInMailbox2(final int messages) throws MessagingException {

        for (int i = 0; i < messages; i++) {
            final MimeMessage message = new MimeMessage((Session) null);
            message.setFrom(new InternetAddress(EMAIL_TO));
            message.addRecipient(Message.RecipientType.TO, new InternetAddress(EMAIL_USER_ADDRESS2));
            message.setSubject(EMAIL_SUBJECT + "::" + i);
            message.setText(EMAIL_TEXT + "::" + SID++);
            message.setSentDate(new Date());
            MockMailbox.get(EMAIL_USER_ADDRESS2).getInbox().add(message);
        }
        
        logger.info("Putted " + messages + " into mailbox "+EMAIL_USER_ADDRESS2);
    }
    
    protected void putMailInMailbox3(final int messages) throws MessagingException {

        for (int i = 0; i < messages; i++) {
            final MimeMessage message = new MimeMessage((Session) null);
            message.setFrom(new InternetAddress(EMAIL_TO));
            message.addRecipient(Message.RecipientType.TO, new InternetAddress(EMAIL_USER_ADDRESS3));
            message.setSubject(EMAIL_SUBJECT + "::" + i);
            message.setText(EMAIL_TEXT + "::" + SID++);
            message.setSentDate(new Date());
            MockMailbox.get(EMAIL_USER_ADDRESS3).getInbox().add(message);
        }
        
        logger.info("Putted " + messages + " into mailbox "+EMAIL_USER_ADDRESS3);
    }

    @SuppressWarnings("unchecked")
    protected void registerRiver(final String typename, final String file) throws ElasticsearchException, IOException {
        imapRiver = new IMAPImporter(MAPPER.readValue(loadFile(file), Map.class), esSetup.client());
        imapRiver.start();
    }
    
    protected SearchHit statusRiver(final String index) throws ElasticsearchException, IOException {
        SearchResponse res = esSetup.client().prepareSearch(index).setTypes("imapriverstate").execute().actionGet();
        if(res.getHits() != null && res.getHits().getHits() != null && res.getHits().getHits().length > 0) {
            return res.getHits().getHits()[0];
        }
        
        return null;
        
        
    }

    protected void renameMailbox(final Properties props, final String folderName, final String user, final String password)
            throws MessagingException {
        final Store store = Session.getInstance(props).getStore();

        store.connect(user, password);
        checkStoreForTestConnection(store);
        final Folder f = store.getFolder(folderName);

        f.renameTo(store.getFolder("renamed_from_" + folderName));

        logger.info("Renamed " + f.getFullName());
        store.close();

    }

    @SuppressWarnings("unchecked")
    protected Map<String, Object> settings(final String resource) throws IOException {
        final InputStream in = this.getClass().getResourceAsStream(resource);
        return MAPPER.readValue(in, Map.class);
    }
    
    protected void waitForGreenClusterState(final Client client) throws IOException {
        waitForCluster(ClusterHealthStatus.GREEN, TimeValue.timeValueSeconds(30), client);
    }

    protected void waitForCluster(final ClusterHealthStatus status, final TimeValue timeout, final Client client) throws IOException {
        try {
            logger.debug("waiting for cluster state {}", status.name());
            final ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForStatus(status)
                    .setTimeout(timeout).execute().actionGet();
            if (healthResponse.isTimedOut()) {
                throw new IOException("cluster state is " + healthResponse.getStatus().name() + " and not " + status.name()
                        + ", cowardly refusing to continue with operations");
            } else {
                logger.debug("... cluster state ok");
            }
        } catch (final ElasticsearchTimeoutException e) {
            throw new IOException("timeout, cluster does not respond to health request, cowardly refusing to continue with operations");
        }
    }

}
