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
package de.saly.elasticsearch.imap;

import static com.github.tlrx.elasticsearch.test.EsSetup.deleteAll;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Date;
import java.util.Properties;

import javax.mail.Flags.Flag;
import javax.mail.Folder;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Store;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.river.RiverSettings;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import com.github.tlrx.elasticsearch.test.EsSetup;

import de.saly.elasticsearch.support.IMAPUtils;
import de.saly.javamail.mock2.MockMailbox;

public abstract class AbstractIMAPRiverUnitTest {

    private static int SID;
    protected static final String EMAIL_SUBJECT = "SID";
    protected static final String EMAIL_TEXT = "This is a test e-mail.";
    protected static final String EMAIL_TO = "esimaprivertest@localhost.com";
    protected static final String EMAIL_USER_ADDRESS = "es_imapriver_unittest@localhost";
    protected static final String USER_NAME = "es_imapriver_unittest";
    protected static final String USER_PASSWORD = USER_NAME;

    @Rule
    public TestName name = new TestName();
    protected EsSetup esSetup;

    protected final ESLogger logger = ESLoggerFactory.getLogger(this.getClass().getName());

    protected final Builder settingsBuilder;

    protected AbstractIMAPRiverUnitTest() {
        super();

        settingsBuilder = ImmutableSettings
                .settingsBuilder()
                // .put(NODE_NAME, elasticsearchNode.name())
                // .put("node.data", elasticsearchNode.data())
                // .put("cluster.name", elasticsearchNode.clusterName())
                .put("index.store.type", "memory").put("index.store.fs.memory.enabled", "true").put("gateway.type", "none")
                .put("path.data", "target/data").put("path.work", "target/work").put("path.logs", "target/logs")
                .put("path.conf", "target/config").put("path.plugins", "target/plugins").put("index.number_of_shards", "1")
                .put("index.number_of_replicas", "0").put(getProperties());

    }

    @Before
    public void setUp() throws Exception {

        System.out.println("--------------------- SETUP " + name.getMethodName() + " -------------------------------------");

        MockMailbox.resetAll();

        // Instantiates a local node & client

        esSetup = new EsSetup(settingsBuilder.build());

        // Clean all, and creates some indices

        esSetup.execute(

        deleteAll()

        );

    }

    @After
    public void tearDown() throws Exception {

        System.out.println("--------------------- TEARDOWN " + name.getMethodName() + " -------------------------------------");

        if (esSetup != null) {
            esSetup.terminate();
        }

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

        logger.info("Putted " + messages + " into mailbox");
    }

    protected void registerRiver(final String typename, final String file) throws ElasticsearchException, IOException {
        final IndexResponse res = esSetup.client().prepareIndex().setIndex("_river").setType(typename).setId("_meta")
                .setSource(loadFile(file)).execute().actionGet();
        if (!res.isCreated()) {
            throw new IOException("Unable to register river");
        }
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

    protected RiverSettings riverSettings(final String resource) throws IOException {
        final InputStream in = this.getClass().getResourceAsStream(resource);
        return new RiverSettings(ImmutableSettings.settingsBuilder().build(), XContentHelper.convertToMap(Streams.copyToByteArray(in),
                false).v2());
    }

}
