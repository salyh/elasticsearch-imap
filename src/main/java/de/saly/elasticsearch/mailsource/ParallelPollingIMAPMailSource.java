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
package de.saly.elasticsearch.mailsource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import javax.mail.Folder;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Store;
import javax.mail.UIDFolder;
import javax.mail.internet.MimeMessage;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.indices.IndexMissingException;

import de.saly.elasticsearch.maildestination.MailDestination;
import de.saly.elasticsearch.riverstate.RiverState;
import de.saly.elasticsearch.riverstate.RiverStateManager;
import de.saly.elasticsearch.support.IMAPUtils;

public class ParallelPollingIMAPMailSource implements MailSource {

    private final ExecutorService es;
    private MailDestination mailDestination;
    private final String password;
    private final Properties props;
    private RiverStateManager stateManager;
    private final int threadCount;
    private final String user;
    private boolean withFlagSync = true;
    protected final ESLogger logger = ESLoggerFactory.getLogger(this.getClass().getName());

    public ParallelPollingIMAPMailSource(final Properties props, final int threadCount, final String user, final String password) {
        super();
        this.props = props;
        this.threadCount = threadCount < 1 ? 1 : threadCount;
        this.es = Executors.newFixedThreadPool(threadCount);

        this.user = user;
        this.password = password;
    }

    @Override
    public void close() {
        if (es != null) {

            logger.info("Initiate shutdown");
            es.shutdown();
            /*try {
                if (es.awaitTermination(2, TimeUnit.SECONDS)) {
                    logger.info("Shutdown completed gracefully");
                } else {
                    logger.warn("Shutdown completed not gracefully, timeout elapsed");
                }
            } catch (final InterruptedException e) {
                logger.warn("Shutdown completed not gracefully, thread interrupted");
            }*/
        }
    }

    @Override
    public void fetch(final Pattern pattern) throws MessagingException, IOException {

        fetch(pattern, null);

    }

    @Override
    public void fetch(final String folderName) throws MessagingException, IOException {

        fetch(null, folderName);

    }

    @Override
    public void fetchAll() throws MessagingException, IOException {

        fetch(null, null);
    }

    public MailDestination getMailDestination() {
        return mailDestination;
    }

    public Properties getProps() {
        return props;
    }

    public RiverStateManager getStateManager() {
        return stateManager;
    }

    public int getThreadCount() {
        return threadCount;
    }

    @Override
    public void setMailDestination(final MailDestination mailDestination) {
        this.mailDestination = mailDestination;

    }

    @Override
    public void setStateManager(final RiverStateManager stateManager) {
        this.stateManager = stateManager;
    }

    public ParallelPollingIMAPMailSource setWithFlagSync(final boolean withFlagSync) {
        this.withFlagSync = withFlagSync;
        return this;
    }

    private ProcessResult process(final int messageCount, final int start, final String folderName) {

        final long startTime = System.currentTimeMillis();

        final int netCount = messageCount - (start == 1 ? 0 : start);

        logger.debug("netCount: {}", netCount);

        if (netCount == 0) {
            return new ProcessResult(0, 0, 0);
        }
        final int block = netCount / threadCount;

        final List<Future<ProcessResult>> fl = new ArrayList<Future<ProcessResult>>();

        logger.debug(netCount + "/" + threadCount + "=" + block);

        int lastStart = 0;

        for (int i = 1; i <= threadCount; i++) {

            final int _start = i == 1 ? start : lastStart + block + 1;
            lastStart = _start;

            final int _end = Math.min(_start + block, messageCount);
            if (_end < _start) {
                continue;
            }

            logger.debug("Schedule: " + _start + " - " + _end);

            final Future<ProcessResult> f = es.submit(new Callable<ProcessResult>() {

                @Override
                public ProcessResult call() throws Exception {
                    return processMessageSlice(_start, _end, folderName);
                }
            });

            fl.add(f);
        }

        long highestUid = 0;
        int processedCount = 0;

        for (final Future<ProcessResult> fu : fl) {
            try {
                highestUid = Math.max(highestUid, fu.get().highestUid);
                processedCount += fu.get().processedCount;
                logger.debug("Finished with " + fu.get());
            } catch (final Exception e) {
                logger.error("Unable to process some mails due to {}", e, e.toString());
            }
        }

        final long endTime = System.currentTimeMillis() + 1;

        return new ProcessResult(highestUid, processedCount, endTime - startTime);

    }

    private ProcessResult processMessageSlice(final int start, final int end, final String folderName) throws Exception {

        logger.debug("processMessageSlice() started with " + start + "/" + end + "/" + folderName);
        final long startTime = System.currentTimeMillis();
        final Store store = Session.getInstance(props).getStore();
        store.connect(user, password);
        final Folder folder = store.getFolder(folderName);
        final UIDFolder uidfolder = (UIDFolder) folder;

        IMAPUtils.open(folder);

        try {

            final Message[] msgs = folder.getMessages(start, end);
            folder.fetch(msgs, IMAPUtils.FETCH_PROFILE_HEAD);

            logger.debug("folder fetch done");

            long highestUid = 0;
            int processedCount = 0;

            for (final Message m : msgs) {
                try {

                    IMAPUtils.open(folder);
                    final long uid = uidfolder.getUID(m);

                    mailDestination.onMessage(m);

                    highestUid = Math.max(highestUid, uid);
                    processedCount++;

                    if (Thread.currentThread().isInterrupted()) {
                        break;
                    }

                } catch (final Exception e) {
                    stateManager.onError("Unable to make indexable message", m, e);
                    logger.error("Unable to make indexable message due to {}", e, e.toString());

                    IMAPUtils.open(folder);
                }
            }

            final long endTime = System.currentTimeMillis() + 1;
            final ProcessResult pr = new ProcessResult(highestUid, processedCount, endTime - startTime);
            logger.debug("processMessageSlice() ended with " + pr);
            return pr;

        } finally {

            IMAPUtils.close(folder);
            IMAPUtils.close(store);
        }

    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected void fetch(final Folder folder) throws MessagingException, IOException {

        if ((folder.getType() & Folder.HOLDS_MESSAGES) == 0) {
            logger.warn("Folder {} cannot hold messages", folder.getFullName());
            return;

        }

        final int messageCount = folder.getMessageCount();

        final UIDFolder uidfolder = (UIDFolder) folder;
        final long servervalidity = uidfolder.getUIDValidity();
        final RiverState riverState = stateManager.getRiverState(folder);
        final Long localvalidity = riverState.getUidValidity();

        logger.info("Fetch mails from folder {} ({})", folder.getURLName().toString(), messageCount);

        logger.debug("Server uid validity: {}, Local uid validity: {}", servervalidity, localvalidity);

        if (localvalidity == null || localvalidity.longValue() != servervalidity) {

            logger.debug("UIDValidity fail, full resync " + localvalidity + "!=" + servervalidity);

            if (localvalidity != null) {
                mailDestination.clearDataForFolder(folder.getFullName());
            }

            final ProcessResult result = process(messageCount, 1, folder.getFullName());

            riverState.setLastCount(result.getProcessedCount());

            if (result.getProcessedCount() > 0) {
                riverState.setLastIndexed(new Date());
            }

            if (result.getProcessedCount() > 0) {
                riverState.setLastTook(result.getTook());
            }

            riverState.setLastSchedule(new Date());

            if (result.getProcessedCount() > 0 && result.getHighestUid() > 0) {
                riverState.setLastUid(result.getHighestUid());
            }

            riverState.setUidValidity(servervalidity);
            stateManager.setRiverState(riverState);

            logger.info("Initiailly processed {} mails for folder {}", result.getProcessedCount(), folder.getFullName());
            logger.debug("Processed result {}", result.toString());

        } else {

            if (messageCount == 0) {
                logger.debug("Folder {} is empty", folder.getFullName());
            } else {

                if (withFlagSync) {
                    // detect flag change
                    final Message[] flagMessages = folder.getMessages();
                    folder.fetch(flagMessages, IMAPUtils.FETCH_PROFILE_FLAGS_UID);

                    for (final Message message : flagMessages) {
                        try {

                            final long uid = ((UIDFolder) message.getFolder()).getUID(message);

                            final String id = uid + "::" + message.getFolder().getURLName();

                            final int storedHashcode = mailDestination.getFlaghashcode(id);

                            if (storedHashcode == -1) {
                                // New mail which is not indexed yet
                                continue;
                            }

                            final int flagHashcode = message.getFlags().hashCode();

                            if (flagHashcode != storedHashcode) {
                                // flags change for this message, must update
                                mailDestination.onMessage(message);

                                if (logger.isDebugEnabled()) {
                                    logger.debug("Update " + id + " because of flag change");
                                }
                            }
                        } catch (final Exception e) {
                            logger.error("Error detecting flagchanges for message " + ((MimeMessage) message).getMessageID(), e);
                            stateManager.onError("Error detecting flagchanges", message, e);
                        }
                    }
                }

                final long highestUID = riverState.getLastUid(); // this uid is
                                                                 // already
                                                                 // processed

                logger.debug("highestUID: {}", highestUID);

                Message[] msgsnew = uidfolder.getMessagesByUID(highestUID+1, UIDFolder.LASTUID);

                logger.debug("msg count for UID >= {}: {}", highestUID+1, msgsnew.length);
                
                // msgnew.size is always >= 1 if folder is not empty
                if (msgsnew.length > 0 && uidfolder.getUID(msgsnew[msgsnew.length-1]) <= highestUID) {
                    logger.debug("will not process UID {}, because already processed", uidfolder.getUID(msgsnew[msgsnew.length-1]));
                    msgsnew = (Message[]) ArrayUtils.remove(msgsnew, msgsnew.length-1);
                }

                if (msgsnew.length > 0) {

                    logger.info("{} new messages in folder {}", msgsnew.length, folder.getFullName());

                    final int start = msgsnew[0].getMessageNumber();

                    final ProcessResult result = process(messageCount, start, folder.getFullName());

                    riverState.setLastCount(result.getProcessedCount());

                    if (result.getProcessedCount() > 0) {
                        riverState.setLastIndexed(new Date());
                    }

                    if (result.getProcessedCount() > 0) {
                        riverState.setLastTook(result.getTook());
                    }

                    riverState.setLastSchedule(new Date());

                    if (result.getProcessedCount() > 0 && result.getHighestUid() > 0) {
                        riverState.setLastUid(result.getHighestUid());
                    }

                    riverState.setUidValidity(servervalidity);
                    stateManager.setRiverState(riverState);

                    logger.info("Processed {} mails for folder {}", result.getProcessedCount(), folder.getFullName());
                    logger.debug("Processed result {}", result.toString());
                } else {
                    logger.debug("no new messages");
                }

            }
            // check for expunged/deleted messages

            final Set<Long> serverMailSet = new HashSet<Long>();

            final long oldmailUid = riverState.getLastUid();
            logger.debug("oldmailuid {}", oldmailUid);

            final Message[] msgsold = uidfolder.getMessagesByUID(1, oldmailUid);

            folder.fetch(msgsold, IMAPUtils.FETCH_PROFILE_UID);

            for (final Message m : msgsold) {
                try {
                    final long uid = uidfolder.getUID(m);
                    serverMailSet.add(uid);

                } catch (final Exception e) {
                    stateManager.onError("Unable to handle old message ", m, e);
                    logger.error("Unable to handle old message due to {}", e, e.toString());

                    IMAPUtils.open(folder);
                }
            }

            final Set localMailSet = new HashSet(mailDestination.getCurrentlyStoredMessageUids(folder.getFullName(), false));

            logger.debug("Check now " + localMailSet.size() + " server mails for expunge");

            localMailSet.removeAll(serverMailSet);
            // localMailSet has now the ones that are not on server

            logger.info(localMailSet.size() + " messages were locally deleted, because they are expunged on server.");

            mailDestination.onMessageDeletes(localMailSet, folder.getFullName(), false);

        }

    }

    protected void fetch(final Pattern pattern, final String folderName) throws MessagingException, IOException {

        logger.debug("fetch() - pattern: {}, folderName: {}", pattern, folderName);

        final Store store = Session.getInstance(props).getStore();
        store.connect(user, password);

        try {
            for (final String fol : mailDestination.getFolderNames()) {
                if (store.getFolder(fol).exists()) {
                    logger.debug("{} exists", fol);
                } else {
                    logger.info("Folder {} does not exist on the server, will remove it (and its content) also locally", fol);
                    final RiverState riverState = stateManager.getRiverState(store.getFolder(fol));
                    riverState.setExists(false);
                    stateManager.setRiverState(riverState);

                    try {
                        mailDestination.clearDataForFolder(fol);
                    } catch (final Exception e) {
                        stateManager.onError("Unable to clean data for stale folder", store.getFolder(fol), e);
                    }
                }
            }
        } catch (final IndexMissingException ime) {
            logger.debug(ime.toString());

        } catch (final Exception e) {
            logger.error("Error checking for stale folders", e);
        }

        final boolean isRoot = StringUtils.isEmpty(folderName);
        final Folder folder = isRoot ? store.getDefaultFolder() : store.getFolder(folderName);

        try {

            if (!folder.exists()) {
                logger.error("Folder {} does not exist on the server", folder.getFullName());
                return;
            }

            logger.debug("folderName: {} is root: {}", folderName, isRoot);

            /*if (logger.isDebugEnabled() && store instanceof IMAPStore) {

            	IMAPStore imapStore = (IMAPStore) store;
            	logger.debug("IMAP server capability info");
            	logger.debug("IMAP4rev1: "
            			+ imapStore.hasCapability("IMAP4rev1"));
            	logger.debug("IDLE: " + imapStore.hasCapability("IDLE"));
            	logger.debug("ID: " + imapStore.hasCapability("ID"));
            	logger.debug("UIDPLUS: " + imapStore.hasCapability("UIDPLUS"));
            	logger.debug("CHILDREN: " + imapStore.hasCapability("CHILDREN"));
            	logger.debug("NAMESPACE: "
            			+ imapStore.hasCapability("NAMESPACE"));
            	logger.debug("NOTIFY: " + imapStore.hasCapability("NOTIFY"));
            	logger.debug("CONDSTORE: "
            			+ imapStore.hasCapability("CONDSTORE"));
            	logger.debug("QRESYNC: " + imapStore.hasCapability("QRESYNC"));
            	logger.debug("STATUS: " + imapStore.hasCapability("STATUS"));
            }*/

            if (pattern != null && !isRoot && !pattern.matcher(folder.getFullName()).matches()) {
                logger.debug(folder.getFullName() + " does not match pattern " + pattern.toString());
                return;
            }

            IMAPUtils.open(folder);

            recurseFolders(folder, pattern);

        } finally {

            IMAPUtils.close(folder);
            IMAPUtils.close(store);

        }
    }

    protected void recurseFolders(final Folder folder, final Pattern pattern) throws MessagingException, IOException {

        if (folder != null) {

            if (es == null || es.isShutdown() || es.isTerminated() || Thread.currentThread().isInterrupted()) {

                logger.warn("Stop processing of mails due to mail source is closed");
                return;

            }

            if ((folder.getType() & Folder.HOLDS_MESSAGES) != 0) {

                if (pattern != null && !pattern.matcher(folder.getFullName()).matches()) {
                    logger.info("Pattern {} does not match {}", pattern.pattern(), folder.getFullName());
                    return;
                }
                IMAPUtils.open(folder);

                try {
                    fetch(folder);
                } finally {
                    IMAPUtils.close(folder);
                    logger.debug("fetch {} done", folder.getFullName());
                }
            }

            if ((folder.getType() & Folder.HOLDS_FOLDERS) != 0) {
                for (final Folder subfolder : folder.list()) {

                    recurseFolders(subfolder, pattern);

                }
            }

        }

    }

    private static class ProcessResult {
        private long highestUid = 1;

        private final int processedCount;
        private final long took;

        public ProcessResult(final long highestUid, final int processedCount, final long took) {
            super();
            this.highestUid = highestUid < 1 ? 1L : highestUid;
            this.processedCount = processedCount;
            this.took = took;
        }

        public long getHighestUid() {
            return highestUid;
        }

        public int getProcessedCount() {
            return processedCount;
        }

        public long getTook() {
            return took;
        }

        @Override
        public String toString() {
            return "ProcessResult [highestUid=" + highestUid + ", processedCount=" + processedCount + ", took=" + took + "]";
        }

    }

}
