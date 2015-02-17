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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.indices.IndexMissingException;

import com.sun.mail.pop3.POP3Folder;

import de.saly.elasticsearch.maildestination.MailDestination;
import de.saly.elasticsearch.riverstate.RiverState;
import de.saly.elasticsearch.riverstate.RiverStateManager;
import de.saly.elasticsearch.support.IMAPUtils;

public class ParallelPollingPOPMailSource implements MailSource {

    private final ExecutorService es;
    private MailDestination mailDestination;
    private final String password;
    private final Properties props;
    private RiverStateManager stateManager;
    private final int threadCount;
    private final String user;
    protected final ESLogger logger = ESLoggerFactory.getLogger(this.getClass().getName());
    private boolean deleteExpungedMessages = true;

    public ParallelPollingPOPMailSource(final Properties props, final int threadCount, final String user, final String password) {
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
    
    public ParallelPollingPOPMailSource setDeleteExpungedMessages(final boolean deleteExpungedMessages) {
        this.deleteExpungedMessages = deleteExpungedMessages;
        return this;
    }

    private ProcessResult process(final int messageCount, final int start, final String folderName) {

        final long startTime = System.currentTimeMillis();

        final int netCount = messageCount - (start == 1 ? 0 : start);

        logger.debug("netCount: {}", netCount);

        if (netCount == 0) {
            return new ProcessResult(0, 0);
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

        int processedCount = 0;

        for (final Future<ProcessResult> fu : fl) {
            try {

                processedCount += fu.get().processedCount;
            } catch (final Exception e) {
                logger.error("Unable to process some mails due to {}", e, e.toString());
            }
        }

        final long endTime = System.currentTimeMillis() + 1;

        return new ProcessResult(processedCount, endTime - startTime);

    }

    private ProcessResult processMessageSlice(final int start, final int end, final String folderName) throws Exception {

        final long startTime = System.currentTimeMillis();
        final Store store = Session.getInstance(props).getStore();
        store.connect(user, password);
        final Folder folder = store.getFolder(folderName);

        IMAPUtils.open(folder);

        try {

            final Message[] msgs = folder.getMessages(start, end);
            folder.fetch(msgs, IMAPUtils.FETCH_PROFILE_HEAD);

            int processedCount = 0;

            for (final Message m : msgs) {
                try {
                    mailDestination.onMessage(m);
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
            return new ProcessResult(processedCount, endTime - startTime);

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

        final int messageCount = folder.getMessageCount(); // can change during
                                                           // method
        final RiverState riverState = stateManager.getRiverState(folder);

        logger.info("Fetch mails from folder {} ({})", folder.getURLName().toString(), messageCount);

        final Map<String, Integer> serverMailSet = new HashMap<String, Integer>(messageCount);// can
                                                                                              // become
                                                                                              // stale
        int highestMsgNum = 1;
        Set localMailSet = new HashSet();
        try {
            localMailSet = new HashSet(mailDestination.getCurrentlyStoredMessageUids(folder.getFullName(), true)); // will
                                                                                                                   // not
                                                                                                                   // change
                                                                                                                   // during
                                                                                                                   // this
                                                                                                                   // method
        } catch (final IndexMissingException ime) {
            logger.debug(ime.toString());

        }

        if (messageCount > 0) {

            // get all uids from server
            final Message[] allmsg = folder.getMessages();
            folder.fetch(allmsg, IMAPUtils.FETCH_PROFILE_UID);

            for (final Message m : allmsg) {
                try {
                    final String uid = ((POP3Folder) folder).getUID(m);
                    serverMailSet.put(uid, m.getMessageNumber());

                } catch (final Exception e) {
                    stateManager.onError("Unable to handle message ", m, e);
                    logger.error("Unable to handle message due to {}", e, e.toString());

                    IMAPUtils.open(folder);
                }
            }

            try {

                // get all local store message uids
                // only retrieve the ones which are not stored locally
                // loop through local mails and look for highest msg num
                for (final Object retuid : localMailSet) {

                    final Integer tmpNum = serverMailSet.get(retuid);

                    if (tmpNum == null) {
                        // message was deleted on server
                        // handle later
                    } else {
                        // evaluate highest msg num (not id)
                        highestMsgNum = Math.max(highestMsgNum, serverMailSet.get(retuid).intValue());

                    }

                }

                logger.debug("highestMsgNum: {}", highestMsgNum);
            } catch (final Exception e) {
                logger.error("Error evalutating new messages. Will download all ... due to {}", e, e.toString());
            }

            final ProcessResult result = process(messageCount, highestMsgNum, folder.getFullName());

            riverState.setLastCount(result.getProcessedCount());

            if (result.getProcessedCount() > 0) {
                riverState.setLastIndexed(new Date());
            }

            riverState.setLastTook(result.getTook());
            riverState.setLastSchedule(new Date());
            stateManager.setRiverState(riverState);

            logger.info("Processed {} mails", result.getProcessedCount());
        } else {
            logger.debug("Mailbox empty");
        }

        if(deleteExpungedMessages) {
        
            // check for expunged/deleted messages
            logger.debug("Check now " + localMailSet.size() + " mails for expunge");
    
            localMailSet.removeAll(serverMailSet.keySet());
            // tmpset has now the ones that are not on server
    
            logger.info(localMailSet.size() + " messages were locally deleted, because they are expunged on server.");
    
            mailDestination.onMessageDeletes(localMailSet, folder.getFullName(), true);
        }

    }

    protected void fetch(final Pattern unusedPattern, final String unusedFolderName) throws MessagingException, IOException {

        logger.debug("fetch() - folderName: {}", "INBOX");

        final Store store = Session.getInstance(props).getStore();
        store.connect(user, password);

        final Folder folder = store.getDefaultFolder();
        try {

            if (!folder.exists()) {
                logger.error("Folder {} does not exist on the server", folder.getFullName());
                return;
            }

            IMAPUtils.open(folder);

            recurseFolders(folder, null);

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
                    logger.trace("Pattern {} does not match {}", pattern.pattern(), folder.getFullName());
                    return;
                }
                IMAPUtils.open(folder);
                try {
                    fetch(folder);
                } finally {
                    IMAPUtils.close(folder);
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
        private final int processedCount;
        private final long took;

        public ProcessResult(final int processedCount, final long took) {
            super();

            this.processedCount = processedCount;
            this.took = took;
        }

        public int getProcessedCount() {
            return processedCount;
        }

        public long getTook() {
            return took;
        }

    }

}
