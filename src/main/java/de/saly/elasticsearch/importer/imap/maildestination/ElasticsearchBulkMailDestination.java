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
package de.saly.elasticsearch.importer.imap.maildestination;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.mail.Message;
import javax.mail.MessagingException;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import de.saly.elasticsearch.importer.imap.support.IndexableMailMessage;

public class ElasticsearchBulkMailDestination extends ElasticsearchMailDestination {

    private static final AtomicInteger outstandingBulkRequests = new AtomicInteger(0);
    private static final AtomicInteger queue = new AtomicInteger(0);

    private BulkProcessor bulk;

    private TimeValue flushInterval = TimeValue.timeValueSeconds(5);

    private final BulkProcessor.Listener listener = new BulkProcessor.Listener() {

        @Override
        public void afterBulk(final long executionId, final BulkRequest request, final BulkResponse response) {
            final long l = outstandingBulkRequests.decrementAndGet();
            final int cur = queue.addAndGet(-response.getItems().length);
            logger.info("Bulk actions done successfully [{}] success [{} items] [{}ms], {} outstanding bulk requests, queue size is {}",
                    executionId, response.getItems().length, response.getTookInMillis(), l, cur);

        }

        @Override
        public void afterBulk(final long executionId, final BulkRequest request, final Throwable failure) {
            final long l = outstandingBulkRequests.decrementAndGet();
            logger.error("Bulk actions done with errors [" + executionId + "] error, {} outstanding bulk requests", failure, l);
            setError(true);
        }

        @Override
        public void beforeBulk(final long executionId, final BulkRequest request) {
            final long l = outstandingBulkRequests.incrementAndGet();
            logger.info("New bulk actions queued [{}] of [{} items], {} outstanding bulk requests", executionId, request.numberOfActions(),
                    l);
        }
    };

    private int maxBulkActions = 100;

    private int maxConcurrentBulkRequests = 30;

    private final ByteSizeValue maxVolumePerBulkRequest = ByteSizeValue.parseBytesSizeValue("10mb");

    @Override
    public ElasticsearchMailDestination client(final Client client) {

        super.client(client);

        bulk = BulkProcessor.builder(client, listener)
                .setBulkActions(maxBulkActions)
                .setConcurrentRequests(maxConcurrentBulkRequests).setBulkSize(maxVolumePerBulkRequest).setFlushInterval(flushInterval)
                .build();
        return this;
    }

    @Override
    public synchronized void close() {

        super.close();

        /*while (!isError() && queue.get() > 0) {
            logger.info("There are {} outstanding bulk messages, will wait until flushed", queue.get());

            try {
                Thread.sleep(200);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("interrupted", e);
            }
        }*/

        if (bulk != null) {
            logger.debug("Shutdown (flush) bulk processor, is super closed " + isClosed());
            bulk.close();
        }

    }

    public ElasticsearchBulkMailDestination flushInterval(final TimeValue flushInterval) {

        this.flushInterval = flushInterval;

        return this;
    }

    public ElasticsearchBulkMailDestination maxBulkActions(final int maxBulkActions) {
        this.maxBulkActions = maxBulkActions;
        return this;
    }

    public ElasticsearchBulkMailDestination maxConcurrentBulkRequests(final int maxConcurrentBulkRequests) {
        this.maxConcurrentBulkRequests = maxConcurrentBulkRequests;
        return this;
    }

    @Override
    public void onMessage(final Message msg) throws IOException, MessagingException {
        if (isClosed()) {
            if (logger.isTraceEnabled()) {
                logger.trace("Is closed, will not index");
            }
            return;
        }

        if (isError()) {
            if (logger.isTraceEnabled()) {
                logger.trace("error, not indexing");
            }
            return;
        }

        final IndexableMailMessage imsg = IndexableMailMessage.fromJavaMailMessage(msg, isWithTextContent(), isWithHtmlContent(), isPreferHtmlContent(), isWithAttachments(),
                isStripTagsFromTextContent(), getHeadersToFields());

        if (logger.isTraceEnabled()) {
            logger.trace("Bulk process mail " + imsg.getUid() + "/" + imsg.getPopId() + " :: " + imsg.getSubject() + "/"
                    + imsg.getSentDate());
        }

        // following block not needs to be synchronized
        try {

            if (!isClosed()) {
                bulk.add(createIndexRequest(imsg));
                queue.incrementAndGet();
            }
        } catch (final ElasticsearchIllegalStateException e) {

            if (isClosed()) {
                logger.debug("Bulkprocessing error due to {}", e.toString());
            } else {
                logger.error("Bulkprocessing error due to {}", e, e.toString());
            }
        }

    }

}
