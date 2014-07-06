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
package de.saly.elasticsearch.maildestination;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.mail.Message;
import javax.mail.MessagingException;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;

import de.saly.elasticsearch.support.IndexableMailMessage;

public class ElasticsearchMailDestination implements MailDestination {

    private Client client;

    private volatile boolean closed;

    private volatile boolean error;

    private String index;

    private Map<String, Object> mapping;

    private Map<String, Object> settings;

    private volatile boolean started;

    private boolean stripTagsFromTextContent = true;

    private String type;

    private boolean withAttachments = false;

    private boolean withTextContent = true;

    private List<String> headersToFields;

    protected final ESLogger logger = ESLoggerFactory.getLogger(this.getClass().getName());

    @Override
    public void clearDataForFolder(final String folderName) throws IOException, MessagingException {

        logger.info("Delete locally all messages for folder " + folderName);

        client.admin().indices().refresh(new RefreshRequest()).actionGet();

        client.prepareDeleteByQuery(index).setTypes(type).setQuery(QueryBuilders.termQuery("folderFullName", folderName)).execute()
                .actionGet();

    }

    public ElasticsearchMailDestination client(final Client client) {
        this.client = client;
        return this;
    }

    @Override
    public synchronized void close() {

        if (closed) {
            return;
        }

        closed = true;

        logger.info("Closed");
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public Set getCurrentlyStoredMessageUids(final String folderName, final boolean isPop) throws IOException, MessagingException {

        client.admin().indices().refresh(new RefreshRequest()).actionGet();

        final Set uids = new HashSet();

        final TermQueryBuilder b = QueryBuilders.termQuery("folderFullName", folderName);

        logger.debug("Term query: " + b.buildAsBytes().toUtf8());

        SearchResponse scrollResp = client.prepareSearch().setIndices(index).setTypes(type).setSearchType(SearchType.SCAN).setQuery(b)
                .setScroll(new TimeValue(1000)).setSize(1000).execute().actionGet();

        while (true) {
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(1000)).execute().actionGet();
            boolean hitsRead = false;
            for (final SearchHit hit : scrollResp.getHits()) {
                hitsRead = true;

                if (!isPop) {
                    uids.add(Long.parseLong(hit.getId().split("::")[0]));
                } else {
                    uids.add(hit.getId().split("::")[0]);
                }

                logger.debug("Local: " + hit.getId());
            }
            if (!hitsRead) {
                break;
            }
        }

        logger.debug("Currently locally stored messages for folder {}: {}", folderName, uids.size());

        return uids;

    }

    @Override
    public int getFlaghashcode(final String id) throws IOException, MessagingException {

        client.admin().indices().refresh(new RefreshRequest()).actionGet();

        final GetResponse getResponse = client.prepareGet().setIndex(index).setType(type).setId(id)
                .setFields(new String[] { "flaghashcode" }).execute().actionGet();

        if (getResponse == null || !getResponse.isExists()) {
            return -1;
        }

        final GetField flaghashcodeField = getResponse.getField("flaghashcode");

        if (flaghashcodeField == null || flaghashcodeField.getValue() == null || !(flaghashcodeField.getValue() instanceof Integer)) {
            throw new IOException("No flaghashcode field for id " + id);
        }

        return ((Integer) flaghashcodeField.getValue()).intValue();

    }

    @Override
    public Set<String> getFolderNames() throws IOException, MessagingException {

        client.admin().indices().refresh(new RefreshRequest()).actionGet();

        final HashSet<String> uids = new HashSet<String>();

        SearchResponse scrollResp = client.prepareSearch().setIndices(index).setTypes(type).setSearchType(SearchType.SCAN)
                .setQuery(QueryBuilders.matchAllQuery()).addField("folderFullName").setScroll(new TimeValue(1000)).setSize(1000).execute()
                .actionGet();

        while (true) {
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(1000)).execute().actionGet();
            boolean hitsRead = false;
            for (final SearchHit hit : scrollResp.getHits()) {
                hitsRead = true;
                uids.add((String) hit.getFields().get("folderFullName").getValue());

            }
            if (!hitsRead) {
                break;
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Currently locally stored folders: {}", uids);
        }

        return uids;

    }

    public List<String> getHeadersToFields() {
        return headersToFields;
    }

    public boolean isStripTagsFromTextContent() {
        return stripTagsFromTextContent;
    }

    public boolean isWithAttachments() {
        return withAttachments;
    }

    public boolean isWithTextContent() {
        return withTextContent;
    }

    @Override
    public void onMessage(final Message msg) throws IOException, MessagingException {
        if (closed) {
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

        final IndexableMailMessage imsg = IndexableMailMessage.fromJavaMailMessage(msg, withTextContent, withAttachments,
                stripTagsFromTextContent, headersToFields);

        if (logger.isTraceEnabled()) {
            logger.trace("Process mail " + imsg.getUid() + "/" + imsg.getPopId() + " :: " + imsg.getSubject() + "/" + imsg.getSentDate());
        }

        client.index(createIndexRequest(imsg)).actionGet();

    }

    @SuppressWarnings("rawtypes")
    @Override
    public void onMessageDeletes(final Set msgs, final String folderName, final boolean isPop) throws IOException, MessagingException {

        if (msgs.size() == 0) {
            return;
        }

        client.admin().indices().refresh(new RefreshRequest()).actionGet();

        logger.info("Will delete " + msgs.size() + " messages locally for folder " + folderName);

        final BoolQueryBuilder query = new BoolQueryBuilder();

        if (isPop) {
            query.must(QueryBuilders.inQuery("popId", msgs));
        } else {
            query.must(QueryBuilders.inQuery("uid", msgs));
        }

        query.must(QueryBuilders.termQuery("folderFullName", folderName));

        client.prepareDeleteByQuery(index).setTypes(type).setQuery(query).execute().actionGet();

    }

    public ElasticsearchMailDestination setIndex(final String index) {
        this.index = index;
        return this;
    }

    public ElasticsearchMailDestination setMapping(final Map<String, Object> mapping) {
        this.mapping = mapping;
        return this;
    }

    public ElasticsearchMailDestination setSettings(final Map<String, Object> settings) {
        this.settings = settings;
        return this;
    }

    public ElasticsearchMailDestination setStripTagsFromTextContent(final boolean stripTagsFromTextContent) {
        this.stripTagsFromTextContent = stripTagsFromTextContent;
        return this;
    }

    public ElasticsearchMailDestination setType(final String type) {
        this.type = type;
        return this;
    }

    public ElasticsearchMailDestination setWithAttachments(final boolean withAttachments) {
        this.withAttachments = withAttachments;
        return this;
    }

    public ElasticsearchMailDestination setWithTextContent(final boolean withTextContent) {
        this.withTextContent = withTextContent;
        return this;
    }

    public MailDestination setHeadersToFields(List<String> headersToFields) {
        this.headersToFields = headersToFields;
        return this;
    }

    @Override
    public synchronized ElasticsearchMailDestination startup() throws IOException {

        if (started) {
            return this;
        }
        waitForCluster();
        createIndexIfNotExists();
        started = true;
        return this;
    }

    private synchronized void createIndexIfNotExists() throws IOException {
        if (isError()) {
            if (logger.isTraceEnabled()) {
                logger.trace("error, not creating index");
            }
            return;
        }

        // see if index already exists
        if (client.admin().indices().prepareExists(index).execute().actionGet().isExists()) {
            return;
        }

        final CreateIndexRequestBuilder createIndexRequestBuilder = client.admin().indices().prepareCreate(index);
        if (settings != null) {
            createIndexRequestBuilder.setSettings(settings);
        }
        if (mapping != null) {
            createIndexRequestBuilder.addMapping(type, mapping);
        }

        final XContentBuilder mappingBuilder = jsonBuilder().startObject().startObject(type).startObject("properties")
                .startObject("folderFullName").field("index", "not_analyzed").field("type", "string").endObject()
                .startObject("receivedDate").field("type", "date").field("format", "basic_date_time").endObject().startObject("sentDate")
                .field("type", "date").field("format", "basic_date_time").endObject().startObject("flaghashcode").field("type", "integer")
                .endObject()
                // .startObject("attachments").startObject("properties").startObject("content").field("type",
                // "attachment").endObject().endObject().endObject()
                .endObject().endObject().endObject();

        final CreateIndexResponse res = createIndexRequestBuilder.get();
        logger.info("Index {} and typemapping for {} created? {}", index, type, res.isAcknowledged());

        final PutMappingResponse response = client.admin().indices().preparePutMapping(index).setType(type).setSource(mappingBuilder)
                .execute().actionGet();

        if (!response.isAcknowledged()) {
            throw new IOException("Could not define mapping for type [" + index + "]/[" + type + "].");
        }

    }

    private void waitForCluster() throws IOException {
        waitForCluster(ClusterHealthStatus.YELLOW, TimeValue.timeValueSeconds(30));
    }

    private void waitForCluster(final ClusterHealthStatus status, final TimeValue timeout) throws IOException {
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

    protected IndexRequest createIndexRequest(final IndexableMailMessage message) throws IOException {

        final String id = (!StringUtils.isEmpty(message.getPopId()) ? message.getPopId() : message.getUid()) + "::"
                + message.getFolderUri();

        final IndexRequest request = Requests.indexRequest(index).type(type).id(id).source(message.build());

        return request;

    }

    protected Client getClient() {
        return client;
    }

    protected synchronized boolean isClosed() {
        return closed;
    }

    protected synchronized boolean isError() {
        return error;
    }

    protected void setError(final boolean error) {
        this.error = error;
    }

}
