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
package de.saly.elasticsearch.riverstate;

import java.io.IOException;

import javax.mail.Folder;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;

public class ElasticsearchRiverStateManager implements RiverStateManager {

    private static final String ERRORS_ID = "errors";
    private static final String FOLDERSTATE_ID = "folderstate";
    private static final String RIVERSTATE_TYPE = "imapriverstate";
    private Client client;
    private String index;
    private final ObjectMapper mapper = new ObjectMapper();
    protected final ESLogger logger = ESLoggerFactory.getLogger(this.getClass().getName());

    public ElasticsearchRiverStateManager client(final Client client) {
        this.client = client;
        return this;
    }

    @Override
    public synchronized RiverState getRiverState(final Folder folder) throws MessagingException {

        try {

            waitForCluster();

            if (client.admin().indices().prepareExists(index()).execute().actionGet().isExists()) {

                final GetResponse response = client
                        .prepareGet(index(), RIVERSTATE_TYPE, FOLDERSTATE_ID + "_" + folder.getURLName().toString().hashCode()).execute()
                        .get();

                if (!response.isSourceEmpty()) {
                    return mapper.readValue(response.getSourceAsString(), new TypeReference<RiverState>() {
                    });

                }
            }
        } catch (final Exception ex) {
            throw new MessagingException("Unable to get river state", ex);
        }

        final RiverState rs = new RiverState();
        rs.setFolderUrl(folder.getURLName().toString());
        rs.setLastUid(1L);
        rs.setExists(true);
        return rs;

    }

    public String index() {
        return index;
    }

    public ElasticsearchRiverStateManager index(final String index) {
        this.index = index;
        return this;
    }

    @Override
    public void onError(final String errmsg, final Folder folder, final Exception e) {

        logger.error("Folder " + folder.getFullName() + " throws an error:" + errmsg + e, e);

        try {
            client.prepareIndex(index(), RIVERSTATE_TYPE, ERRORS_ID + "_" + folder.getURLName().toString().hashCode())
                    .setSource(mapper.writeValueAsString(new IndexableError(null, folder.getURLName().toString(), errmsg + e))).execute()
                    .actionGet();

        } catch (final Exception ex) {
            logger.error("Unable to log an error because of " + ex + errmsg, e);
        }

    }

    @Override
    public void onError(final String errmsg, final Message msg, final Exception e) {

        try {
            logger.error("Message " + ((MimeMessage) msg).getMessageID() + " throws an error: " + errmsg + e, e);

            client.prepareIndex(index(), RIVERSTATE_TYPE, ERRORS_ID + "_" + ((MimeMessage) msg).getMessageID().hashCode())
                    .setSource(mapper.writeValueAsString(new IndexableError(((MimeMessage) msg).getMessageID(), null, errmsg + e)))
                    .execute().actionGet();

        } catch (final Exception ex) {
            logger.error("Unable to log an error because of " + ex + errmsg, e);
        }
    }

    @Override
    public void setRiverState(final RiverState state) throws MessagingException {

        try {
            logger.debug("set riverstate " + state);

            client.prepareIndex(index(), RIVERSTATE_TYPE, FOLDERSTATE_ID + "_" + state.getFolderUrl().hashCode())
                    .setSource(mapper.writeValueAsString(state)).execute().actionGet();

            logger.debug("set riverstate done");
        } catch (final Exception ex) {
            throw new MessagingException("Unable to set river state", ex);
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

    private static class IndexableError {
        private final String errormsg;
        private final String folderurl;
        private final String messageid;

        public IndexableError(final String messageid, final String folderurl, final String errormsg) {
            super();
            this.messageid = messageid;
            this.folderurl = folderurl;
            this.errormsg = errormsg;
        }

        public String getErrormsg() {
            return errormsg;
        }

        public String getFolderurl() {
            return folderurl;
        }

        public String getMessageid() {
            return messageid;
        }

    }

}
