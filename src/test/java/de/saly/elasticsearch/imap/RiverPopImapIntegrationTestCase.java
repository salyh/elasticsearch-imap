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

import java.util.Map;
import java.util.Properties;

import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.junit.Assert;
import org.junit.Test;

import de.saly.elasticsearch.river.imap.IMAPRiver;

public class RiverPopImapIntegrationTestCase extends RiverPopImapTestCase {

    @Test
    public void testDeletesIMAPIntgr() throws Exception {

        deleteScenarioIMAP("priv-river-imap-strato.json");
    }

    // @Test
    public void testRealInteractive() throws Exception {
        realIntegrationInteractive("priv-river-strato-pop3.json");
    }

    @Test
    public void testRenameIMAPIntgr() throws Exception {

        renameScenarioIMAP("priv-river-imap-strato-2.json");
    }

    protected void deleteScenarioIMAP(final String resource) throws Exception {

        final RiverSettings settings = riverSettings("/" + resource);

        final Properties props = new Properties();
        final String indexName = XContentMapValues.nodeStringValue(settings.settings().get("mail_index_name"), "imapriver");
        final String typeName = XContentMapValues.nodeStringValue(settings.settings().get("mail_type_name"), "mail");
        final String user = XContentMapValues.nodeStringValue(settings.settings().get("user"), null);
        final String password = XContentMapValues.nodeStringValue(settings.settings().get("password"), null);

        for (final Map.Entry<String, Object> entry : settings.settings().entrySet()) {

            if (entry != null && entry.getKey().startsWith("mail.")) {
                props.setProperty(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }

        createInitialIMAPTestdata(props, user, password, 10, true);

        registerRiver("myrivunit", resource);

        Thread.sleep(1000);

        Thread.sleep(5 * 1000);

        deleteMailsFromUserMailbox(props, "INBOX", 5, 3, user, password); // delete
                                                                          // 3

        Thread.sleep(5 * 1000);

        createInitialIMAPTestdata(props, user, password, 5, false);

        Thread.sleep(5 * 1000);

        deleteMailsFromUserMailbox(props, "INBOX", 4, 2, user, password); // delete
                                                                          // 2

        Thread.sleep(5 * 1000);

        Assert.assertEquals(10, getCount(indexName, typeName));

    }

    protected void realIntegrationInteractive(final String resource) throws Exception {

        final RiverSettings settings = riverSettings("/" + resource);
        final IMAPRiver river = new IMAPRiver(new RiverName("a", "b"), settings, esSetup.client());

        river.start();

        while (true) {
            Thread.sleep(1000);
        }

        // river.close();

        // Assert.assertEquals(100, getCount(river));

    }

    protected void renameScenarioIMAP(final String resource) throws Exception {

        final RiverSettings settings = riverSettings("/" + resource);

        final Properties props = new Properties();
        final String indexName = XContentMapValues.nodeStringValue(settings.settings().get("mail_index_name"), "imapriver");
        final String typeName = XContentMapValues.nodeStringValue(settings.settings().get("mail_type_name"), "mail");
        final String user = XContentMapValues.nodeStringValue(settings.settings().get("user"), null);
        final String password = XContentMapValues.nodeStringValue(settings.settings().get("password"), null);

        for (final Map.Entry<String, Object> entry : settings.settings().entrySet()) {

            if (entry != null && entry.getKey().startsWith("mail.")) {
                props.setProperty(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }

        createInitialIMAPTestdata(props, user, password, 10, true);

        registerRiver("myrivunit", resource);

        Thread.sleep(1000);

        Thread.sleep(15 * 1000);

        Assert.assertEquals(30, getCount(indexName, typeName));

        renameMailbox(props, "ES-IMAP-RIVER-TESTS", user, password);

        Thread.sleep(15 * 1000);

        deleteMailsFromUserMailbox(props, "renamed_from_ES-IMAP-RIVER-TESTS", 4, 2, user, password); // delete
                                                                                                     // 2

        Thread.sleep(15 * 1000);

        Assert.assertEquals(28, getCount(indexName, typeName));

    }
}
