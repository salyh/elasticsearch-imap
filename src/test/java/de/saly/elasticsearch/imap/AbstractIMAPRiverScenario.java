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

import de.saly.elasticsearch.river.imap.IMAPRiver;

public abstract class AbstractIMAPRiverScenario extends AbstractIMAPRiverUnitTest {

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

        createInitialIMAPTestdata(props, user, password, 11, true); // 3*11
                                                                    // =
                                                                    // 33

        registerRiver("myrivunit", resource);

        Thread.sleep(1000);

        Thread.sleep(25 * 1000);

        Assert.assertEquals(33, getCount(indexName, typeName));

        deleteMailsFromUserMailbox(props, "INBOX", 5, 3, user, password); // delete
        // 3

        Thread.sleep(25 * 1000);

        Assert.assertEquals(30, getCount(indexName, typeName));

        createInitialIMAPTestdata(props, user, password, 7, false); // 3*7=21

        Thread.sleep(25 * 1000);

        Assert.assertEquals(51, getCount(indexName, typeName));
        deleteMailsFromUserMailbox(props, "INBOX", 4, 2, user, password); // delete
        // 2

        Thread.sleep(25 * 1000);

        Assert.assertEquals(49, getCount(indexName, typeName));

    }

    protected void deleteScenarioPOP(final String resource) throws Exception {

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

        putMailInMailbox(10);

        registerRiver("myrivunit", resource);

        Thread.sleep(1000);

        Thread.sleep(5 * 1000);

        deleteMailsFromUserMailbox(props, "INBOX", 1, 3, user, password); // delete
        // 3

        Thread.sleep(5 * 1000);

        putMailInMailbox(5);

        Thread.sleep(5 * 1000);

        deleteMailsFromUserMailbox(props, "INBOX", 4, 2, user, password); // delete
        // 2

        Thread.sleep(5 * 1000);

        Assert.assertEquals(10, getCount(indexName, typeName));

    }

    protected void massScenario(final String resource) throws Exception {

        putMailInMailbox(50);

        final RiverSettings settings = riverSettings("/" + resource);
        final IMAPRiver river = new IMAPRiver(new RiverName("a", "b"), settings, esSetup.client());

        river.start();

        Thread.sleep(1000);

        putMailInMailbox(50);

        Thread.sleep(5500);

        river.close();

        Assert.assertEquals(100, getCount(river.getIndexName(), river.getTypeName()));

    }

    protected void plainScenario(final String resource) throws Exception {

        final RiverSettings settings = riverSettings("/" + resource);
        final IMAPRiver river = new IMAPRiver(new RiverName("a", "b"), settings, esSetup.client());
        river.start();

        Thread.sleep(100);

        putMailInMailbox(50);

        Thread.sleep(500);

        putMailInMailbox(5);

        Thread.sleep(15000);

        river.close();

        Assert.assertEquals(55, getCount(river.getIndexName(), river.getTypeName()));

    }

    protected void plainScenarioOnce(final String resource) throws Exception {

        final RiverSettings settings = riverSettings("/" + resource);
        final IMAPRiver river = new IMAPRiver(new RiverName("a", "b"), settings, esSetup.client());
        river.once();

        putMailInMailbox(50);

        river.once();

        putMailInMailbox(5);

        river.once();

        Thread.sleep(10000);

        river.close();

        Assert.assertEquals(55, getCount(river.getIndexName(), river.getTypeName()));

    }

    protected void plainScenarioOnceNoFirstRun(final String resource) throws Exception {

        final RiverSettings settings = riverSettings("/" + resource);
        final IMAPRiver river = new IMAPRiver(new RiverName("a", "b"), settings, esSetup.client());

        putMailInMailbox(50);

        river.once();

        putMailInMailbox(5);

        river.once();

        Thread.sleep(10000);

        river.close();

        Assert.assertEquals(55, getCount(river.getIndexName(), river.getTypeName()));

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

        createInitialIMAPTestdata(props, user, password, 10, true); // 3*10

        registerRiver("myrivunit", resource);

        Thread.sleep(1000);

        Thread.sleep(25 * 1000);

        Assert.assertEquals(30, getCount(indexName, typeName));

        renameMailbox(props, "ES-IMAP-RIVER-TESTS", user, password);

        Thread.sleep(25 * 1000);

        deleteMailsFromUserMailbox(props, "renamed_from_ES-IMAP-RIVER-TESTS", 4, 2, user, password); // delete
        // 2

        Thread.sleep(25 * 1000);

        Assert.assertEquals(28, getCount(indexName, typeName));

    }

}
