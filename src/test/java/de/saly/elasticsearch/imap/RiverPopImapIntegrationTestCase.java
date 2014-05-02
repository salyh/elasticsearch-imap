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

import org.junit.Test;

public class RiverPopImapIntegrationTestCase extends RiverPopImapTestCase {

    @Override
    @Test
    public void testDeletesIMAPIntgr() throws Exception {

        deleteScenarioIMAP("priv-river-imap-strato.json");
    }

    // @Test
    public void testRealInteractive() throws Exception {
        realIntegrationInteractive("priv-river-strato-pop3.json");
    }

    @Override
    @Test
    public void testRenameIMAPIntgr() throws Exception {

        renameScenarioIMAP("priv-river-imap-strato.json");
    }

}
