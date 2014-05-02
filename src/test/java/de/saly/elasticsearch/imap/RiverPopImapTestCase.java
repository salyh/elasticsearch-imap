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

public class RiverPopImapTestCase extends AbstractIMAPRiverScenario {

    @Test
    public void testDeletesIMAPIntgr() throws Exception {

        deleteScenarioIMAP("river-imaps-1.json");
    }

    @Test
    public void testDeletesPOP() throws Exception {

        deleteScenarioPOP("river-pop3-1.json");
    }

    @Test
    public void testImapMassScenario() throws Exception {

        massScenario("river-imaps-1.json");

    }

    @Test
    public void testImapPlainScenario() throws Exception {
        plainScenario("river-imaps-1.json");

    }

    @Test
    public void testImapPlainScenarioInvalidFolderPattern() throws Exception {
        plainScenario("river-imaps-invpattern.json");

    }

    @Test
    public void testImapPlainScenarioOnce() throws Exception {

        plainScenarioOnce("river-imaps-1.json");
    }

    @Test
    public void testImapPlainScenarioOnceNoFirstRun() throws Exception {

        plainScenarioOnceNoFirstRun("river-imaps-1.json");
    }

    @Test
    public void testPopMassScenario() throws Exception {

        massScenario("river-pop3-1.json");

    }

    @Test
    public void testPopPlainScenario() throws Exception {
        plainScenario("river-pop3-1.json");

    }

    @Test
    public void testPopPlainScenarioOnce() throws Exception {
        plainScenarioOnce("river-pop3-1.json");
    }

    @Test
    public void testPopPlainScenarioOnceNoFirstRun() throws Exception {
        plainScenarioOnceNoFirstRun("river-pop3-1.json");
    }

    @Test
    public void testRenameIMAPIntgr() throws Exception {

        renameScenarioIMAP("river-imaps-1.json");
    }

}
