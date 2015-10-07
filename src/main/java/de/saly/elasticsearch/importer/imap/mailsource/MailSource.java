/***********************************************************************************************************************
 *
 * Elasticsearch IMAP/Pop3 E-Mail Importer
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
package de.saly.elasticsearch.importer.imap.mailsource;

import java.io.IOException;
import java.util.regex.Pattern;

import javax.mail.MessagingException;

import de.saly.elasticsearch.importer.imap.maildestination.MailDestination;
import de.saly.elasticsearch.importer.imap.state.StateManager;

public interface MailSource {

    public void close();

    public void fetch(Pattern pattern) throws MessagingException, IOException; // blocks

    public void fetch(String folderName) throws MessagingException, IOException; // blocks

    public void fetchAll() throws MessagingException, IOException; // blocks

    public void setMailDestination(MailDestination mailDestination);
    
    public MailDestination getMailDestination();

    public void setStateManager(StateManager stateManager);

    public void setDeleteExpungedMessages(boolean deleteExpungedMessages);
}
