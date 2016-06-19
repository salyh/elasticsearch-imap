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
package de.saly.elasticsearch.importer.imap.maildestination;

import java.io.IOException;
import java.util.Set;

import javax.mail.Folder;
import javax.mail.Message;
import javax.mail.MessagingException;

public interface MailDestination {

    public abstract void clearDataForFolder(Folder folder) throws IOException, MessagingException;

    public abstract void close();

    @SuppressWarnings("rawtypes")
    public abstract Set getCurrentlyStoredMessageUids(Folder folder) throws IOException, MessagingException;

    public int getFlaghashcode(String id) throws IOException, MessagingException;

    public abstract Set<String> getFolderNames() throws IOException, MessagingException;

    public abstract void onMessage(Message msg) throws IOException, MessagingException;

    @SuppressWarnings("rawtypes")
    public abstract void onMessageDeletes(Set msgs, Folder folder) throws IOException, MessagingException;

    public abstract ElasticsearchMailDestination startup() throws IOException;
}
