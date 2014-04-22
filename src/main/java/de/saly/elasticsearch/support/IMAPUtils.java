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
package de.saly.elasticsearch.support;

import javax.mail.FetchProfile;
import javax.mail.FetchProfile.Item;
import javax.mail.Folder;
import javax.mail.MessagingException;
import javax.mail.Store;
import javax.mail.UIDFolder;

import com.sun.mail.imap.IMAPFolder.FetchProfileItem;

public class IMAPUtils {

    public static final FetchProfile FETCH_PROFILE_HEAD = new FetchProfile();
    public static final FetchProfile FETCH_PROFILE_UID = new FetchProfile();

    static {

        FETCH_PROFILE_HEAD.add(Item.ENVELOPE);
        FETCH_PROFILE_HEAD.add(Item.CONTENT_INFO);
        FETCH_PROFILE_HEAD.add(FetchProfileItem.HEADERS);
        // FETCH_PROFILE_HEAD.add(FetchProfileItem.FLAGS);

        FETCH_PROFILE_UID.add(UIDFolder.FetchProfileItem.UID);

    }

    public static void close(final Folder folder) {
        try {
            if (folder != null && folder.isOpen()) {
                folder.close(false);
            }
        } catch (final Exception e) {
            // ignore
        }

    }

    public static void close(final Store store) {
        try {
            if (store != null && store.isConnected()) {
                store.close();
            }
        } catch (final Exception e) {
            // ignore
        }

    }

    public static void open(final Folder folder) throws MessagingException {

        if (folder != null && !folder.isOpen() && (folder.getType() & Folder.HOLDS_MESSAGES) != 0) {
            folder.open(Folder.READ_ONLY);
        }

    }

}
