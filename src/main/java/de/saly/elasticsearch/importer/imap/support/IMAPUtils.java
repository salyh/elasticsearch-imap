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
package de.saly.elasticsearch.importer.imap.support;

import java.util.ArrayList;
import java.util.List;

import javax.mail.FetchProfile;
import javax.mail.FetchProfile.Item;
import javax.mail.Flags;
import javax.mail.Folder;
import javax.mail.MessagingException;
import javax.mail.Store;
import javax.mail.UIDFolder;

import com.sun.mail.imap.IMAPFolder.FetchProfileItem;

public class IMAPUtils {

    public static final FetchProfile FETCH_PROFILE_FLAGS_UID = new FetchProfile();
    public static final FetchProfile FETCH_PROFILE_HEAD = new FetchProfile();
    public static final FetchProfile FETCH_PROFILE_UID = new FetchProfile();

    static {

        FETCH_PROFILE_HEAD.add(Item.ENVELOPE);
        FETCH_PROFILE_HEAD.add(Item.CONTENT_INFO);
        FETCH_PROFILE_HEAD.add(FetchProfileItem.HEADERS);
        FETCH_PROFILE_FLAGS_UID.add(Item.FLAGS);
        FETCH_PROFILE_FLAGS_UID.add(UIDFolder.FetchProfileItem.UID);

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

        if (folder != null && folder.exists() && !folder.isOpen() && (folder.getType() & Folder.HOLDS_MESSAGES) != 0) {
            folder.open(Folder.READ_ONLY);
        }

    }

    public static String[] toStringArray(final Flags flags) {
        final List<String> flagsL = new ArrayList<String>(10);

        if (flags.contains(Flags.Flag.DELETED)) {
            flagsL.add("Deleted");
        }
        if (flags.contains(Flags.Flag.ANSWERED)) {
            flagsL.add("Answered");
        }
        if (flags.contains(Flags.Flag.DRAFT)) {
            flagsL.add("Draft");
        }
        if (flags.contains(Flags.Flag.FLAGGED)) {
            flagsL.add("Flagged");
        }
        if (flags.contains(Flags.Flag.RECENT)) {
            flagsL.add("Recent");
        }
        if (flags.contains(Flags.Flag.SEEN)) {
            flagsL.add("Seen");
        }

        if (flags.contains(Flags.Flag.USER)) {
            final String[] userFlags = flags.getUserFlags();
            for (int j = 0; j < userFlags.length; j++) {
                flagsL.add(userFlags[j]);
            }
        }

        return flagsL.toArray(new String[flagsL.size()]);
    }

}
