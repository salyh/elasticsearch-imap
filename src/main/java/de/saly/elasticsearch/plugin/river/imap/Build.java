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
package de.saly.elasticsearch.plugin.river.imap;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.StringReader;
import java.net.URL;
import java.util.Enumeration;
import java.util.Properties;

//copied from https://github.com/jprante/elasticsearch-river-jdbc/blob/master/src/main/java/org/xbib/elasticsearch/plugin/river/jdbc/Build.java
public class Build {

    private static final Build INSTANCE;

    static {
        String version = "NA";
        String hash = "NA";
        String hashShort = "NA";
        String timestamp = "NA";
        String date = "NA";

        try {
            final String pluginName = IMAPRiverPlugin.class.getName();
            final Enumeration<URL> e = IMAPRiverPlugin.class.getClassLoader().getResources("es-plugin.properties");
            while (e.hasMoreElements()) {
                final URL url = e.nextElement();
                final InputStream in = url.openStream();
                final ByteArrayOutputStream out = new ByteArrayOutputStream();
                final byte[] buffer = new byte[1024];
                int len;
                while ((len = in.read(buffer)) != -1) {
                    out.write(buffer, 0, len);
                }
                in.close();
                final Properties props = new Properties();
                props.load(new StringReader(new String(out.toByteArray())));
                final String plugin = props.getProperty("plugin");
                if (pluginName.equals(plugin)) {
                    version = props.getProperty("version");
                    hash = props.getProperty("hash");
                    if (!"NA".equals(hash)) {
                        hashShort = hash.substring(0, 7);
                    }
                    timestamp = props.getProperty("timestamp");
                    date = props.getProperty("date");
                }
            }
        } catch (final Throwable e) {
            // just ignore...
        }
        INSTANCE = new Build(version, hash, hashShort, timestamp, date);
    }

    public static Build getInstance() {
        return INSTANCE;
    }

    private final String date;

    private final String hash;

    private final String hashShort;

    private final String timestamp;

    private final String version;

    Build(final String version, final String hash, final String hashShort, final String timestamp, final String date) {
        this.version = version;
        this.hash = hash;
        this.hashShort = hashShort;
        this.timestamp = timestamp;
        this.date = date;
    }

    public String getDate() {
        return date;
    }

    public String getHash() {
        return hash;
    }

    public String getShortHash() {
        return hashShort;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getVersion() {
        return version;
    }

}
