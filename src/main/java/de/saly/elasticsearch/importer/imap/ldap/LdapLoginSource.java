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
package de.saly.elasticsearch.importer.imap.ldap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.naming.NamingEnumeration;
import javax.naming.directory.Attribute;
import javax.naming.directory.SearchResult;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import com.google.common.collect.Lists;

/**
 * Login data provider connecting to an ldap directory, reading all available
 * users. For Dovecot, a master user account can be supplied who can access
 * all users' mailboxes, even if their passwords are encrypted.
 */
public class LdapLoginSource implements ILoginSource, Runnable {
    private static final String DEF_USER_NAME_FIELD = "dn";
    private static final String DEF_USER_PW_FIELD = "userPassword";
    private static final String DEF_REFRESH_INT = "60";

    private static final String MASTER_SEP = "*";

    private final String fNameField;
    private final String fPasswordField;
    private final String fLdapFilter;

    private final long fRefreshInt;

    // TODO: filter configuration?

    private final List<String> fUserNames, fUserPasswords;
    private final String fMasterUser, fMasterPassword;

    private final ILdapConnector fConnector;

    private final ESLogger fLogger;

    private final Object fLock;
    private volatile boolean fInitialized;
    private volatile boolean fActive; 

    public LdapLoginSource(Map<String, Object> settings) {
	this(settings, null, null);
    }

    public LdapLoginSource(Map<String, Object> settings, String masterUser, String masterPassword) {
	fUserNames = new ArrayList<>();
	fUserPasswords = new ArrayList<>();

	fMasterUser = masterUser;
	fMasterPassword = masterPassword;

	String url = XContentMapValues.nodeStringValue(settings.get("ldap_url"), null);
	String base = XContentMapValues.nodeStringValue(settings.get("ldap_base"), null);
	String user = XContentMapValues.nodeStringValue(settings.get("ldap_user"), null);
	String password = XContentMapValues.nodeStringValue(settings.get("ldap_password"), null);

	fConnector = new SimpleLdapConnector(url, base, user, password, true);

	fNameField = XContentMapValues.nodeStringValue(settings.get("ldap_name_field"), DEF_USER_NAME_FIELD);
	fPasswordField = XContentMapValues.nodeStringValue(settings.get("ldap_password_field"), DEF_USER_PW_FIELD);
	fLdapFilter = fNameField + "=*";

	fLogger = ESLoggerFactory.getLogger(LdapLoginSource.class.getName());

	fLock = new Object();
	fInitialized = false;

	//start refreshing thread once initialized; interval in minutes
	String refreshParam = XContentMapValues.nodeStringValue(settings.get("ldap_refresh_interval"), DEF_REFRESH_INT);
	fRefreshInt = Long.parseLong(refreshParam) * 60000L;
	if(fRefreshInt > 0) {
	    //TODO: actually stop refreshing thread somehow
	    fActive = true;
	    Thread t = new Thread(this);
	    t.setDaemon(true);
	    t.start();
	}
    }

    @SuppressWarnings("rawtypes")
    private void read() throws Exception {
	Exception ex = null;

	synchronized (fLock) {
	    //backup old data
            List<String> nameBackup = Lists.newArrayList(fUserNames);
            List<String> passBackup = Lists.newArrayList(fUserPasswords);
	    try {
                //clear old data
                fUserNames.clear();
                fUserPasswords.clear();

                // start reading from LDAP
                fConnector.connect();

                NamingEnumeration data = fConnector.query("", fLdapFilter);
                if (data != null) {
                    // extract usernames and passwords
                    readData(data);
                }
	    }
            catch(Exception e)
            {
                //use backup
                fUserNames.clear();
                fUserPasswords.clear();
                fUserNames.addAll(nameBackup);
                fUserPasswords.addAll(passBackup);

                ex = e;
            }
	    finally
	    {
                // close connection
                fConnector.disconnect();
	    }
	}

	if(ex != null) {
	    throw ex;
	}

	fInitialized = true;
    }

    @SuppressWarnings("rawtypes")
    private void readData(NamingEnumeration ldapContents) throws Exception {
	SearchResult result = null;
	NamingEnumeration<? extends Attribute> atts = null;
	Attribute a = null;

	while (ldapContents.hasMore()) {
	    String name = null;
	    String password = null;

	    result = (SearchResult) ldapContents.next();

	    // comes in the format "uid=name"
	    name = result.getName();
	    name = name.split("=")[1];

	    // append master user if configured
	    if (fMasterUser != null) {
		name += MASTER_SEP + fMasterUser;
	    }

	    // try password extraction
	    atts = result.getAttributes().getAll();
	    while (atts.hasMore()) {
		a = atts.next();

		if (fPasswordField.equals(a.getID()) && a.get() != null) {
		    Object val = a.get();
		    if (val instanceof byte[]) {
			// hashed passwords
			// TODO: usable at all?
			password = new String((byte[]) val);
		    } else {
			// clear text passwords
			password = val.toString();
		    }
		    break;
		}
	    }

	    // master password insertion
	    if (fMasterPassword != null) {
		password = fMasterPassword;
	    }

	    // add name and password
	    fUserNames.add(name);
	    fUserPasswords.add(password);
	}
    }

    @Override
    public String getName() {
	return "LDAP login source";
    }

    @Override
    public List<String> getUserNames() {
	if (!fInitialized) {
	    try {
		read();
	    } catch (Exception e) {
		fLogger.error("Failed to get usernames", e);
	    }
	}

	return fUserNames;
    }

    @Override
    public List<String> getUserPasswords() {
	if (!fInitialized) {
	    try {
		read();
	    } catch (Exception e) {
		e.printStackTrace();
		fLogger.error("Failed to get passwords", e);
	    }
	}

	return fUserPasswords;
    }

    public void deactivate() {
	fActive = false;
    }

    @Override
    public void run() {
	//refresh loop
	while(fActive)
	{
	    try {
		read();
	    } catch (Exception e) {
		fLogger.error("Failed to refresh", e);
	    }

	    try {
		Thread.sleep(fRefreshInt);
	    } catch (InterruptedException e) {
	        // Restore the interrupted status
	        Thread.currentThread().interrupt();
	    }
	}
    }
}
