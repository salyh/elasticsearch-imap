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
package de.saly.elasticsearch.ldap;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.ModificationItem;
import javax.naming.directory.SearchControls;

/**
 * Simple plain text LDAP connector providing an interface to query an LDAP
 * directory service. Supports anonymous and user-password authentication. All
 * names are considered UIDs.
 */
public class SimpleLdapConnector implements ILdapConnector {
    private final String fUrl, fUser, fUserContext;
    private final char[] fPassword;

    private final SearchControls fCtrl;

    private final boolean fReadOnly;

    private DirContext fContext;
    private boolean fConnected;

    /**
     * Creates a simple LDAP connector using the given configuration. Throws a
     * NullPointerException if the specified URL is null or empty.
     *
     * TODO: parameters
     */
    public SimpleLdapConnector(String url, String context, String user,
	    String password, boolean readOnly) {
	fUrl = url;

	if (fUrl == null || fUrl.isEmpty()) {
	    throw new NullPointerException("no URL given");
	}

	if (context == null) {
	    fUserContext = "";
	} else {
	    fUserContext = "," + context;
	}

	fUser = user;
	fPassword = password.toCharArray();
	fReadOnly = readOnly;

	fCtrl = new SearchControls();
	fCtrl.setSearchScope(SearchControls.SUBTREE_SCOPE);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void connect() throws Exception {
	if (!fConnected) {
	    Hashtable env = new Hashtable();
	    env.put(Context.INITIAL_CONTEXT_FACTORY,
		    "com.sun.jndi.ldap.LdapCtxFactory");
	    env.put(Context.PROVIDER_URL, fUrl);

	    if (fUser != null && fPassword != null) {
		env.put(Context.SECURITY_AUTHENTICATION, "simple");
		env.put(Context.SECURITY_PRINCIPAL, fUser);
		env.put(Context.SECURITY_CREDENTIALS, new String(fPassword));
	    }

	    fContext = new InitialDirContext(env);

	    fConnected = true;
	}
    }

    public void disconnect() throws Exception {
	if (fConnected) {
	    fConnected = false;
	    fContext.close();
	}
    }

    public boolean isConnected() {
	return fConnected;
    }

    @SuppressWarnings("rawtypes")
    public NamingEnumeration nameQuery(String name) throws Exception {
	return query(name, "uid=*");
    }

    @SuppressWarnings("rawtypes")
    public NamingEnumeration filterQuery(String filter) throws Exception {
	return query("", filter);
    }

    @SuppressWarnings("rawtypes")
    public NamingEnumeration query(String name, String filter) throws Exception {
	if (!name.isEmpty()) {
	    name = "uid=" + name + fUserContext;
	} else {
	    name = fUserContext.substring(1);
	}

	return fContext.search(name, filter, fCtrl);
    }

    public void update(String name, ModificationItem[] mods) throws Exception {
	if (!fReadOnly && !name.isEmpty()) {
	    name = "uid=" + name + fUserContext;

	    fContext.modifyAttributes(name, mods);
	}
    }

    public void create(String name, DirContext object) throws Exception {
	if (!fReadOnly) {
	    if (!name.isEmpty()) {
		name = "uid=" + name + fUserContext;
	    }

	    fContext.bind(name, object);
	}
    }

    public void remove(String name) throws Exception {
	if (!name.isEmpty()) {
	    name = "uid=" + name + fUserContext;
	}

	fContext.destroySubcontext(name);
    }
}
