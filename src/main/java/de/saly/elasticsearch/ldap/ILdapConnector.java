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

import javax.naming.NamingEnumeration;
import javax.naming.directory.DirContext;
import javax.naming.directory.ModificationItem;

/**
 * Interface for LDAP connectors that manage domains and other prefixes and
 * suffixes and offer manipulation functionality. All names are considered UIDs.
 */
public interface ILdapConnector {
    /**
     * Establishes a connection to the specified LDAP directory service.
     *
     * @throws Exception
     *             if creating the connection fails
     */
    void connect() throws Exception;

    /**
     * Disconnects any established connection to the LDAP directory service.
     *
     * @throws Exception
     *             if disconnecting fails
     */
    void disconnect() throws Exception;

    /**
     * @return whether a connection is currently established
     */
    boolean isConnected();

    /**
     * Queries the LDAP directory service for the given name of a context or
     * object which may not be null.
     *
     * @param name
     *            name (UID) of the context or object to search
     * @return query result
     * @throws Exception
     *             if the query is flawed or fails
     */
    @SuppressWarnings("rawtypes")
    NamingEnumeration nameQuery(String name) throws Exception;

    /**
     * Queries the LDAP directory service for all contexts and objects that
     * match the given filter expression which may not be null or empty.
     *
     * @param filter
     *            filter expression to use
     * @return query result
     * @throws Exception
     *             if the query is flawed or fails
     */
    @SuppressWarnings("rawtypes")
    NamingEnumeration filterQuery(String filter) throws Exception;

    /**
     * Queries the LDAP directory service for the given name of a context or
     * object and filters the results with the given expression. None of the
     * parameters may be null.
     *
     * @param name
     *            name (UID) of the context or object to search
     * @param filter
     *            filter expression to use
     * @return query result
     * @throws Exception
     *             if the query is flawed or fails
     */
    @SuppressWarnings("rawtypes")
    NamingEnumeration query(String name, String filter) throws Exception;

    /**
     * Carries out the given modifications on the specified directory entry.
     * None of the parameters may be null.
     *
     * @param name
     *            name (UID) of the entity to modify
     * @param mods
     *            attribute modifications
     * @throws Exception
     *             if parameters are flawed or the operation fails
     */
    void update(String name, ModificationItem[] mods) throws Exception;

    /**
     * Creates the entity as defined by the directory context and the given
     * name. No parameter may be null or empty.
     *
     * @param name
     *            name (UID) of the entity to create
     * @param object
     *            entity to store with initial attributes to set
     * @throws Exception
     *             if the creation fails
     */
    void create(String name, DirContext object) throws Exception;

    /**
     * Removes an entry from the LDAP directory as defined by the given name.
     * Should be used with caution.
     *
     * @param name
     *            name (UID) of the entity to remove
     * @throws Exception
     *             if the removal fails
     */
    void remove(String name) throws Exception;
}
