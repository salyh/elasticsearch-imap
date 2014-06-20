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

/*
import java.util.Properties;

import javax.mail.Session;
import javax.mail.Store;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public class PooledStoreFactory extends BasePooledObjectFactory<Store> {

    private final String password;
    private final Properties props;
    private final String user;

    public PooledStoreFactory(final Properties props, final String user, final String password) {
        super();
        this.props = props;
        this.user = user;
        this.password = password;
    }

    @Override
    public Store create() throws Exception {
        final Session session = Session.getInstance(props, null);
        final Store store = session.getStore();
        store.connect(user, password);
        return store;
    }

    @Override
    public void destroyObject(final PooledObject<Store> p) throws Exception {
        final Store store = p.getObject();
        IMAPUtils.close(store);
    }

    @Override
    public boolean validateObject(final PooledObject<Store> p) {
        final Store store = p.getObject();
        return store != null && store.isConnected();
    }

    @Override
    public PooledObject<Store> wrap(final Store obj) {
        return new DefaultPooledObject<Store>(obj);
    }

}*/
