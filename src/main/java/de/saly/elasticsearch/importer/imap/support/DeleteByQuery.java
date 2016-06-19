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

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;

public class DeleteByQuery {

    public static BulkResponse deleteByQuery(final Client client, final String index, final String[] types,
            final QueryBuilder queryBuilder) {

        //TODO use delete by query plugin
        //https://github.com/elastic/elasticsearch/tree/2.3/plugins/delete-by-query
        final int size = 10000;
        
        final SearchRequestBuilder searchBuilder = client.prepareSearch().setIndices(index).setTypes(types).setQuery(queryBuilder).setSize(size);
        
        SearchResponse deleteResponse = searchBuilder.get();

        final BulkRequestBuilder brb = client.prepareBulk();

        while (deleteResponse.getHits().getTotalHits() > 0) {
            for (final SearchHit hit : deleteResponse.getHits()) {
                brb.add(new DeleteRequest(hit.getIndex(), hit.getType(), hit.getId()));
            }
            
            brb.setRefresh(true).get();
            
            deleteResponse = searchBuilder.get();
        }
        
        return null;
    }

}
