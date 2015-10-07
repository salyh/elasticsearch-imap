package de.saly.elasticsearch.importer.imap.support;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;

public class DeleteByQuery {

    public static BulkResponse deleteByQuery(Client client, String index, String[] types, QueryBuilder queryBuilder) {
        
        SearchResponse scrollResp = client.prepareSearch().setIndices(index).setTypes(types).setSearchType(SearchType.SCAN).setQuery(queryBuilder)
                .setScroll(new TimeValue(1000)).setSize(1000).execute().actionGet();

        BulkRequestBuilder brb = client.prepareBulk();
        
        while (true) {
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(1000)).execute().actionGet();
            boolean hitsRead = false;
            for (final SearchHit hit : scrollResp.getHits()) {
                hitsRead = true;
                brb.add(new DeleteRequest(hit.getIndex(), hit.getType(), hit.getId()));
            }
            if (!hitsRead) {
                break;
            }
        }
        
        if(brb.numberOfActions() == 0) {
            return null;
        }
        
        return brb.get();
    }
    
    
    
}
