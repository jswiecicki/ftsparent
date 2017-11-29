package com.CSBFTS.EventGenerator;

import com.CSBFTS.Config.ServerConfig;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;

import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;



import java.io.IOException;

import java.util.Set;

/**
 * used to clean up the es database after each test
 */
public class ESCleanUp {
    final static String index = "accounts_index";
    final static String type = "accountnew";


    public static void removeElasticsearchEntries(Set<Integer> keySet){
        RestClient client  = RestClient.builder(
                new HttpHost(ServerConfig.ELASTICSEARCH_IP, 9200, "http")).build();

        RestHighLevelClient hlClient = new RestHighLevelClient(client);

        BulkRequest request = new BulkRequest();
        for(Integer key : keySet) {
            request.add(new DeleteRequest(index, type, key.toString()));
        }


        BulkResponse bulkResponse = null;
        try {
            bulkResponse = hlClient.bulk(request);
            if(bulkResponse.hasFailures()) {
                for (BulkItemResponse bulkItemsResponse : bulkResponse){
                    if (bulkItemsResponse.isFailed()) {
                        BulkItemResponse.Failure failure = bulkItemsResponse.getFailure();
                        System.out.println(failure.toString());
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
