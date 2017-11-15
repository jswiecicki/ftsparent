/*package com.CSBFTS.PerformanceAnalysis;

import com.CSBFTS.Config.ServerConfig;
import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.net.UnknownHostException;

public class ElasticConnection {

    private static String ipAddress = ServerConfig.ELASTICSEARCH_IP;
    //private static int port;


    public ElasticConnection() {
        //this.ipAddress= ServerConfig.ELASTICSEARCH_IP;
        //this.port = port;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public static boolean elasticConnection(String json) {
        try {

            RestClient lowLevelRestClient = RestClient.builder(
                    new HttpHost(ipAddress, 9200, "http")).build();

            RestHighLevelClient client =
                    new RestHighLevelClient(lowLevelRestClient);

            int status = indexData(client, json);
            String getResponse;


            //getResponse = getLog(client);

            System.out.println("Response: "+status);
            //System.out.println("Response: " +getResponse);



            lowLevelRestClient.close();
            return true;

        } catch (UnknownHostException ex) {
            System.out.println("Unknown host exception");
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
            return false;
        }
        return true;
    }

    public static int indexData(RestHighLevelClient client, String json) {
        try {
            IndexRequest request = new IndexRequest("demo", "tweet", "testID");
            request.source(json, XContentType.JSON);

            IndexResponse response = client.index(request);

            int status = response.status().getStatus();

            return status;
        }
        catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
    }

    public static String getData(RestHighLevelClient client) {
        try {
            GetRequest request = new GetRequest("demo", "tweet", "testID");
            //Header h = new BasicHeader("GET",);
            GetResponse response = client.get(request);
            //System.out.println("Response: "+response.toString());
            return response.toString();
        } catch(IOException e) {
            e.printStackTrace();
        }
        //GetResponse response = client.prepareGet("demo", "tweet", "testID").get();
        return "null";
    }

    public static String getLog(RestHighLevelClient client) {
        try {
            GetRequest request = new GetRequest("ES_HOME", "logs","my_es_cluster_index_indexing_slowlog.log");
            GetResponse response = client.get(request);

            //ESLogger logger = ESLoggerFactory.getLogger("logging.yml");


            //System.out.println(response.toString());
            return response.toString();
        } catch(IOException e) {
            e.printStackTrace();
        }
        return "";
    }

}
*/