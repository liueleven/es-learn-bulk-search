package es;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * @description: 获取 client
 * @date: 2019-10-31 15:19
 * @author: 十一
 */
public class ESClient {

    private static volatile RestHighLevelClient client = null;

    public static RestHighLevelClient getClient() {
        if (client == null) {
            synchronized (ESClient.class) {
                if (client == null) {
                    RestClientBuilder restClientBuilder = RestClient.builder(
                            new HttpHost("localhost", 9200, "http"),
                            new HttpHost("localhost", 8200, "http"),
                            new HttpHost("localhost", 7200, "http"));
                    restClientBuilder.setFailureListener(new RestClient.FailureListener() {
                        public void onFailure(HttpHost host) {
                            System.out.println("host " + host.getHostName() + "失败");
                        }
                    });
                    client = new RestHighLevelClient(restClientBuilder);
                }
            }
        }
        return client;
    }
}
