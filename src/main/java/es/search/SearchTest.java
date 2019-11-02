package es.search;

import es.ESClient;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;

/**
 * @description: ES 查询，需要自己构造数据
 * @date: 2019-10-31 15:53
 * @author: 十一
 */
public class SearchTest {

    public void scrollSearchID(String indexName) {
        int count = 0;
        int size = 10;
        RestHighLevelClient client = ESClient.getClient();
        try {
            SearchRequest searchRequest = new SearchRequest(indexName);
            //请求设置
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            // 设置匹配模式
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            // 根据年龄升序
            searchSourceBuilder.sort("age", SortOrder.ASC);
            searchSourceBuilder.fetchSource(false);
//          每次查询数量
            searchSourceBuilder.size(size);
//            位置指针的过期时间
            searchRequest.scroll(TimeValue.timeValueMinutes(5L));
            searchRequest.source(searchSourceBuilder);
//            请求发送
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            // 获取数据集合
            SearchHit[] searchHit = searchResponse.getHits().getHits();
            while ((searchHit != null && searchHit.length > 0)) {
                printData(searchHit,client);
                // 重置searchResponse 和ID
                String scrollId = searchResponse.getScrollId();
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(TimeValue.timeValueMinutes(5));
                searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
                // searchHit就是一个封装了查询的数据的一个类
                searchHit = searchResponse.getHits().getHits();
                System.out.println(searchHit.toString());
//                break;
            }
        } catch (IOException e) {
            e.printStackTrace();

        } finally {
            if (client != null) {
                try {
                    client.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    private void printData(SearchHit[] searchHit,RestHighLevelClient client) throws IOException {
        for (SearchHit documentFields : searchHit) {
            System.out.print("索引名称：" + documentFields.getIndex());
            System.out.println(documentFields.getSourceAsString());
            GetRequest getRequest = new GetRequest(documentFields.getIndex(), documentFields.getId());
            GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
            System.out.println(" , 数据：" + response.toString());
        }
    }

    public static void main(String[] args) {
        SearchTest searchTest = new SearchTest();
        String indexName = "megacorp";
        searchTest.scrollSearchID(indexName);
    }
}
