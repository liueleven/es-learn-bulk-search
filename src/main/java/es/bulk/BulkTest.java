package es.bulk;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.text.csv.CsvData;
import cn.hutool.core.text.csv.CsvReader;
import cn.hutool.core.text.csv.CsvRow;
import cn.hutool.core.text.csv.CsvUtil;
import cn.hutool.json.JSON;
import cn.hutool.json.JSONUtil;
import es.ESClient;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @description: 插入数据
 * @date: 2019-10-31 15:22
 * @author: 十一
 */
public class BulkTest {

    private static RestHighLevelClient client = ESClient.getClient();
    public static BulkRequest bulkRequest = new BulkRequest();

    /**
     * 成功后会返回文档id
     * 然后可以在浏览器中搜索：http://localhost:9200/a/_search?q=name:张三
     */
    public  void bulkData(String indexName,String json) {
        // 索引名称是 a
        bulkRequest.add(new IndexRequest(indexName).source(json, XContentType.JSON));
        //#下面是一个监听器
        ActionListener<BulkResponse> listener = new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkResponse) {
                BulkItemResponse[] bulkItemResponses = bulkResponse.getItems();
                for (BulkItemResponse itemResponse : bulkItemResponses) {
                    if (itemResponse.isFailed()) {
                        System.out.println("文档id: " + itemResponse.getId() + " add data error");
                    } else {
                        System.out.println("文档id: " + itemResponse.getId() + " add data success");
                    }
                }
            }

            @Override
            public void onFailure(Exception e) {
                e.printStackTrace();
            }
        };
        // 执行发送，结果状态会在上面的监听器打印出来
        client.bulkAsync(bulkRequest, RequestOptions.DEFAULT, listener);
    }

    public static void main(String[] args) {
        BulkTest st = new BulkTest();
        String indexName = "tags";
//        userId,movieId,tag,timestamp
        Map<String,String> map = new HashMap<String,String>();

        CsvReader reader = CsvUtil.getReader();
        //从文件中读取CSV数据
        CsvData data = reader.read(FileUtil.file("tags.csv"));
        List<CsvRow> rows = data.getRows();
        //遍历行
        for (CsvRow row : rows) {
            //getRawList返回一个List列表，列表的每一项为CSV中的一个单元格（既逗号分隔部分）
            List<String> rawList = row.getRawList();
            map.put("userId",rawList.get(0));
            map.put("movieId",rawList.get(1));
            map.put("tag",rawList.get(2));
            map.put("timestamp",rawList.get(3));
            JSON parse = JSONUtil.parse(map);
            System.out.println(parse.toString());
            st.bulkData(indexName,parse.toString());
        }
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}