import client.ElasticsearchClient;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import initConf.PropertyUtil;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import utils.WriteThread;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * @author pandaqyang
 * @date 2019/11/13 11:47
 */
public class PressureWrite {

    public static void main(String[] args) {
        args = new String[]{"20", "2", "2"};
        if (args.length != 3) {
            System.out.println("args must be \"groups\" \"dataSizePerGroup\" \"threadNum\"");
            return;
        }
        int groups = Integer.parseInt(args[0]);
        int dataSizePerGroup = Integer.parseInt(args[1]);
        int threadNum = Integer.parseInt(args[2]);
        String index = PropertyUtil.getINSTANCE().getIndexName();
        String type = PropertyUtil.getINSTANCE().getMappingType();
        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        WriteThread[] writeThread = new WriteThread[threadNum];
        for (int i = 0; i < threadNum; i++) {
            writeThread[i] = new WriteThread(index, type, groups, dataSizePerGroup, countDownLatch, PropertyUtil.getINSTANCE().isSpecifyDocId());
        }
        long startTime = System.currentTimeMillis();
        System.out.println("start time at: " + startTime);
        for (int i = 0; i < threadNum; i++) {
            writeThread[i].start();
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long endTime = System.currentTimeMillis();
        System.out.println("end time at: " + endTime);
        System.out.println("cos time: " + (endTime - startTime) + "ms");

        RestClient restClient = ElasticsearchClient.getRestClient();
        Response rsp = null;
        Map<String, String> params = Collections.singletonMap("pretty", "true");
        String queryString = "{\n" +
                "  \"query\": {\n" +
                "    \"range\": {\n" +
                "      \"timestamp\": {\n" +
                "        \"gte\": " + startTime + ",\n" +
                "        \"lte\": " + endTime + "\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        HttpEntity entity = new NStringEntity(queryString, ContentType.APPLICATION_JSON);
        try {
            Thread.sleep(3000);
            rsp = restClient.performRequest("GET", "/" + index + "/_search", params, entity);
            String responseBody = EntityUtils.toString(rsp.getEntity());
            System.out.println("********************************************");
            JSONObject jsonObject = JSON.parseObject(responseBody);
            String jsonObejectStr = jsonObject.get("hits").toString();
            JSONObject jsonObject1 = JSON.parseObject(jsonObejectStr);
            Integer totalHitsInt = (Integer) jsonObject1.get("total");
            long totalHits = Long.valueOf(totalHitsInt);
            System.out.println("the number of the data is: " + totalHits);
            System.out.println("********************************************");
            System.out.println("the qps is: " + (totalHits * 1000L) / (endTime - startTime));
            System.out.println("********************************************");
            restClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            try {
                if (restClient != null) {
                    restClient.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

    }
}
