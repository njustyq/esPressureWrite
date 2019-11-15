package bulk;

import utils.JsonUtils;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * @author pandaqyang
 * @date 2019/11/13 11:35
 */
public class BulkService {

    public static void bulkWithDocId(RestClient restClient, String index, String type,int dataSize)  {
        String indexRequests = JsonUtils.getIndexRequests(dataSize);
        StringEntity entity = new StringEntity(indexRequests, ContentType.APPLICATION_JSON);
        entity.setContentEncoding("UTF-8");
        Map<String, String> params = Collections.singletonMap("pretty", "true");
        Response rsp = null;
        try {
            rsp = restClient.performRequest("PUT", "/"+index+"/"+type+"/_bulk" ,params ,entity);
            if(HttpStatus.SC_OK != rsp.getStatusLine().getStatusCode()) {
                System.out.println("At:" + System.currentTimeMillis() + ",Bulk response entity is : " + EntityUtils.toString(rsp.getEntity()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    public static void bulkWithoutDocId(RestClient restClient, String index, String type,int dataSize) {
        String indexRequests = JsonUtils.getIndexRequestsWithoutDocId(dataSize);
        StringEntity entity = new StringEntity(indexRequests, ContentType.APPLICATION_JSON);
        entity.setContentEncoding("UTF-8");
        Map<String, String> params = Collections.singletonMap("pretty", "true");
        Response rsp = null;
        try {
            rsp = restClient.performRequest("PUT", "/"+index+"/"+type+"/_bulk" ,params ,entity);
            if(HttpStatus.SC_OK != rsp.getStatusLine().getStatusCode()) {
                System.out.println("At :" + System.currentTimeMillis() + ",Bulk response entity is : " + EntityUtils.toString(rsp.getEntity()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void bulk(RestClient restClient, String index, String type,int dataSize,boolean specifyDocId) {
        if (specifyDocId){
            bulkWithDocId(restClient,index,type,dataSize);
        }
        else {
            bulkWithoutDocId(restClient,index,type,dataSize);
        }

    }

}
