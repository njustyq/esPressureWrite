package utils;

import bulk.BulkService;
import client.ElasticsearchClient;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @author pandaqyang
 * @date 2019/11/13 11:53
 */
public class WriteThread extends Thread {
    String index;
    String type;
    public int dataSize = 0;
    public int groups = 0;
    CountDownLatch countDownLatch;
    RestClient restClient = ElasticsearchClient.getRestClient();
    boolean specifyDocId;
    public WriteThread(String index,String type,int groups,int dataSize,CountDownLatch countDownLatch,boolean specifyDocId){
        this.index = index;
        this.type = type;
        this.dataSize = dataSize;
        this.groups = groups;
        this.countDownLatch = countDownLatch;
        this.specifyDocId = specifyDocId;
    }
    @Override
    public void run() {
        super.run();
        try {
            for (int i = 0;i<groups;i++){
                BulkService.bulk(restClient,index,type,dataSize,specifyDocId);
            }
            countDownLatch.countDown();
            restClient.close();
        }catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (restClient != null){
                try {
                    restClient.close();
                    countDownLatch.countDown();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    public static void main(String[] args) {

    }
}
