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
    long cosTime = 0L;

    public WriteThread(String index, String type, int groups, int dataSize, CountDownLatch countDownLatch, boolean specifyDocId) {
        this.index = index;
        this.type = type;
        this.dataSize = dataSize;
        this.groups = groups;
        this.countDownLatch = countDownLatch;
        this.specifyDocId = specifyDocId;
    }

    @Override
    public void run() {
        long cosTimeOnce = 0L;
        long maxCosTime = 0L;
        super.run();
        try {
            for (int i = 0; i < groups; i++) {
                cosTimeOnce = BulkService.bulk(restClient, index, type, dataSize, specifyDocId);
                cosTime += cosTimeOnce;
                maxCosTime = cosTimeOnce > maxCosTime?cosTimeOnce:maxCosTime;
//                System.out.println(Thread.currentThread().getName() + " : avgCosTime: "+(cosTime/(i+1))+"ms, maxCosTime: "+maxCosTime+"ms");
            }
            System.out.println(Thread.currentThread().getName() + " : avgCosTime: "+(cosTime/groups)+"ms, maxCosTime: "+maxCosTime+"ms");
            countDownLatch.countDown();
            restClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (restClient != null) {
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
