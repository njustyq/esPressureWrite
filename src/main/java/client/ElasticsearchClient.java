package client;

import initConf.PropertyUtil;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

/**
 * @author pandaqyang
 * @date 2019/11/12 16:47
 */
public class ElasticsearchClient {
//    private RestClient restClient = null;
//    private static ElasticsearchClient INSTANCE = new ElasticsearchClient();
    public ElasticsearchClient(){
//        initRestClient();
    }
//
//    public static ElasticsearchClient getINSTANCE() {
//        return INSTANCE;
//    }

    public static RestClient getRestClient() {
        return initRestClient();
    }

    private static RestClient initRestClient()  {
        RestClient restClient = null;
        RestClientBuilder builder = RestClient.builder(getHttpHost());
        Header[] defaultHeaders = new Header[] { new BasicHeader("Accept", "application/json"),
                new BasicHeader("Content-type", "application/json") };
        builder = builder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
            @Override
            public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                return requestConfigBuilder.setConnectTimeout(PropertyUtil.getINSTANCE().getConnectTimeout()).setSocketTimeout(PropertyUtil.getINSTANCE().getSocketTimeout());
            }
        }).setMaxRetryTimeoutMillis(PropertyUtil.getINSTANCE().getMaxRetryTimeoutMillis());
        builder.setDefaultHeaders(defaultHeaders);
        restClient = builder.build();
        return restClient;
    }

    private static HttpHost[] getHttpHost(){
        String[] nodes = PropertyUtil.getINSTANCE().getClusterNodeList().split(";");
        HttpHost[] httpHost = new HttpHost[nodes.length];
        String[] address;
        for (int i = 0 ; i < nodes.length ; i++){
            address = nodes[i].split(":");
            httpHost[i] = new HttpHost(address[0],Integer.parseInt(address[1]),"http");
        }
        return httpHost;
    }

}
