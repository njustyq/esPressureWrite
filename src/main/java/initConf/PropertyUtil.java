package initConf;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * @author pandaqyang
 * @date 2019/11/12 19:26
 */
public class PropertyUtil {
    private int connectTimeout;
    private int socketTimeout;
    private int maxRetryTimeoutMillis;
    private String clusterNodeList;
    private String fields;
    private String originalData;
    private String indexName;
    private String mappingType;
    private boolean specifyDocId = true;
    private static PropertyUtil INSTANCE = new PropertyUtil();
    private PropertyUtil(){
        try {
            Properties pro = new Properties();
            pro.load(new FileInputStream("conf/config.properties"));
            connectTimeout = Integer.parseInt(pro.getProperty("connectTimeout", String.valueOf(6000)));
            socketTimeout = Integer.parseInt(pro.getProperty("socketTimeout", String.valueOf(6000)));
            maxRetryTimeoutMillis = Integer.parseInt(pro.getProperty("maxRetryTimeoutMillis", String.valueOf(6000)));
            clusterNodeList = pro.getProperty("clusterNodeList");
            fields = pro.getProperty("fields");
            originalData = pro.getProperty("originalData");
            indexName = pro.getProperty("indexName");
            mappingType = pro.getProperty("mappingType");
            specifyDocId = Boolean.parseBoolean(pro.getProperty("specifyDocId", String.valueOf(true)));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static PropertyUtil getINSTANCE() {
        return INSTANCE;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public int getMaxRetryTimeoutMillis() {
        return maxRetryTimeoutMillis;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public String getClusterNodeList() {
        return clusterNodeList;
    }

    public String getFields() {
        return fields;
    }

    public String getOriginalData() {
        return originalData;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getMappingType() {
        return mappingType;
    }

    public boolean isSpecifyDocId() {
        return specifyDocId;
    }

    public static void main(String[] args) {

    }
}
