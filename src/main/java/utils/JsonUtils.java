package utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.RandomStringUtils;
/**
 * @author pandaqyang
 * @date 2019/11/12 21:52
 */
public class JsonUtils {
//    public static String indexHeader = "{ \"index\" : { \"_index\" : \" + index + \", \"_type\" :  \" + type + \"} }";
    public static String indexHeader = "{ \"index\" : {  \"_id\" :  \"docs_id_match\"} }";
    public static String indexHeader2 = "{ \"index\" : {}}";
    static TupleUtil[] tuples = DataUtils.getTuple();

    public static String getStringJsonArray(int dataSize) {
//        String jsonResult = null;
        JSONArray jsonArray = new JSONArray();
        for (int i = 0; i < dataSize; i++) {
            JSONObject jsonObject = new JSONObject();
            for (int j = 0;j < tuples.length;j++){
                if (tuples[j].getIsString()){
                    jsonObject.put(DataUtils.getFields()[j], simuString(tuples[j].getLength()));
                }
                else {
                    jsonObject.put(DataUtils.getFields()[j],simuLong(tuples[j].getLength()));
                }
            }
            jsonArray.add(jsonObject);
        }
        return jsonArray.toJSONString().replace("[","").replace("]","");
    }

    public static String getStringJson() {
            JSONObject jsonObject = new JSONObject();
            for (int j = 0;j < tuples.length;j++){
                if (tuples[j].getIsString()){
                    jsonObject.put(DataUtils.getFields()[j], simuString(tuples[j].getLength()));
                }
                else {
                    jsonObject.put(DataUtils.getFields()[j],simuLong(tuples[j].getLength()));
                }
            }
        return jsonObject.toJSONString();
    }

    public static String getIndexRequests(int dataSize){
        StringBuffer sb = new StringBuffer();
        for (int i=0;i<dataSize;i++){
            sb.append(indexHeader.replace("docs_id_match",simuString(20))).append("\n");
            sb.append(getStringJson()).append("\n");
        }
        return sb.toString();
    }

    public static String getIndexRequestsWithoutDocId(int dataSize){
        StringBuffer sb = new StringBuffer();
        for (int i=0;i<dataSize;i++){
            sb.append(indexHeader2).append("\n");
            sb.append(getStringJson()).append("\n");
        }
        return sb.toString();
    }


    public static String simuString(int cLenth){
        RandomStringUtils randomStringUtils = new RandomStringUtils();
        String str = randomStringUtils.randomAlphanumeric(cLenth);
        return str;
    }
    public static long simuLong(int cLenth){
        long l = System.currentTimeMillis();
        if (cLenth > 13){
            for (int i=0;i<(cLenth-13);i++){
                l *= 10;
            }
        }
        else{
            Long index = 1L;
            for (int j=0;j<cLenth;j++){
                index *= 10;
            }
            l %= index;
        }
        return l;
    }

    public static void main(String[] args) {
        String str = getStringJsonArray(2);
        String str1 = str;
        System.out.println(str);
        str = str.replace("[","").replace("]","");
        System.out.println(str);
        str1 = str1.replace("[","");
        str1 = str1.replace("]","");
        System.out.println(str1);
        System.out.println(getStringJson());
        System.out.println(indexHeader);
    }
}
