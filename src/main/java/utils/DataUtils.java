package utils;

import initConf.PropertyUtil;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author pandaqyang
 * @date 2019/11/12 21:38
 */
public class DataUtils {
//    private static String[] fields = ("timestamp,create_time,listid,pur_type,modify_time,total_fee,coding,memo,lstate,state,spid,acc_time,refund_fee,trade_id,fund_code,real_redem_amt,trade_date,fund_vdate,rela_listid,cft_trans_id,cft_bank_billmo,sub_trans_id,cur_type,purpose,spe_tag,sign,fetch_sync,refund_type,refund_reason,pay_channel,loading_type,loading_cuin,loading_listid,transit_spid,cft_refund_type,uid,fetch_result,Fstandby1,fetch_fail_reason,Fstandby2,data_version,balance,balance_time,balance_time_stamp,balance_show,bk_id").split(",");
//    private static String[] originalData = ("1573628784828,9221105674366605024,18000070021906040024664285863673,9221105672806989835,1559615200,aaaaa,aaaaa,1aaaaa,aaaaaaaaaa1,3aaaaa,1800007002,1559615199,0,201701211018864662,000343,400000,20190604,20190605,1,1,18000070021906040024664285863673,1000098601190604001100146642858650405673,5107,0,0,1,0,0,0,5,0,1,1,1,0,690811673,0,0,2,400000,9221105674366605023,0,400000,1").split(",");
    private static String[] fields = PropertyUtil.getINSTANCE().getFields().split(",");
    private static String[] originalData = PropertyUtil.getINSTANCE().getOriginalData().split(",");
    public static Pattern pattern = Pattern.compile("[0-9]*");
    static TupleUtil[] tuple = new TupleUtil[originalData.length];
    public static boolean isNumeric(String str){
        Matcher isNum = pattern.matcher(str);
        if( !isNum.matches() ){
            return false;
        }
        return true;
    }
    static void initTuple(){
        for (int i=0;i<tuple.length;i++){
            tuple[i] = new TupleUtil(!isNumeric(originalData[i]),originalData[i].length());
        }
    }
    static {
        initTuple();
    }

    public static TupleUtil[] getTuple() {
        return tuple;
    }

    public static String[] getFields(){
        return fields;
    }
    public static void main(String[] args) {
        System.out.println(isNumeric(""));
        System.out.println(fields.length);
        System.out.println(originalData.length);
        System.out.println("20180830753182277392211056743666050241800007002190604002466428586367392211056728069898351559615200aaaaaaaaaa1aaaaaaaaaaaaaaa13aaaaa1800007002155961519902017012110188646620003434000002019060420190605111800007002190604002466428586367310000986011906040011001466428586504056735107001000501110690811673002400000922110567436660502304000001".length());
        StringBuffer strFieldBuffer = new StringBuffer();
        StringBuffer longFieldBuffer = new StringBuffer();
        for (int i=0;i<tuple.length;i++){
            if (tuple[i].getIsString()){
                strFieldBuffer.append(fields[i]).append(",");
            }
            else {
                longFieldBuffer.append(fields[i]).append(",");
            }
        }
        System.out.println(strFieldBuffer.toString());
        System.out.println(strFieldBuffer.toString().split(",").length);
        System.out.println(longFieldBuffer.toString().split(",").length);
    }
}
