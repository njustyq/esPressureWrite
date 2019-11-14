package utils;

/**
 * @author pandaqyang
 * @date 2019/11/7 16:40
 */
public class TupleUtil {
    private int length = 0;
    private boolean isString = true;
    TupleUtil(boolean isString, int length){
        this.isString = isString;
        this.length = length;
    }
    public int getLength() {
        return length;
    }
    public boolean getIsString() {
        return isString;
    }
}
