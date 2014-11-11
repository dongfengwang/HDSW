package Storage;

import Basic.RdfNode;
import java.lang.Object;

/**
 * Created by Administrator on 2014/11/9.
 */
public class ReadTuple {
    public RdfNode n = new RdfNode();
    public RdfNode read(Object value){
        String[] val = value.toString().split(" ");
        n = new RdfNode(val[0],val[1],val[2]);
        return n;
    }
}
