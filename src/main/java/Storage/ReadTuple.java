package Storage;

import Basic.RdfNode;
import Basic.RdfWritable;

import java.lang.Object;

/**
 * Created by Administrator on 2014/11/9.
 */
public class ReadTuple {
    public RdfWritable n = new RdfWritable();
    public RdfWritable read(Object value){
        String[] val = value.toString().split("	");
        System.out.println(value);
        if(val.length>3){
        	n = new RdfWritable(val[0],val[1],val[2],"");
        }else if(val.length==2){
        	n = new RdfWritable(val[0],val[1],"","");
        }else{
        	n = new RdfWritable("","","","");
        }
        return n;
    }
}
