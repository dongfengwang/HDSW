package Storage;
import Basic.RdfNode;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;

/**
 * Created by Administrator on 2014/11/9.
 */
public class TupleReducer extends TableReducer<RdfNode,IntWritable,ImmutableBytesWritable> {
    @Override
    public void reduce(RdfNode key, Iterable<IntWritable> values, Context context) throws InterruptedException {

        String k = key.toString();
        StringBuffer str=null;
        int sum=0;
        for(IntWritable value: values){
            sum+=value.get();
        }
        String S = key.S;
        String P = key.P;
        String O = key.O;

        Put putrow = new Put(S.getBytes());
        putrow.add("P".getBytes(), P.getBytes(), O.getBytes());
        putrow.add("O".getBytes(), O.getBytes(), P.getBytes());
        putrow.add("PO".getBytes(), (P+"+"+O).getBytes(), S.getBytes());

    }
}
