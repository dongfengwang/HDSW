package Storage;

import java.io.DataInput;
import java.io.IOException;

import Basic.RdfWritable;
import HBase.HBaseOperation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

/**
 * Created by Administrator on 2014/11/9.
 */
public class TupleReducer extends TableReducer<Text,Text,NullWritable> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException {
    	//1.初始化HBaseOperation

        Configuration conf = new Configuration();
        //与hbase/conf/hbase-site.xml中hbase.zookeeper.quorum配置的值相同
        conf.set("hbase.zookeeper.quorum", "localhost");
        //与hbase/conf/hbase-site.xml中hbase.zookeeper.property.clientPort配置的值相同
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        HBaseOperation hbase = new HBaseOperation(conf);

        //2.测试相应操作

        //2.1创建表

        String tableName = "sds";
        
        Long ts = (long) 0 ;
        String S = key.toString();
        System.out.println(S);
        String P = ""; 
        String O = "";
        for(Text value: values){
            String[] arr = value.toString().split(" ");
            if(arr.length==3){
            	P = arr[0];
            	O = arr[1];
            	ts = Long.parseLong(arr[2]);
            }else if(arr.length==2){
            	P = arr[0];
            	O = arr[1];
            }else if (arr.length==1){
            	P = arr[0];
            }
            Put putrow = new Put(S.getBytes());
            putrow.add(Bytes.toBytes("P"),Bytes.toBytes(P), Bytes.toBytes(O));
            putrow.add("O".getBytes(), O.getBytes(), P.getBytes());
            putrow.add("PO".getBytes(), (P+"+"+O).getBytes(), S.getBytes());
            context.write(NullWritable.get(), putrow);  
        }

         
    }
}
