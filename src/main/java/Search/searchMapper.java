package Search;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Administrator on 2014/11/29.
 */
public class searchMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Text> {
    private ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable();
    @Override
    protected void map(LongWritable key, Text value,
                       Context context)
            throws IOException, InterruptedException {
        String rowkey = value.toString().split(":")[0];
        immutableBytesWritable.set(Bytes.toBytes(rowkey));
        context.write(immutableBytesWritable, value);
        System.out.println(rowkey+" "+value);
    }
}