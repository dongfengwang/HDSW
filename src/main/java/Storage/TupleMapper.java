package Storage;

import Basic.RdfNode;
import Basic.RdfWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

/**
 * Created by Administrator on 2014/11/9.
 */
public class TupleMapper extends Mapper<IntWritable, Text, RdfNode, IntWritable> {
    public IntWritable one = new IntWritable(1);
    @Override
    public void map(IntWritable intWritable, Text value, Context context) throws IOException, InterruptedException {
        RdfWritable n;
        ReadTuple readT = new ReadTuple();
        n = readT.read(value);
        context.write(n,one);
    }
}
