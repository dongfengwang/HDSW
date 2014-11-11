package Storage;

import Basic.RdfNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by Administrator on 2014/11/9.
 */
public class TupleDriver extends Configured implements Tool{
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum.", "localhost");
        Job job =new Job(conf);
        job.setJarByClass(TupleDriver.class);
        job.setJobName("Storage in HBase");
        job.setOutputKeyClass(RdfNode.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(TupleMapper.class);
        job.setReducerClass(TupleReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        TableMapReduceUtil.initTableReducerJob("HRDF", TupleReducer.class, job);
        boolean success=job.waitForCompletion(true);
        return success ? 0:1;
    }
    public static void main(String[] args) throws Exception
    {
        int ret= ToolRunner.run(new TupleDriver(), args);
        System.exit(ret);
    }
}