package Storage;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import Basic.RdfNode;
import Basic.RdfWritable;

/**
 * Created by Administrator on 2014/11/9.
 */
public class TupleDriver extends Configured implements Tool{
    public final static Logger  logger = LoggerFactory.getLogger(TupleDriver.class);
    public int run(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum.", "localhost");
        
        Job job =new Job(conf);
        job.setJarByClass(TupleDriver.class);
        job.setJobName("Storage in HBase");
        
        job.setMapperClass(StorageMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        if(args[2].equals("O")){
        	TableMapReduceUtil.initTableReducerJob("ods", OdsReduce.class, job);	
        }else if(args[2].equals("S")){
        	TableMapReduceUtil.initTableReducerJob("sds", TupleReducer.class, job);
        }else if(args[2].equals("G")){
        	TableMapReduceUtil.initTableReducerJob("sds", genReducer.class, job);
        }
        Date startTime = new Date();
        logger.info("CoverageLogger: Job started " + startTime);
        boolean success=job.waitForCompletion(true);
        Date end_time = new Date();
        logger.info("CoverageLogger: Job end " + end_time);
        logger.info("CoverageLogger: job took " +
                (end_time.getTime() - startTime.getTime()) /1000 + " seconds.");
        return success?1:0;
    }
    public static void main(String[] args) throws Exception
    {
        int ret= ToolRunner.run(new TupleDriver(), args);
        System.exit(ret);
    }
}