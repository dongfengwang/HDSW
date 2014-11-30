package Search;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by Administrator on 2014/11/29.
 */
public class searchDriver extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Search");
        job.setJarByClass(searchDriver.class);
        job.setMapperClass(searchMapper.class);
        job.setReducerClass(searchReducer.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(strings[0]));
        HFileOutputFormat.setOutputPath(job, new Path(strings[1]));
        Configuration HBASE_CONFIG = new Configuration();
        HBASE_CONFIG.set("hbase.zookeeper.quorum","bfdbjc2:2181,bfdbjc3:2181,bfdbjc4:2181");
        HBASE_CONFIG.set("hbase.rootdir", "hdfs://bfdbjc1:12000/hbase");
        HBASE_CONFIG.set("zookeeper.znode.parent","/hbase");
        String tableName = "t1";
        HTable htable = new HTable(HBASE_CONFIG, tableName);
        HFileOutputFormat.configureIncrementalLoad(job, htable);
        return job.waitForCompletion(true) ? 0 : 1;
    }
    public static void main(String[] args) throws Exception {
        int ret= ToolRunner.run(new searchDriver(), args);
        System.exit(ret);
    }
}
