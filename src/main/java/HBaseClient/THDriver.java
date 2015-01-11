package HBaseClient;

/**
 * Created by Administrator on 2015/1/11.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;

public class THDriver extends Configured implements Tool{

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum.", "localhost");  //千万别忘记配置

        Job job = new Job(conf,"Txt-to-Hbase");
        job.setJarByClass(TxtHbase.class);

        Path in = new Path(args[0]);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, in);

        job.setMapperClass(THMapper.class);
        job.setReducerClass(THReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        if(args[2].equals("O")){
            TableMapReduceUtil.initTableReducerJob("ods", THReducer.class, job);
        }else if(args[2].equals("S")){
            TableMapReduceUtil.initTableReducerJob("sds", THReducer.class, job);
        }else if(args[2].equals("G")){
            TableMapReduceUtil.initTableReducerJob("sds", THReducer.class, job);
        }


        job.waitForCompletion(true);
        return 0;
    }

}
