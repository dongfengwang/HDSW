package HBaseClient;

/**
 * Created by Administrator on 2015/1/11.
 */
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class THMapper extends Mapper<LongWritable,Text,Text,Text>{
    public void map(LongWritable key,Text value,Context context){
        String[] items = value.toString().split(" ");
        String k = items[0];
        String v = items[1];
        System.out.println("key:"+k+","+"value:"+v);
        try {

            context.write(new Text(k), new Text(v));

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

}