package HBaseClient;

/**
 * Created by Administrator on 2015/1/11.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class TxtHbase {
    public static void main(String [] args) throws Exception{
        int mr;
        mr = ToolRunner.run(new Configuration(),new THDriver(),args);
        System.exit(mr);
    }
}