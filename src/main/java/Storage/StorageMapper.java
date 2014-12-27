package Storage;

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class StorageMapper extends Mapper<LongWritable,Text,Text,Text> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] arr = value.toString().split("	");
        Text outKey = new Text();
        Text outValue = new Text();
    	if(arr.length>2){
    		outKey.set(arr[0].toString());
    		outValue.set(arr[1].toString()+" "+arr[2].toString());
    		//System.out.print("outkey:"+outKey+" outValue:"+outValue);
            context.write(outKey,outValue);
    	}else if(arr.length==2){
    		outKey.set(arr[0]);
    		outValue.set(arr[1]);
    		//System.out.print("outkey:"+outKey+" outValue:"+outValue);
            context.write(outKey,outValue);
    	}
    	
	}

}
