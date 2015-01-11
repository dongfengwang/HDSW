package Storage;

import Basic.TimeTransfer;
import HBase.HBaseOperation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

public class OdsReduce extends TableReducer<Text,Text,NullWritable> {
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
        String geneStr = "om-owl:generatedObservation";
        
        Long ts = (long) 0 ;
        Long Sts = (long)0,Pts = (long)0,Ots = (long)0;
        String S = key.toString();
        //System.out.println(S);
        String P = ""; 
        String O = "";
        for(Text value: values){
            String[] arr = value.toString().split(" ");
            if(arr.length==3){
            	P = arr[0];
            	O = arr[1];
            	
            }else if(arr.length==2){
            	P = arr[0];
            	O = arr[1];
            }else if (arr.length==1){
            	P = arr[0];
            }
            TimeTransfer timeT = new TimeTransfer();
            Map<String,String> mapS = new HashMap<String,String>();
            Map<String,String> mapP = new HashMap<String,String>();
            Map<String,String> mapO = new HashMap<String,String>();
            try {
    			mapS = timeT.getTs(S);
    			if(mapS.isEmpty()==false){
    				for(String Skey : mapS.keySet()){
    					S=Skey;
    					Sts = Long.parseLong(mapS.get(Skey));
    				}
    			}
    			mapP = timeT.getTs(P);
    			if(mapP.isEmpty()==false){
    				for(String Pkey : mapP.keySet()){
    					P=Pkey;
    					Pts = Long.parseLong(mapP.get(Pkey));
    				}
    			}
    			mapO = timeT.getTs(O);
    			if(mapO.isEmpty()==false){
    				for(String Okey : mapO.keySet()){
    					O=Okey;
    					Ots = Long.parseLong(mapO.get(Okey));
    				}
    			}
    		} catch (ParseException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		}
            if(Pts>0){
            	ts=Pts;
            }else if(Ots>0){
            	ts=Ots;
            }else if(Sts>0){
            	ts=Sts;
            }
            if(P.indexOf(geneStr)>=0){
            	hbase.insertRecord(tableName,S, "P", P,O,ts);
            	hbase.insertRecord(tableName,S, "P", P,O,ts);
            	hbase.insertRecord(tableName,S, "P", P,O,ts);
            }else{
            Put putrow;
	        if(ts>0){
	            putrow = new Put(S.getBytes(),ts);
	        }else{
	            putrow = new Put(S.getBytes());
	        }
	            System.out.println(S);
	            putrow.add("P".getBytes(),P.getBytes(), O.getBytes());
	            putrow.add("O".getBytes(), O.getBytes(), P.getBytes());
	            putrow.add("PO".getBytes(), (P+"+"+O).getBytes(), S.getBytes());
	            context.write(NullWritable.get(), putrow);
            }
        }
         
    }
}