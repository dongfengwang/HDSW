package HBase;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.List;

public class wdfTest {
	public static void main(String[] args) throws IOException{
		
		HBaseOperation hOperation = new HBaseOperation();
		/*
		List<Result> rl = hOperation.getAllRecord("sds");
		for(Result r1 : rl){
			for(KeyValue kv : r1.raw()){
				System.out.println("Family：" + new String(kv.getFamily())
	            +";Qualifier:" + new String(kv.getQualifier())
	            + ";Vaule:" + new String(kv.getValue()));
			}
		}
		*/
		
		Long startLs = System.currentTimeMillis();
		/*
		Result r = hOperation.getOneRecord("sds", "sens-obs:System_A02");
		for (KeyValue keyValue : r.raw()) {
            System.out.println("Family：" + new String(keyValue.getFamily())
            +";Qualifier:" + new String(keyValue.getQualifier())
            + ";Vaule:" + new String(keyValue.getValue()));
        }
		*/
		/*String[] families={"P"};
		String[] qualifiers={"om-owl:procedure"};
		String[] values={"sens-obs:System_A01"};
		*/
		List<Result> rl = hOperation.ScanRowByP("sds", "P", "om-owl:generatedObservation");

		for(int i=0;i<rl.size();i++){
			//System.out.println(i);
			Result r1=rl.get(i);
			for(KeyValue kv : r1.raw()){
				Result rl1=hOperation.GetRowByP("ods", new String(kv.getValue()),"P","om-owl:result");
				//System.out.println(new String(kv.getValue()));
				for(KeyValue kv1: rl1.raw()){
					if(new String(kv1.getKey()).indexOf("AirTemperature")!=-1){
						Result rl2 = hOperation.GetRowByP("ods", new String(kv1.getValue()),"P","om-owl:floatValue");
						for(KeyValue kv2:rl2.raw()){
							//System.out.println("sensor: "+new String(kv.getRow())+"airTemperature value: "+new String(kv2.getValue()));
						}
					}
				}
			}
		}
		System.out.println(rl.size());
		//hOperation.getOneRecordP("sds", "", "P:");
		//hOperation.QueryByFilter("ods", "P", "om-owl:procedure", "sens-obs:System_A02");
		Long endLs = System.currentTimeMillis();
		System.out.println("Run time is"+(endLs-startLs));
		
	}
}
