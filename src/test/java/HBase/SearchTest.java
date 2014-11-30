package HBase;

import SPARQL.CSparqlModel;
import SPARQL.CSparqlParser;
import SPARQL.tuple;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2014/11/30.
 */
public class SearchTest {
    public static void main() throws IOException {
        String sql = "SELECT ?sensor\n" +
                "FROM STREAM <http://www.cwi.nl/SRBench/observations> [RANGE 1h STEP 10m]\n" +
                "WHERE {" +
                "?observation om-owl:procedure ?sensor ;" +
                "om-owl:observedProperty weather:WindSpeed ;" +
                "om-owl:result [ om-owl:floatValue ?value ] . }\n" +
                "GROUP BY ?sensor\n" +
                "HAVING ( AVG(?value) >= 74 )";

        CSparqlParser cParser = new CSparqlParser();
        CSparqlModel cModel = new CSparqlModel();
        cModel=cParser.cParse(sql);
        Result rs = new Result();
        List<KeyValue> kvl =new ArrayList<KeyValue>();
        HBaseOperation hOperation = new HBaseOperation();
        int i=0;
        for(tuple tu : cModel.where){
            if(i==0){
                kvl.addAll(whereMap1(tu, hOperation, rs));
            }else{
                kvl.addAll(whereMap2(tu,hOperation,rs,kvl));
            }
            i++;

        }
    }
    /*
    *where第一个条件
    */
    public static List<KeyValue> whereMap1(tuple tu,HBaseOperation hOperation,Result rs) throws IOException {
        List<KeyValue> kvl =new ArrayList<KeyValue>();
        if(tu.S.indexOf("?")!=-1){
            if(tu.P.indexOf("?")!=-1){
                //S_P查找
                rs = hOperation.getOneRecordP("ods",tu.S,tu.P);
                if(tu.O.indexOf("?")!=-1){
                    //存在O值
                    for(KeyValue kv : rs.raw()){
                        if(kv.getValue().toString().equals(tu.O)==true){
                            kvl.add(kv);
                        }
                    }
                }else{
                    for(KeyValue kv : rs.raw()) {
                        kvl.add(kv);
                    }
                }
            }else if(tu.O.indexOf("?")!=-1){
                //通过S_O查找P
            }else{
                //通过S查找
            }
        }else if(tu.P.indexOf("?")!=-1){
            if(tu.O.indexOf("?")!=-1){
                //通过P_O查找
            }else{

                //通过P查找
            }
        }else if(tu.O.indexOf("?")!=-1){
            //通过O查找
            //通过O查找
        }
        return kvl;
    }
     /*
    *where第一个条件
    */
    public static List<KeyValue> whereMap2(tuple tu,HBaseOperation hOperation,Result rs,List<KeyValue> kvlist){
        List<KeyValue> kvl = new ArrayList<KeyValue>();
        if(tu.P.indexOf("?")!=-1){
            if(tu.O.indexOf("?")!=-1){
                hOperation.QueryByFilter("ods","P",tu.P,tu.O);
            }else{

            }
        }
        return kvl;
    }
}
