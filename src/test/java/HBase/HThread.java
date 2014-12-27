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
 * Created by Administrator on 2014/12/27.
 */
public class HThread extends Thread {

    /**
     * 重写（Override）run()方法 JVM会自动调用该方法
     */
    String sql="";
    public HThread(String sql){
        this.sql=sql;
    }
    public void run() {
        CSparqlParser cParser = new CSparqlParser();
        CSparqlModel cModel = new CSparqlModel();
        cModel=cParser.cParse(sql);
        Result rs = new Result();
        List<KeyValue> kvl =new ArrayList<KeyValue>();
        HBaseOperation hOperation = null;
        try {
            hOperation = new HBaseOperation();
        } catch (IOException e) {
            e.printStackTrace();
        }
        int i=0;
        tuple tu;
        while(i < cModel.where.size()){
            tu=cModel.where.get(i);
            System.out.println(tu.S+" "+tu.P+" "+tu.O);
            if(i==0){
                try {
                    kvl.addAll(whereMap1(tu, hOperation, rs));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }else{
                for(KeyValue kv : kvl) {
                    try {
                        kvl.addAll(whereMap2(tu, hOperation, rs, kvl));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            i++;
        }
    }
    /*
    *where第一个条件
    */
    public static List<KeyValue> whereMap1(tuple tu,HBaseOperation hOperation,Result rs) throws IOException {
        List<KeyValue> kvl =new ArrayList<KeyValue>();
        List<Result> rsl = new ArrayList<Result>();

        if(tu.S.indexOf("?")==-1){
            if(tu.P.indexOf("?")==-1){
                //S_P查找
                rs = hOperation.getOneRecordP("ods",tu.S,tu.P);
                if(tu.O.indexOf("?")==-1){
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
            }else if(tu.O.indexOf("?")==-1){
                rs = hOperation.getOneRecordO("ods",tu.S,tu.O);
                //通过S_O查找P
            }else{
                //通过S查找
                rs = hOperation.getOneRecord("ods",tu.S);
            }
        }else if(tu.P.indexOf("?")==-1){
            if(tu.O.indexOf("?")==-1){
                //通过P_O查找

            }else{
                System.out.println('P');
                rsl = hOperation.ScanRowByP("ods", "P", tu.P);
                for(Result rs1 : rsl){
                    System.out.println(rs1.getRow());
                }
                //通过P查找
            }
        }else if(tu.O.indexOf("?")==-1){
            //通过O查找
            //通过O查找
        }
        return kvl;
    }
    /*
   *where第一个条件
   */
    public static List<KeyValue> whereMap2(tuple tu,HBaseOperation hOperation,Result rs,List<KeyValue> kvlist) throws IOException {
        List<KeyValue> kvl = new ArrayList<KeyValue>();
        if(tu.P.indexOf("?")==-1){
            if(tu.O.indexOf("?")==-1){
                hOperation.QueryByFilter("ods","P",tu.P,tu.O);
            }else{
                hOperation.getOneRecordO("ods","O",tu.O);
            }
        }
        return kvl;
    }
}
