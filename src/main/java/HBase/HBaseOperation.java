package HBase;

/**
 * Created by Administrator on 2014/11/9.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class HBaseOperation {
    //.相关属性
    private  Configuration conf ;
    private  HBaseAdmin admin;
    static HBaseConfiguration cfg = null;
        static {
                Configuration HBASE_CONFIG = new Configuration();
                HBASE_CONFIG.set("hbase.zookeeper.quorum", "localhost");
                HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", "2181");
                cfg = new HBaseConfiguration(HBASE_CONFIG);

        }

    public  HBaseOperation(Configuration conf) throws  IOException{
        this.conf=HBaseConfiguration.create(conf);
        this.admin =new HBaseAdmin(this.conf);
    }
    public HBaseOperation() throws IOException{
        Configuration cnf = new Configuration();
        
        this.conf=HBaseConfiguration.create(cnf);
        this.admin=new HBaseAdmin(this.conf);
    }

    //1.创建表
    public void  createTable(String tableName,String colFamilies[]) throws IOException{

        if(this.admin.tableExists(tableName)){
            System.out.println("Table: "+tableName+" already exists !");
        }else{
            HTableDescriptor dsc = new HTableDescriptor(tableName);
            int len = colFamilies.length;
            for(int i=0;i<len;i++){
                HColumnDescriptor family = new HColumnDescriptor(colFamilies[i]);
                dsc.addFamily(family);
            }
            admin.createTable(dsc);
            System.out.println("创建表成功");
        }
    }
    //2.删除表
    public void deleteTable(String tableName) throws IOException{
        if(this.admin.tableExists(tableName)){
            admin.deleteTable(tableName);
            System.out.println("删除表成功");
        }else{
            System.out.println("Table Not Exists !");
        }
    }
    //3.插入一行记录
    public void insertRecord(String tableName,String rowkey,String family,String qualifier,String value,Long ts) throws IOException {

        HTable table = new HTable(this.conf,tableName);
        Put  put = new Put(rowkey.getBytes(),ts);
        put.add(family.getBytes(),qualifier.getBytes(),value.getBytes());
        table.put(put);

        System.out.println("插入行成功");
    }
    //4.删除一行记录
    public void deleteRecord(String tableName,String rowkey) throws IOException{

        HTable table = new HTable(this.conf,tableName);
        Delete del =new Delete(rowkey.getBytes());
        table.delete(del);
        System.out.println("删除行成功");
    }
    //5.获取一行记录
    public Result getOneRecord(String tableName,String rowkey) throws IOException{
        HTable table =new HTable(this.conf,tableName);
        Get get =new Get(rowkey.getBytes());
        Result rs = table.get(get);
        return rs;
    }
    /*
     *5.1 获取一行记录byP
     */
    public Result getOneRecordP(String tableName,String rowkey,String p) throws IOException {
        HTable table =new HTable(this.conf,tableName);
        Get get = new Get(rowkey.getBytes()).addColumn("P".getBytes(),p.getBytes());
        Result rs = table.get(get);
        return rs;
    }

    /*
     *5.2 获取一行记录 根据SO获取一行
     */
    public Result getOneRecordO(String tableName,String rowkey,String o) throws IOException {
        HTable table =new HTable(this.conf,tableName);
        Get get = new Get(rowkey.getBytes()).addColumn("O".getBytes(),o.getBytes());
        Result rs = table.get(get);
        return rs;
    }
    //6.获取所有记录
    public List<Result> getAllRecord(String tableName) throws IOException{

        HTable table = new HTable(this.conf,tableName);
        Scan scan = new Scan();
        //scan.setStartRow(Bytes.toBytes("sens-obs:System_A01"));
        //scan.setStopRow(Bytes.toBytes("sens-obs:System_A01_145950494"));
        //scan.setBatch(1000);
        ResultScanner scanner = table.getScanner(scan);
        List<Result> list =new ArrayList<Result>();
        for(Result r:scanner){
            list.add(r);
        }
        scanner.close();
        return list;
    }
    	/**
    	 * query Row based on P 
    	 * scan
    	 */
    public static List<Result> ScanRowByP(String tableName,String family,String qualifier) {
        List<Result> list =new ArrayList<Result>();
        try {
             HTablePool pool = new HTablePool(cfg, 1000);
                 //HTable table = (HTable) pool.getTable(tableName);
             Scan s = new Scan();
             s.addColumn(family.getBytes(), qualifier.getBytes());
             //s.setStartRow("sens-obs:System_A01_1090000000000".getBytes());
             //s.setStopRow("sens-obs:System_A01_1400000000000".getBytes());
           
             //s.setTimeRange(Long.parseLong("1090000000000"),Long.parseLong("1090800000000"));
             s.setCaching(0);
             
             ResultScanner rs = pool.getTable(tableName).getScanner(s);
                 for (Result r : rs) {
                     list.add(r);
                     /*System.out.println("rowkey:" + new String(r.getRow()));
                         for (KeyValue keyValue : r.raw()) {
                             System.out.println("Family：" + new String(keyValue.getFamily())
                             +";Qualifier:" + new String(keyValue.getQualifier())
                             + ";Vaule:" + new String(keyValue.getValue()));
                         }
                      */
                 }
             rs.close();
        } catch (Exception e) {
             e.printStackTrace();
        }
        return list;
    }
    /**
	 * query Row based on P 
	 * get()
     * @throws IOException 
	 */
	public static Result GetRowByP(String tableName,String rowkey,String family,String qualifier) throws IOException {
		HTable table =new HTable(cfg,tableName);
        Get get = new Get(rowkey.getBytes()).addColumn(family.getBytes(),qualifier.getBytes());
        Result rs = table.get(get);
        return rs;
	}

        /**
         * 单条件按查询，查询多条记录
         * @param tableName
         */
        public static List<Result> QueryByFilter(String tableName,String family,String qualifier,String value) {
            List<Result> list =new ArrayList<Result>();
            try {
                 HTablePool pool = new HTablePool(cfg, 1000);
                     //HTable table = (HTable) pool.getTable(tableName);
                     SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(family),
                        Bytes.toBytes(qualifier),
                        CompareFilter.CompareOp.EQUAL,Bytes.toBytes(value));
                     filter.setFilterIfMissing(true);

                 Scan s = new Scan();
                 s.setFilter(filter);
                 s.setStartRow("sens-obs:Observation_AirTemperature_A01_1090000000000".getBytes());
                 s.setStopRow("sens-obs:Observation_AirTemperature_A01_1400000000000".getBytes());
                 s.setCaching(0);
                 
                 
                 s.setTimeRange(1234049430, 1490949320);
                 ResultScanner rs = pool.getTable(tableName).getScanner(s);
                     for (Result r : rs) {
                         list.add(r);
                         System.out.println("rowkey:" + new String(r.getRow()));
                             for (KeyValue keyValue : r.raw()) {
                                 System.out.println("Family：" + new String(keyValue.getFamily())
                                 +";Qualifier:" + new String(keyValue.getQualifier())
                                 + ";Vaule:" + new String(keyValue.getValue()));
                             }
                     }
                 rs.close();
            } catch (Exception e) {
                 e.printStackTrace();
            }
            return list;
        }

        /*
        *P和O一起查询
        * @param tableName,families,qulifiers,value
         */
        public static List<Result> scanRowByPO(String tableName,String[] families,String[] qualifiers,String[] values) {
            List<Result> list = new ArrayList<Result>();
            try {
                HTablePool pool = new HTablePool(cfg, 100);
                //HTable table = (HTable) pool.getTable(tableName);
                List<Filter> filters = new ArrayList<Filter>();
                for(int index=0;index<families.length;index++){
                    SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(families[index]),
                            Bytes.toBytes(qualifiers[index]),
                            CompareFilter.CompareOp.EQUAL,Bytes.toBytes(values[index]));
                    filters.add(filter);
                }
                FilterList filterList = new FilterList(filters);
                Scan scan = new Scan();
                scan.setFilter(filterList);
                ResultScanner rs = pool.getTable(tableName).getScanner(scan);

                for (Result r : rs) {
                    list.add(r);
                    System.out.println("rowkey:" + new String(r.getRow()));
                    for (KeyValue keyValue : r.raw()) {
                        System.out.println("Family：" + new String(keyValue.getFamily())
                                +";Qualifier:" + new String(keyValue.getQualifier())
                                + ";Vaule:" + new String(keyValue.getValue()));
                    }
                }
                rs.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return list;
        }
        /**
        * 组合条件查询
        * @param tableName
        */
        public static void QueryByMultiFilter(String tableName,String[] families,String[] qualifiers,String[] values) {
        try {
            HTablePool pool = new HTablePool(cfg, 100);
                //HTable table = (HTable) pool.getTable(tableName);
                List<Filter> filters = new ArrayList<Filter>();
                for(int index=0;index<families.length;index++){
                    SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(families[index]),
                         Bytes.toBytes(qualifiers[index]),
                         CompareFilter.CompareOp.EQUAL,Bytes.toBytes(values[index]));
                    filters.add(filter);
                }
                FilterList filterList = new FilterList(filters);
                Scan scan = new Scan();
                scan.setFilter(filterList);
                ResultScanner rs = pool.getTable(tableName).getScanner(scan);
                for (Result r : rs) {
                    System.out.println("rowkey:" + new String(r.getRow()));
                    for (KeyValue keyValue : r.raw()) {
                        System.out.println("Family：" + new String(keyValue.getFamily())
                        +";Qualifier:" + new String(keyValue.getQualifier())
                        + ";Vaule:" + new String(keyValue.getValue()));
                    }
                }
                rs.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

}
