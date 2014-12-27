package HBase;

import java.io.IOException;

/**
 * Created by Administrator on 2014/11/30.
 */


public class SearchTest {
    public static void main(String[] args) throws IOException {
        String sql = "SELECT ?sensor\n" +
                "FROM STREAM <http://www.cwi.nl/SRBench/observations> [RANGE 1h STEP 10m]\n" +
                "WHERE {" +
                "?observation om-owl:procedure ?sensor ;" +
                "om-owl:observedProperty weather:WindSpeed ;" +
                "om-owl:result ?result ;"
                + "?result om-owl:floatValue ?value . }\n" +
                "GROUP BY ?sensor\n" +
                "HAVING ( AVG(?value) >= 74 )";
        HThread hthread = new HThread(sql);
        hthread.run();

    }
}

