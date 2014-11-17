package Basic;

import java.util.*;

/**
 * Created by Administrator on 2014/11/17.
 */
public class CsparqlParser {
    public Map<String,String> pexi = new HashMap<String, String>();
    //public List<RdfNode> RNode = new ArrayList<RdfNode>();
    public void parse(String Csparql){
        String[] queryData = Csparql.split(".");
        int i=0;
        while(i<queryData.length){
            if(queryData[i].indexOf("register")==1){
                String[] register = queryData[i].split(" ");
            }else if(queryData[i].indexOf("prepix")==1){
                String[] prepix = queryData[i].split(" ");
            }else if(queryData[i].indexOf("select")==1) {
                String[] select = queryData[i].split(" ");
            }else if(queryData[i].indexOf("where")==1){
                String[] where = queryData[i].split(" ");
            }else if(queryData[i].indexOf("aggregate")==1){
                String[] aggregate = queryData[i].split(" ");
            }else if(queryData[i].indexOf("stream")==1){
                String[] stream = queryData[i].split(" ");
            }else if(queryData[i].indexOf("range")==1){
                String[] range = queryData[i].split(" ");
            }
        }
    }
    public static void main(String[] args){
        String Csparql = "SELECT ?sensor\n" +
                "FROM STREAM <http://www.cwi.nl/SRBench/observations> [RANGE 1h STEP 10m]\n" +
                "WHERE {\n" +
                "?observation om-owl:procedure ?sensor ;\n" +
                "om-owl:observedProperty weather:WindSpeed ;\n" +
                "om-owl:result [ om-owl:floatValue ?value ] . }\n" +
                "GROUP BY ?sensor HAVING ( AVG(?value) >= \"74\"^^xsd:float )";
        String[] line = Csparql.split("FROM");
        if(line[0].indexOf("SELECT")==1){
            String[] selectParam = line[0].substring(line[0].indexOf("SELECT")).split(" ");
            int i=0;
            while (i<selectParam.length){
                System.out.println(selectParam[i]);
                i++;
            }
        }
    }
}
