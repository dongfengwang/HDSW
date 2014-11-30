package HBase;/**
 * Created by Administrator on 2014/11/24.
 */

import javafx.application.Application;
import javafx.stage.Stage;

public class CSPARQLParserTest extends Application {

    public static void main(String[] args) {
        launch(args);
    }

    @Override
    public void start(Stage primaryStage) {
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
