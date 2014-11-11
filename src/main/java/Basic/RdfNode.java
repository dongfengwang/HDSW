package Basic;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2014/11/9.
 */
public class RdfNode {
    public String S;
    public String P;
    public String O;
    public Map<String,String> PO = new HashMap<String, String>();
    public Map<String,Map> RdfTuple = new HashMap<String,Map>();
    public RdfNode(){
    }

    public RdfNode(String S,String P,String O){
        this.S = S;
        this.P = P;
        this.O = O;
        this.PO.put(P,O);
        RdfTuple.put(this.S,this.PO);
    }
    public void setRdfNode(String S,String P,String O){
        this.S = S;
        this.P = P;
        this.O = O;
        this.PO.put(P,O);
        RdfTuple.put(this.S,this.PO);
    }

    public void setO(String o) {
        O = o;
    }

    public void setP(String p) {
        P = p;
    }

    public void setS(String s) {
        S = s;
    }

    public String getS() {
        return S;
    }

    public String getP() {
        return P;
    }

    public String getO() {
        return O;
    }
}
