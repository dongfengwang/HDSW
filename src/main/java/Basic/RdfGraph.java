package Basic;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2014/11/9.
 */
public class RdfGraph {
    public Map<String,RdfNode> RNode = new HashMap<String, RdfNode>();
    public RdfGraph(){

    }
    public RdfGraph(Map<String,RdfNode> RNode){
        this.RNode = RNode;
    }
    public void AddNode(RdfNode Rnode){
        RNode.put(Rnode.getP(),Rnode);
    }
    /*
     * 获取父节点
     */
    public RdfNode GetParent(String Parent){
        return RNode.get(Parent);
    }
    public void PrintTree(RdfGraph RdfG){

    }
    /*
     * 根据S获取RDF节点
     */
    public RdfNode GetRdfNodeByS(String S){
        RdfNode n = new RdfNode();
        for(String key : this.RNode.keySet()){
            if(this.RNode.get(key).getS() == S){
                n = this.RNode.get(key);
            }
        }
        return n;
    }
    /*
     *根据P获取RDF节点
     */
    public RdfNode GetRdfNodeByP(String P){
        return this.RNode.get(P);
    }
    /*
     *根据O获取RDF节点
     */
    public RdfNode GetRdfNodeByO(String O){
        RdfNode n = new RdfNode();
        for(String key : this.RNode.keySet()){
            if(this.RNode.get(key).getO() == O){
                n = this.RNode.get(key);
            }
        }
        return n;
    }
}
