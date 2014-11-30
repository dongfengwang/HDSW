package SPARQL;

import java.util.ArrayList;
import java.util.List;

public class CSparqlModel {
	public List<String> select = new ArrayList<String>();
	public List<tuple> where = new ArrayList<tuple>();
	public List<String> groupBy = new ArrayList<String>();
	public List<String> orderBy = new ArrayList<String>();
	public List<String> having = new ArrayList<String>();
	public String range,step;
	
	public CSparqlModel() {

	}

	public CSparqlModel(List<String> select, List<tuple> where,
			List<String> groupBy, List<String> orderBy, List<String> having) {
		super();
		this.select = select;
		this.where = where;
		this.groupBy = groupBy;
		this.orderBy = orderBy;
		this.having = having;
	}
    public List<tuple> getWhere(){
        return where;
    }
	public void CDump(){
		System.out.println("Select:"+select);
		System.out.println("Range:"+range+" step:"+step);	
		
		System.out.println("Where:"+where);
		System.out.println("groupBy:"+groupBy);
		System.out.println("orderBy:"+orderBy);
		System.out.println("having:"+having);
	}
}
