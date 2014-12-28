package SPARQL;

public class CSparqlParser {
	public CSparqlModel cParse(String sparql){
		CSparqlModel cModel = new CSparqlModel();
		 String Csparql = sparql;

	        String[] param = Csparql.split("\n");
	        for(String key : param){
	        	/*
	        	 * select 
	        	 */
	        	if(key.indexOf("SELECT")!=-1){
	        		String[] select = key.split(" ");
	        		int i=1;
	        		while(i<select.length){
	        			//System.out.println(select[i]);
	        			cModel.select.add(select[i].toString());
	        			i++;
	        		}
	        	}
	        	/*
	        	 * From
	        	 */
	        	if(key.indexOf("FROM")!=-1){
	        		key = key.replace("[", "");
	        		key = key.replace("]", "");
	        		String[] from = key.split(" ");
	        		int i=0;
	        		while(i<from.length){
	        			System.out.println(from[i]);
	        			if(from[i].equals("RANGE")==true){
	        				cModel.range=from[++i].toString();
	        			}
	        			if(from[i].equals("STEP")){
	        				cModel.step=from[++i].toString();
	        			}
	        			i++;
	        		}
	        		
	        	}
	        	/*
	        	 * Where
	        	 */
	        	if(key.indexOf("WHERE")!=-1){
	        		key = key.replace("WHERE", "").replace("{","").replace("}","").trim();
	        		String[] keys = key.split(";");
	        		int i=0;
	        		String S=null;
	        		tuple tu = new tuple();
	        		while(i<keys.length){
	        			//System.out.println(keys[i]);
	        			tu = new tuple();
	        			
		        		String[] element=keys[i].split(" ");
		        		if(element.length>=3){
		        			S=element[0];
		        			tu.S=element[0];
		        			tu.P=element[1];	
			        		tu.O=element[2];
		        		}else{
		        			tu.S=S;
		        			tu.P=element[0];	
			        		tu.O=element[1];
		        		}
	        			cModel.where.add(tu);
	        			i++;
	        		}
	        		
	        	}
	        	/*
	        	 * Groupby ����
	        	 */
	        	if(key.indexOf("GROUP BY")!=-1){
	        		key = key.replace("GROUP BY", "");
	        		String[] group = key.split(" ");
	        		int i=0;
	        		while(i<group.length){
	        			if(group[i].isEmpty()==false){
	        				cModel.groupBy.add(group[i]);
	        			}
	        			i++;
	        		}
	        		
	        	}
	        	/*
	        	 * Orderby ����
	        	 */
	        	if(key.indexOf("ORDER BY")>=0){
	        		key = key.replace("ORDER BY", "");
	        		String[] order = key.split(" ");
	        		int i=0;
	        		while(i<order.length){
	        			//System.out.println(select[i]);
	        			cModel.orderBy.add(order[i]);
	        			
	        			i++;
	        		}
	        		
	        	}
	        	/*
	        	 * Having ����
	        	 */
	        	if(key.indexOf("HAVING")!=-1){
	        		key = key.replace("HAVING", "");
	        		String[] having = key.split(" ");
	        		int i=0;
	        		while(i<having.length){
	        			//System.out.println(select[i]);
	        			if(!having[i].isEmpty()&&having[i].equals("(")==false&&having[i].equals(")")==false){
	        				cModel.having.add(having[i]);
	        			}
	        			i++;
	        		}
	        	}
	        }
	        cModel.CDump();
        return cModel;
	}
}
