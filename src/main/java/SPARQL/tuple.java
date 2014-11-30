package SPARQL;

public class tuple {
	public String S,P,O;
	public tuple(){
		
	}
	public tuple(String S,String P,String O){
		this.S = S;
		this.P = P;
		this.O = O;
	}
	public String getS(){
		return S;
	}
	public String getP(){
		return P;
	}
	public String getO(){
		return O;
	}
	public void setS(String S){
		this.S = S;
	}
	public void setP(String P){
		this.P = P;
	}
	public void setO(String O){
		this.O = O;
	}
	public tuple getAll(){
		return this;
	}
}
