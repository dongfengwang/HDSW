package Basic;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TimeTransfer {
	public TimeTransfer(){
		
	}
	public static Map<String,String> getTs(String value) throws ParseException{
        String str = value;
        Map<String,String> map = new HashMap<String,String>();
        Pattern p = Pattern.compile("(\\d{4}_\\d{1,2}_\\d{1,2}_\\d{1,2}_\\d{1,2}_\\d{1,2})");
        Matcher m = p.matcher(str);
        ArrayList<String> strs = new ArrayList<String>();
        while (m.find()) {
            strs.add(m.group(1));            
        } 
        for (String s : strs){
        	SimpleDateFormat simple = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");
        	Date date = simple.parse(s);
            str = replace(str,"(\\d{4}_\\d{1,2}_\\d{1,2}_\\d{1,2}_\\d{1,2}_\\d{1,2})",date.getTime()+"");
            map.put(str,date.getTime()+"");
        }
        
		return map;        
    }

    public static String replace(String str,String reold,String renew) {
        
        str = str.replaceAll(reold,renew);
        return str;
             
    }
    public static void main(String[] args) throws ParseException{
    	getTs("Syseme-gd:id_2014_11_27_11_11_11");
    }
}
