package org.sci.historycrawl;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json.*;
import org.apache.commons.validator.routines.UrlValidator;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;


/*
 * program parses JSON message and returns a bag of tuples
 * tuple contains three fields(l, froml, l"&"froml) of type chararray
 */


public class formatdate extends EvalFunc<String>
  {

    
    @Override
	public String exec(Tuple input){
    	String date1 = null;
    	String tmp = null;
        try{
        	 
        	String date = (String)input.get(1);
        	date1 = curateDate(date);
        	
        	tmp = (String)input.get(0);
        	tmp = tmp + " ";
        	tmp = tmp + date1;
     		    	     
    		
   		 //Tuple t = tf.newTuple();
   		 //t.append(input.get(2));
        }catch(Exception e){
        }    
        return tmp;
    }
   
    public String curateDate(String date){
    	String year = date.substring(0, 4);
    	String tmp = year;
    	String month = date.substring(4, 6);
    	tmp = tmp + month;
    	String day = date.substring(6, 8);
    	tmp = tmp + day;
    	/*tmp = tmp + " ";
    	
    	String hour = date.substring(8, 10);
    	String min = date.substring(10, 12);
    	String sec = date.substring(12, 14);
    	
    	tmp = tmp + hour;
    	tmp = tmp + ":";
    	tmp = tmp + min;
    	tmp = tmp + ":";
    	tmp = tmp + sec;*/
    	
    	return tmp;
    }
    
   
  }
