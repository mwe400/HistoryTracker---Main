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


public class parser extends EvalFunc<DataBag>
  {
	BagFactory mBagFactory = BagFactory.getInstance();
    
    TupleFactory tf = TupleFactory.getInstance();

    static final HashMap<String,String> tsld = csvreader.buildmap("https://s3.amazonaws.com/historycrawl/tsld/SLDs.csv");
    		//"https://s3.amazonaws.com/historycrawl/tsld/SLDs.csv");
    static final char DOT ='.';
    
    @Override
	public DataBag exec(Tuple input){
    	DataBag output = mBagFactory.newDefaultBag();
    	if (input == null || input.size() == 0)
            return null;
        try{
        	UrlValidator u = new UrlValidator();
        	String src = Trim.trim((String)input.get(1));
            Pattern urlPattern = Pattern.compile("(.*://)([^:^/]*)(:\\d*)?(.*)?");	//	".*//(.*?)/"	
            Pattern ipAddress = Pattern.compile("\\b\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\b");
            // pattern just detects whether it is in ip format or not xxx.xxx.xxx.xxx (even 999.xxx.xxx.xxx) is correct

            Matcher urlMatcher = null;
            
            Tuple header = tf.newTuple();
            header.append("record ");
            header.append(src+" parsing started");
            output.add(header);
            
            StringBuffer str = new StringBuffer((String)input.get(0));
        	JSONObject myjson = new JSONObject(str.toString());	
    		JSONArray nameArray = myjson.getJSONArray("Links");
    		for (int i = 0; i < nameArray.length(); i++){
    		     JSONObject childJSONObject = nameArray.getJSONObject(i);
    		     if(childJSONObject.has("url")){    		    	
    		    	 String temp = childJSONObject.getString("url");
    		    	 if(u.isValid(temp)){
    		    		 urlMatcher = urlPattern.matcher(temp); 
    		    		 if(urlMatcher.find()){
    		    			 String x = urlMatcher.group(2);
    		    			 if(!(ipAddress.matcher(x).find()||ipAddress.matcher(src).find())){
    		    				 x=curateURL(x);
    		    				 Tuple t = tf.newTuple();
    		    				 t.append(curateURL(src)); 
    		    				 t.append(x);
    		    				 t.append(curateURL(src)+"&"+x);
    		    				 
    		    				 t.append(input.get(2));
    		    				 //String strDate = curateDate((String)input.get(2));
    		    			     //t.append(strDate);
    		    			     
    		    				 output.add(t);	   
    		    				 
    		    			 }
    		    		 }
    		    	 }    		    	 
    		     }    		    	     
    		}
   		 //Tuple t = tf.newTuple();
   		 //t.append(input.get(2));
        }catch(Exception e){
        }
        
		 Tuple t = tf.newTuple();		 
		 t.append("record ");
		 try {
			t.append(Trim.trim((String)input.get(1))+" is parsed");
		} catch (ExecException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 output.add(t);	 

        return output;
    }
    
    public String curateDate(String date){
    	String year = date.substring(0, 4);
    	String tmp = year + "-";
    	String month = date.substring(4, 6);
    	tmp = tmp + month;
    	tmp = tmp + "-";
    	String day = date.substring(6, 8);
    	tmp = tmp + day;
    	tmp = tmp + " ";
    	
    	String hour = date.substring(8, 10);
    	String min = date.substring(10, 12);
    	String sec = date.substring(12, 14);
    	
    	tmp = tmp + hour;
    	tmp = tmp + ":";
    	tmp = tmp + min;
    	tmp = tmp + ":";
    	tmp = tmp + sec;
    	
    	return tmp;
    }
    
	public String curateURL(String url){
		
		String[] domains = url.split("\\.");
		 int index = domains.length;
		
		 if(index>1 && null!=url){				//check if at-least two domains are available after the split
			 index = index-2;					//get the second level domain from the split
			 String sld = domains[index];
			 if(tsld.containsKey(DOT+sld+DOT+domains[index+1])){
				 if(index-1>=0){				//&&index+1<domains.length
//					 System.out.println("SLD \t"+domains[index-1]+DOT+domains[index]+DOT+domains[index+1]);
					 return domains[index-1]+DOT+domains[index]+DOT+domains[index+1];
				 }	 
			 }
//			 System.out.println("TLD \t"+domains[index]+DOT+domains[index+1]);
			 return domains[index]+DOT+domains[index+1];
		 }
		 return null;		
	}
   
    @Override
    public Schema outputSchema(Schema input) {
        try {
        	Schema tuple = new Schema();
          	Schema s = new Schema();
            s.add(new Schema.FieldSchema("l", DataType.CHARARRAY));
            s.add(new Schema.FieldSchema("froml", DataType.CHARARRAY));
            s.add(new Schema.FieldSchema("url", DataType.CHARARRAY));
            s.add(new Schema.FieldSchema("date", DataType.CHARARRAY));
            //tuple.add(new Schema.FieldSchema("links", DataType.TUPLE));		//	original version
            tuple.add(new Schema.FieldSchema("links", s, DataType.TUPLE));
            return new Schema(new Schema.FieldSchema("bagwati",tuple, DataType.BAG));            
/*            //getSchemaName(this.getClass().getName().toLowerCase(), input)
            Schema bagSchema = new Schema();
            bagSchema.add(new Schema.FieldSchema("urls", DataType.TUPLE));
            return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), s),
                                                   bagSchema, DataType.BAG));            
        	Schema bag = new Schema();
            bag.add(new Schema.FieldSchema("urls", DataType.CHARARRAY));
            return bag;
            
        	 Schema bagSchema = new Schema();
             bagSchema.add(new Schema.FieldSchema("urls", DataType.CHARARRAY));

             return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
                                                    bagSchema, DataType.BAG));
*/            
        } catch (Exception e) {
            return null;
        }
    }
   
  }
