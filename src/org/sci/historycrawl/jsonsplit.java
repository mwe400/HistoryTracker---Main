package org.sci.historycrawl;


/*
 * program parses json message and returns a bag of tuples
 * tuple comtains two fields(l, froml) both are chararrays
 */


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
import org.sci.historycrawl.Trim;

public class jsonsplit extends EvalFunc<DataBag>
  {
	BagFactory mBagFactory = BagFactory.getInstance();
    DataBag output = mBagFactory.newDefaultBag();
    TupleFactory tf = TupleFactory.getInstance();
    
    @Override
	public DataBag exec(Tuple input){
    	if (input == null || input.size() == 0)
            return null;
        try{
        	UrlValidator u = new UrlValidator();
        	String src = Trim.trim((String)input.get(1));
        	System.out.println(input.toString());
            Pattern urlPattern = Pattern.compile("(.*://)([^:^/]*)(:\\d*)?(.*)?");	//".*//(.*?)/"	
            Matcher urlMatcher = null;
            
            Tuple header = tf.newTuple();
            header.append("record  ");
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
    		    			 Tuple t = tf.newTuple();
		    				 t.append(src);
    		    			 t.append(urlMatcher.group(2));
    		    			 output.add(t);	 
    		    		 }
    		    	 }    		    	 
    		     }    		    	     
    		}
        }catch(Exception e){
        }
		 Tuple t = tf.newTuple();
		 t.append("record  ");
		 try {
			t.append(Trim.trim((String)input.get(1))+" is parsed");
		} catch (ExecException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 output.add(t);	 

        return output;
    }
    
    @Override
    public Schema outputSchema(Schema input) {
        try {
        	Schema tuple = new Schema();
        	
        	Schema s = new Schema();
            s.add(new Schema.FieldSchema("l", DataType.CHARARRAY));
            s.add(new Schema.FieldSchema("froml", DataType.CHARARRAY));
            //tuple.add(new Schema.FieldSchema("links", DataType.TUPLE));//original version
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
  //  	return new Schema(new Schema.FieldSchema("url",DataType.TUPLE));
   // }
    
  }
