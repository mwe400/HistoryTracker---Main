package org.sci.historycrawl;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json.*;
import org.apache.commons.validator.routines.UrlValidator;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class jsplit extends EvalFunc<String>
  {
	
    @Override
	public String exec(Tuple input){
        StringBuffer url = new StringBuffer();
    	if (input == null || input.size() == 0)
            return null;
        try{
        	UrlValidator u = new UrlValidator();
            Pattern urlPattern = Pattern.compile("(.*://)([^:^/]*)(:\\d*)?(.*)?");	//".*//(.*?)/"	
            Matcher urlMatcher = null;
            
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
    		    			 url.append(urlMatcher.group(2)).append("\n");
    		    		 }
    		    	 }    		    	 
    		     }    		    	     
    		}
        }catch(Exception e){
        	
        }
        return url.toString();
    }
    

  }
