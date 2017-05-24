package org.sci.historycrawl;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Trim 
  {
    public static String trim(String input){

        if (input == null )
            return null;
        try{
        	Pattern urlPattern = Pattern.compile("(.*://)([^:^/]*)(:\\d*)?(.*)?");	//".*//(.*?)/"	
            Matcher urlMatcher = urlPattern.matcher(input);
        	if(urlMatcher.find())
        		return urlMatcher.group(2);
        }catch(Exception e){
            System.out.println(e.getStackTrace());
        }
		return null;
    }
}
