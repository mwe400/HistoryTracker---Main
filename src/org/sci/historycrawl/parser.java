package org.sci.historycrawl;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.*;

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
import java.net.*;


/*
 * program parses JSON message and returns a bag of tuples
 * tuple contains three fields(l, froml, l"&"froml) of type chararray
 */


public class parser extends EvalFunc<DataBag>
  {
	BagFactory mBagFactory = BagFactory.getInstance();
    Hashtable<String, Integer> hcHashTable = null;
    FileInputStream fstream = null;
    DataInputStream in = null;
    BufferedReader br = null;
    TupleFactory tf = TupleFactory.getInstance();

    static final HashMap<String,String> tsld = csvreader.buildmap("SLDs.csv");
    //static final HashMap<String,String> tsld = csvreader.buildmap("SLDs.csv");
    		//"https://s3.amazonaws.com/historycrawl/tsld/SLDs.csv");
    static final char DOT ='.';
    static int hckeywords = 0;
    String keywords;
    String[] hctokens;
    //BufferedWriter hcdebug = null;
    @Override
	public DataBag exec(Tuple input) throws IOException{
    	DataBag output = mBagFactory.newDefaultBag();
    	
    	if (input == null || input.size() == 0)
            return null;  	
       	String src = null;
    		try {
    			src = Trim.trim((String)input.get(1));
    		} catch (ExecException e1) {
    			// TODO Auto-generated catch block
    			e1.printStackTrace();
    		}
    		/*Writer writer = null;
        	try {
        	    writer = new BufferedWriter(new OutputStreamWriter(
        	          new FileOutputStream("/home/hai/myurl1.txt"), "utf-8"));
        	    writer.write("abc");
        	} catch (IOException ex){
        		
        	}finally {
        		   try {writer.close();} catch (Exception ex) {}
        	}*/
    		//hcdebug = new BufferedWriter(new FileWriter("/home/hdn11/hcdebug.txt",true));
    		
        		buildHashTable();
        		//fstream = new FileInputStream("/home/hdn11/options1.txt");
        		try
        		{
        			fstream = new FileInputStream("/tmp/options1.txt");
        			in = new DataInputStream(fstream);
        			br = new BufferedReader(new InputStreamReader(in));
        		}
        		catch(IOException e)
        		{
        			
        		}
        		finally{
        			try{
        				if(fstream != null)
        			{
        				fstream.close();
        			}
        			} 
        			catch (IOException e)
        			{
        				
        			}        			       			
        		}
            	
        		boolean matchingType = getTypeOfMatching();
        	    boolean urlType = getTypeOfURL();
        	    boolean isWhiteList = getWhiteList();
        	    boolean isSearch = isSearchText();
        	    //if (hckeywords == 0){
        	    	keywords = getSearchText();
        	    	hckeywords = 1;
        	    	hctokens = keywords.split("\\ ");
        	    	//System.out.println("tokens length = " + hctokens.length);
        	    	//for(int i = 0; i < hctokens.length;i++)
        	    	//	System.out.println("tokens " + i + " " + hctokens[i]);
        	    //}
        	/*try
            	{
            	    String filename= "MyFile.txt";
            	    FileWriter fw = new FileWriter(filename,true); //the true will append the new dat
            	    fw.write(hcHashTable.get("yahoo.com").toString());
            	    fw.close();
            	}
            	catch(IOException ioe)
            	{
            	    System.err.println("IOException: " + ioe.getMessage());
            	}
        	
        if (src != null)
        	if (hcHashTable.containsKey(curateURL(src))){	*/
        try{
        	UrlValidator u = new UrlValidator();
        	//String src = Trim.trim((String)input.get(1));
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
    		
    		if (!isWhiteList || (isWhiteList && (hcHashTable.containsKey(curateURL(src)) || !matchingType))){
    		if (!isWhiteList || (isWhiteList && matchingType)){
    			String srcURL = formatURL(src);
    		if (srcURL != null){
    		for (int i = 0; i < nameArray.length(); i++){
    		     JSONObject childJSONObject = nameArray.getJSONObject(i);
    		     if(childJSONObject.has("url")){    		    	
    		    	 String temp = childJSONObject.getString("url");
    		    	 if(u.isValid(temp)){
    		    		 urlMatcher = urlPattern.matcher(temp); 
    		    		 if(urlMatcher.find()){
    		    			 String x = urlMatcher.group(2);
    		    			 String dstURL = formatURL(x);
    		    			 
    		    			 if (!isWhiteList || (isWhiteList && (hcHashTable.containsKey(dstURL) ||  (!matchingType && hcHashTable.containsKey(curateURL(src)))))){
    		    				 String temp2 = childJSONObject.getString("text");
    		    			     if (temp2 == null)
    		    			    	 temp2 = "no text";
    		    			     if (!isSearch || (isSearch && (searchText(temp2)))){
    		    			 if (formatURL(x) !=null){
    		    			 if(!(ipAddress.matcher(x).find()||ipAddress.matcher(src).find())){
    		    				 //x=curateURL(x);
    		    				 Tuple t = tf.newTuple();
    		    				 //t.append(curateURL(src)); 
    		    				 if(!urlType){
    		    					 srcURL = (String)input.get(1);
    		    					 dstURL = temp;
    		    					 
    		    					 srcURL = getFullURL(srcURL);
    		    					 dstURL = getFullURL(dstURL);
    		    					 //dstURL = curateURL(x);
    		    				 }
    		    				 t.append(srcURL);
    		    				 //t.append(x);
    		    				 t.append(dstURL); 
    		    				 //t.append(curateURL(src)+"&"+x+"&"+temp2);
    		    			     //t.append(curateURL(src)+"&"+dstURL+"&"+temp2);
    		    			     //t.append(srcURL+"&"+dstURL+"&"+temp2);
    		    			     t.append(srcURL+"`"+dstURL+"`"+temp2);
    		    				 t.append(input.get(2));
    		    				 t.append(temp2);
    		    				 //String strDate = curateDate((String)input.get(2));
    		    			     //t.append(strDate);
    		    				 output.add(t);	       		    				 
    		    			 }
    		    			 }
    		    			 }
    		    		 }
    		    			 }
    		    	 }    		    	 
    		     }    		    	    
    		}
    		}
    		
        }    		
    		else{
    			String srcURL = formatURL(src);
        		for (int i = 0; i < nameArray.length(); i++){
        		     JSONObject childJSONObject = nameArray.getJSONObject(i);
        		     if(childJSONObject.has("url")){    		    	
        		    	 String temp = childJSONObject.getString("url");
        		    	 if(u.isValid(temp)){
        		    		 urlMatcher = urlPattern.matcher(temp); 
        		    		 if(urlMatcher.find()){
        		    			 String x = urlMatcher.group(2);
        		    			 String dstURL = formatURL(x);
        		    			 
        		    			 if (!isWhiteList || (isWhiteList && (hcHashTable.containsKey(dstURL) ||  (!matchingType && hcHashTable.containsKey(curateURL(src)))))){
        		    			     String temp2 = childJSONObject.getString("text");
        		    			     if (temp2 == null)
        		    			    	 temp2 = "no text";
        		    			     if (!isSearch || (isSearch && searchText(temp2))){
        		    				 if (formatURL(x) !=null || srcURL !=null){
        		    				 if (srcURL != null)
        		    					 dstURL = getRootURL(x);
        		    				 else
        		    					 srcURL = getRootURL(x);
        		    			 if(!(ipAddress.matcher(x).find()||ipAddress.matcher(src).find())){
        		    				 //x=curateURL(x);
        		    				 Tuple t = tf.newTuple();
        		    				 //t.append(curateURL(src)); 
        		    				 if(!urlType){
        		    					 srcURL = (String)input.get(1);
        		    					 dstURL = temp;
        		    					 
        		    					 srcURL = getFullURL(srcURL);
        		    					 dstURL = getFullURL(dstURL);
        		    				 }
        		    				 t.append(srcURL);
        		    				 //t.append(x);
        		    				 t.append(dstURL);
        		    			     
        		    				 //t.append(curateURL(src)+"&"+x+"&"+temp2);
        		    			     //t.append(curateURL(src)+"&"+dstURL+"&"+temp2);
        		    			     //t.append(srcURL+"&"+dstURL+"&"+temp2);
        		    			     t.append(srcURL+"`"+dstURL+"`"+temp2);
        		    				 t.append(input.get(2));
        		    				 t.append(temp2);
        		    				 //String strDate = curateDate((String)input.get(2));
        		    			     //t.append(strDate);

        		    				 output.add(t);	   
        		    				 
        		    			 }}
        		    			 }
        		    			     }
        		    		 }
        		    	 }    		    	 
        		     }    		    	    
        		}
        		
    		}
        }
        }catch(Exception e){
        }
		 Tuple t = tf.newTuple();		 
		 t.append("record hai93  ");
		 try {
			t.append(Trim.trim((String)input.get(1))+" is parsed");
		} catch (ExecException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 output.add(t);	 
		 //hcdebug.close();
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
    
    public boolean getTypeOfURL(){
    	String strLine = "";
    	try{
    	//FileInputStream fstream = new FileInputStream("/home/hdn11/typeofurl.txt");
    	//DataInputStream in = new DataInputStream(fstream);
    	//BufferedReader br = new BufferedReader(new InputStreamReader(in));
    	strLine = br.readLine();
    	strLine = br.readLine();
    	}
    	catch (Exception e){
    		
    	}
    	
    	if (strLine.equals("0")){
    		return false; //full url
    	}
    	
    	return true;//root url
    }
    
    public boolean getWhiteList(){
    	String strLine = "";
    	try{
    	//FileInputStream fstream = new FileInputStream("/home/hdn11/typeofurl.txt");
    	//DataInputStream in = new DataInputStream(fstream);
    	//BufferedReader br = new BufferedReader(new InputStreamReader(in));
    	strLine = br.readLine();
    	strLine = br.readLine();
    	}
    	catch (Exception e){
    		
    	}
    	
    	if (strLine.equals("0")){
    		return false; //No white list
    	}
    	
    	return true;//White List
    }
    
    public boolean isSearchText(){
    	String strLine = "";
    	try{
    	//FileInputStream fstream = new FileInputStream("/home/hdn11/typeofurl.txt");
    	//DataInputStream in = new DataInputStream(fstream);
    	//BufferedReader br = new BufferedReader(new InputStreamReader(in));
    	strLine = br.readLine();
    	strLine = br.readLine();
    	}
    	catch (Exception e){
    		
    	}
    	
    	if (strLine.equals("0")){
    		return false; //No white list
    	}
    	
    	return true;//White List
    }
    
    public String getSearchText(){
    	String strLine = "";
    	FileInputStream fstream = null;
    	try{
    	//FileInputStream fstream = new FileInputStream("/home/hdn11/keywords1.txt");
    		fstream = new FileInputStream("/tmp/keywords1.txt");
    		DataInputStream in = new DataInputStream(fstream);
    		BufferedReader br = new BufferedReader(new InputStreamReader(in));
    		strLine = br.readLine();
    	}
    	catch (Exception e){
    		
    	} finally{
    		try {
    			if(fstream != null){
    				fstream.close();
    			}
    		}catch (IOException e){
    			
    		}
    		
    	}
    	
    	return strLine;//root url
    }
    
    public boolean getTypeOfMatching(){
    	String strLine = "";
    	try{
    	//FileInputStream fstream = new FileInputStream("/home/hdn11/typeofmatching.txt");
    	//DataInputStream in = new DataInputStream(fstream);
    	//BufferedReader br = new BufferedReader(new InputStreamReader(in));
    	strLine = br.readLine();
    	strLine = br.readLine();
    	}
    	catch (Exception e){
    		
    	}
    	
    	if (strLine.equals("0")){
    		return false; //single matching
    	}
    	
    	return true;//both matching
    }
    public void buildHashTable(){
    	hcHashTable = new Hashtable();
    	FileInputStream fstream = null;
    	try{
    	//FileInputStream fstream = new FileInputStream("/home/hdn11/seedlist1.txt");
    		fstream = new FileInputStream("/tmp/seedlist1.txt");
    		DataInputStream in = new DataInputStream(fstream);
    		BufferedReader br = new BufferedReader(new InputStreamReader(in));
    		String strLine;
    	while ((strLine = br.readLine()) != null){
    		if (!hcHashTable.contains(strLine))
    		hcHashTable.put(strLine, 1);
    	}
    	}catch (IOException e){
    		
    	}
    	finally{
    		try{
    			if(fstream != null){
    				fstream.close();
    			}
    				
    		}
    		catch(IOException e){
    			
    		}
    	}
    	
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
					 System.out.println("SLD \t"+domains[index-1]+DOT+domains[index]+DOT+domains[index+1]);
					 return domains[index-1]+DOT+domains[index]+DOT+domains[index+1];
				 }	 
			 }
			 System.out.println("\nSLD: " + domains[index]+DOT+domains[index+1]);
			 return domains[index]+DOT+domains[index+1];
		 }
		 return null;		
	}
    public String getRootURL(String url) throws MalformedURLException{
    		   String url1 = url;
    		   String trimmedURL;
    		   String rootURL = null;
    		   StringBuffer strBuf = new StringBuffer();
    		   URL myURL;
    		   if (!url1.startsWith("http") && !url1.startsWith("https"))
    				url1 = "http://" + url1;

    		   myURL = new URL(url1);
    		   trimmedURL = myURL.getHost();
    		   if (trimmedURL.contains("www."))
    			   trimmedURL = trimmedURL.substring(4);
    		   return trimmedURL;
    		   
    }
    
    public boolean searchText(String text) throws IOException{
    	for(int i = 0; i < hctokens.length; i++){
    		//hcdebug.write(hctokens + " ");
    		//System.out.println(hctokens + " ");
    		if (text.toLowerCase().contains(hctokens[i].toLowerCase()))
    			return true;
    	}
    	return false;
    }
    
    public String getFullURL(String url)
    {
    	String[] tokens = url.split("\\?");
    	
    	if (tokens.length > 0){
    		return tokens[0];
    	}
    	return url;
    }
    
   public String formatURL(String url) throws MalformedURLException{
	   String url1 = url;
	   String trimmedURL;
	   String rootURL = null;
	   StringBuffer strBuf = new StringBuffer();
	   URL myURL;
	   if (!url1.startsWith("http") && !url1.startsWith("https"))
			url1 = "http://" + url1;

	   myURL = new URL(url1);
	   trimmedURL = myURL.getHost();
	   
	   if(trimmedURL.contains("www")){
		   int indexTmp = trimmedURL.indexOf("www");
		   trimmedURL = trimmedURL.substring(indexTmp + 4);
	   }
	   /*String[] tokens = trimmedURL.split("\\.");
	   int i = 0;
	   int j =0;
	   int max = 0;
	   
	   for(i = tokens.length - 1; i >= 0;i=i-1){
		   strBuf.delete(0, strBuf.length());
		   for (j = i; j < tokens.length; j++){
		   strBuf.append(tokens[j]);
		   if (j != tokens.length - 1)
			   strBuf.append(".");
		   }
		   if (hcHashTable.containsKey(strBuf.toString())){
			   max = tokens.length - i;
			   rootURL = strBuf.toString();
		   }
	   }
	   
	   if (max == 0)
		   return null;
	   */

	   //return rootURL;
	   return trimmedURL;
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
            s.add(new Schema.FieldSchema("text", DataType.CHARARRAY));
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