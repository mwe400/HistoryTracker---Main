package org.sci.historycrawl;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.StringTokenizer;

public class csvreader {
	public static HashMap<String,String> tsld ;
	public static HashMap<String,String> buildmap(String filename){
		if(null==tsld){
			tsld = new HashMap<String,String>();
			System.out.println("object created");
		}
		 try
         {  
			 
			 URL u = new URL(filename);
			 URLConnection uc = u.openConnection();
			 uc.connect();
			 BufferedReader br = new BufferedReader(new InputStreamReader(uc.getInputStream()));
			 System.out.println("Reading file from "+filename);
			 Thread.sleep(1000);
			// 	BufferedReader br = new BufferedReader( new FileReader(filename));
                 String strLine = null;
                 StringTokenizer st = null;
                 while((strLine = br.readLine()) != null){
                         st = new StringTokenizer(strLine, ",");
                         while(st.hasMoreTokens()){
                         	tsld.put(st.nextToken().toLowerCase(), null);
                         }
                 }
                 return tsld;
         }
         catch(Exception e)
         {
                 System.out.println("something is wrong with the csv file plese check:  " + e);                  
         }
		return null;
	}
}