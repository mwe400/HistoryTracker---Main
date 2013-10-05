package org.sci.historycrawl;
import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;


public class splitter extends EvalFunc<String> {

	public static final int FIRST = 0;
	public static final int SECOND = 1;
	public static final String DELIMITER = "&";
	
	
	@Override
	public String exec(Tuple arg0) throws IOException {
		// TODO Auto-generated method stub

		/*if(!(arg0.isNull(FIRST)||arg0.isNull(SECOND))){
			if(arg0.toString().contains(DELIMITER)){
				String temp = arg0.get(FIRST).toString();
				return temp.substring(FIRST, temp.indexOf(DELIMITER))+"\t"+temp.substring(temp.indexOf(DELIMITER)+1)+"\t"+arg0.get(SECOND);
			}
		}*/
		
		if(arg0.toString().contains(DELIMITER)){
			String temp = arg0.get(FIRST).toString();
			return temp.substring(FIRST, temp.indexOf(DELIMITER))+"\t"+temp.substring(temp.indexOf(DELIMITER)+1);
		}
		
		return null;
		/*String tmp = (String)arg0.get(0);
		String[] tokens = null;
		
		if (tmp.contains("&"))
		tokens = tmp.split("&");
		
		String output = "";
		
		for (String token : tokens) {
			output = token + " ";
		}
		
		return output;	*/


	}

	
}
