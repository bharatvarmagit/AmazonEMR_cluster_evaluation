package com.group9.application.project;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

public class ArrayUtil {
	
	public static int[] convertStringToIntArray(String input) {
		
		JSONArray jsonArray = (JSONArray) new JSONObject(new JSONTokener("{data:" + input + "}")).get("data");
	    int[] jsonIntArray = new int[jsonArray.length()]; 
	    for (int i = 0; i < jsonArray.length(); i++) {
	    	jsonIntArray[i] = jsonArray.getInt(i);
	    }
	    
	    return jsonIntArray;
	}

}
