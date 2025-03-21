package com.elastic;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONObject;

public class UpdateClass {
	public static JSONObject updateMethod(String query) {
		JSONObject result =new JSONObject();
		System.out.println(query);
		Pattern pattern = Pattern.compile(
			    "UPDATE\\s+(\\w+)\\s+SET\\s+(\\w+)\\s*=\\s*'([^']+)'\\s+WHERE\\s+(.+)",
			    Pattern.CASE_INSENSITIVE
			);
			Matcher matcher = pattern.matcher(query);
			if (matcher.find()) {
				String index = matcher.group(1);
				String columnName = matcher.group(2);
				String columnValue = matcher.group(3);
				String condition = matcher.group(4);
				
				result.put("index", index);
				JSONObject script = new JSONObject();
	            script.put("inline", "ctx._source." + columnName + " = '" + columnValue + "'");
	            script.put("lang", "painless");
	
	        
	            JSONObject queryJson = SelectOperation.parseWhereClause(condition);
	            result.put("query", queryJson);
	            result.put("script", script);
	            
	        } else {
	            System.out.println("No match found! Ensure the query is correctly formatted.");
	        }
	
	        return result;
	    }
}