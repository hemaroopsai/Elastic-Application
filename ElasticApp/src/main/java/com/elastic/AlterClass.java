package com.elastic;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONObject;

public class AlterClass {
	public static JSONObject extractForAddAlterQuery(String query) {
		
		JSONObject result = new JSONObject();
		Pattern pattern = Pattern.compile(
				"ALTER TABLE (\\w+)\\s+ADD\\s+(\\w+)\\s+([\\w\\(\\)]+);",Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(query);
		
		if (matcher.find()) {
            String tableName = matcher.group(1);  
            String columnName = matcher.group(2); 
            String dataType = matcher.group(3).replaceAll("\\(\\d+\\)", "");;   
            JSONObject properties = new JSONObject();
            if (dataType!=null) {
            	String esType = CreateTable.convertToElasticType(dataType);
                
                
                properties.put(columnName, new JSONObject().put("type", esType));
            }
            JSONObject mappings = new JSONObject();
            mappings.put("properties", properties);
            result.put("index", tableName);
            result.put("mappings", mappings);
            System.out.print(result);
		}
		
		return result;
	}
	
		public static JSONObject extractForModifyAlterQuery(String query) {
		
			JSONObject result = new JSONObject();
			Pattern pattern = Pattern.compile(
					"(?i)ALTER TABLE\\s+(\\w+)\\s+MODIFY\\s+(\\w+)\\s+([\\w\\s\\(\\),]+);",Pattern.CASE_INSENSITIVE);
			Matcher matcher = pattern.matcher(query);
		
			if (matcher.find()) {
				String tableName = matcher.group(1);  
				String columnName = matcher.group(2); 
				String newDataType = matcher.group(3).replaceAll("\\(\\d+\\)", "");
				
				 String esType = CreateTable.convertToElasticType(newDataType);
			
			
				result.put("error", "cannot update");
				
			}
			return result;
		}
}

