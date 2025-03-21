package com.elastic;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONObject;

public class CreateTable {
	public static JSONObject extractTableDetails(String query) {
        query = query.toLowerCase().trim();

        if (query.startsWith("create table")) {
 
            Pattern pattern = Pattern.compile("create table\\s+([`'\\\"]?\\w+[`'\\\"]?)", Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(query);
            String tableName = matcher.find() ? matcher.group(1) : null;

            if (tableName == null) {
                return null;
            }

            // Extract fields
            String fieldsPart = query.substring(query.indexOf("(") + 1, query.lastIndexOf(")"));
            String[] fields = fieldsPart.split(",");

            JSONObject properties = new JSONObject();

            for (String field : fields) {
                field = field.trim();
                String[] parts = field.split("\\s+");
                if (parts.length >= 2) {
                    String fieldName = parts[0];
                    String fieldType = parts[1].replaceAll("\\(\\d+\\)", ""); 

                    String esType = convertToElasticType(fieldType);
                    
                

                    properties.put(fieldName, new JSONObject().put("type", esType));
                }
            }
          
            String typeName = "doc";

            JSONObject mappings = new JSONObject();
            mappings.put(typeName, new JSONObject().put("properties", properties));

            JSONObject result = new JSONObject();
            result.put("index", tableName);
            result.put("mappings", mappings);

            return result;
        }
        return null;
    }
	public static String convertToElasticType(String sqlType) {
        sqlType = sqlType.toLowerCase();
        switch (sqlType) {
            case "int":
            case "integer":
            case "bigint":
            case "smallint":
                return "long";  
            case "decimal":
            case "numeric":
            case "double":
            case "float":
                return "double"; 
            case "char":
            case "varchar":
            case "text":
                return "text";  
            case "date":
            case "datetime":
            case "timestamp":
                return "date";  
            case "boolean":
                return "boolean";  
            default:
                return "keyword"; 
        }
    }


}
