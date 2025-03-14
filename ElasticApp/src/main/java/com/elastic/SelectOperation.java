package com.elastic;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json.JSONArray;
import org.json.JSONObject;

public class SelectOperation {

	public static JSONObject parseSelectQuery(String query) {
	    JSONObject result = new JSONObject();

	    Pattern pattern = Pattern.compile(
	        "SELECT\\s+(.*?)\\s+FROM\\s+(\\w+)(?:\\s+WHERE\\s+(.*))?", 
	        Pattern.CASE_INSENSITIVE
	    );
	    Matcher matcher = pattern.matcher(query);

	    if (matcher.find()) {
	        String fieldsPart = matcher.group(1).trim();  // Fields in SELECT
	        String tableName = matcher.group(2).trim();  // Table Name
	        String whereClause = matcher.group(3) != null ? matcher.group(3).trim() : null;  // WHERE Clause

	        result.put("table", tableName);

	        JSONObject queryBody = new JSONObject();

	        if (fieldsPart.equals("*")) {
	            queryBody.put("query", new JSONObject().put("match_all", new JSONObject()));
	        } else {
	            String[] fieldArray = fieldsPart.split("\\s*,\\s*");
	            JSONArray sourceFields = new JSONArray(Arrays.asList(fieldArray));
	            queryBody.put("_source", sourceFields);
	        }

	        if (whereClause != null) {
	            JSONObject conditions = parseWhereClause(whereClause);
	            queryBody.put("query", conditions);
	        }

	        result.put("query", queryBody);
	    }

	    return result;
	}
    

	
	    public static JSONObject parseWhereClause(String whereClause) {
	        JSONObject boolQuery = new JSONObject();
	        JSONObject boolConditions = new JSONObject();
	        JSONArray mustClauses = new JSONArray();
	        JSONArray shouldClauses = new JSONArray();

	        // Split by operators
	        String[] conditions = whereClause.split("(?i)\\s+(AND|OR)\\s+");
	        String operator = whereClause.matches("(?i).*\\s+OR\\s+.*") ? "OR" : "AND";

	        // Process conditions
	        for (String condition : conditions) {
	            condition = condition.trim();
	            if (condition.isEmpty()) continue;

	            JSONObject conditionClause = parseCondition(condition);
	            if (operator.equals("AND")) {
	                mustClauses.put(conditionClause);
	            } else {
	                shouldClauses.put(conditionClause);
	            }
	        }

	        // Build the bool query
	        if (mustClauses.length() > 0) {
	            boolConditions.put("must", mustClauses);
	        }
	        if (shouldClauses.length() > 0) {
	            boolConditions.put("should", shouldClauses);
	            boolConditions.put("minimum_should_match", 1);
	        }

	        boolQuery.put("bool", boolConditions);
	        return boolQuery;
	    }

	    private static JSONObject parseCondition(String condition) {
	        JSONObject conditionClause = new JSONObject();

	        if (condition.contains("=")) {
	            String[] parts = condition.split("\\s*=\\s*");
	            String field = parts[0].trim();
	            String value = parts[1].trim().replace("'", "").replace("\"", "").replace(";", "");

	            // Check if the value is numeric or not
	            if (value.matches("\\d+")) {
	                conditionClause.put("term", new JSONObject().put(field, Integer.parseInt(value)));
	            } else {
	                conditionClause.put("term", new JSONObject().put(field + ".keyword", value));
	            }
	        } else if (condition.contains(">")) {
	            String[] parts = condition.split(">");
	            String field = parts[0].trim();
	            int value = Integer.parseInt(parts[1].trim());

	            conditionClause.put("range", new JSONObject().put(field, new JSONObject().put("gt", value)));
	        } else if (condition.contains("<")) {
	            String[] parts = condition.split("<");
	            String field = parts[0].trim();
	            int value = Integer.parseInt(parts[1].trim());

	            conditionClause.put("range", new JSONObject().put(field, new JSONObject().put("lt", value)));
	        }

	        return conditionClause;
	    }



}
