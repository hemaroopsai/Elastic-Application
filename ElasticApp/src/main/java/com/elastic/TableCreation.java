package com.elastic;


import java.util.*;
import org.json.JSONArray;
import org.json.JSONObject;

public class TableCreation {
	public static String createJoinedTable(String responseOne, String responseTwo, String joinField, String joinType, Map<String, String> columnTableMapping) {
        if (responseOne == null || responseOne.trim().isEmpty() ||
            responseTwo == null || responseTwo.trim().isEmpty()) {
            return "Error: One or both responses are empty.";
        }
        if (joinField == null || joinField.trim().isEmpty()) {
            return "Error: Join field not provided.";
        }
        if (joinType == null || joinType.trim().isEmpty()) {
            return "Error: Join type not provided.";
        }
        if (columnTableMapping == null || columnTableMapping.isEmpty()) {
            return "Error: Column table mapping is not provided.";
        }
        
        // Parse responses.
        JSONObject jsonOne = new JSONObject(responseOne);
        JSONObject jsonTwo = new JSONObject(responseTwo);
        
        // Extract hits arrays.
        JSONArray hitsOne = jsonOne.optJSONObject("hits") != null ?
                            jsonOne.getJSONObject("hits").optJSONArray("hits") : new JSONArray();
        JSONArray hitsTwo = jsonTwo.optJSONObject("hits") != null ?
                            jsonTwo.getJSONObject("hits").optJSONArray("hits") : new JSONArray();
                            
        if (hitsOne.length() == 0 || hitsTwo.length() == 0) {
            return "Error: One or both responses contain no data.";
        }
        
        // Build mapping for responseOne ("students").
        Map<String, List<JSONObject>> mapOne = new HashMap<>();
        for (int i = 0; i < hitsOne.length(); i++) {
            JSONObject hit = hitsOne.optJSONObject(i);
            if (hit == null) continue;
            JSONObject source = hit.optJSONObject("_source");
            if (source == null) continue;
            String joinValue = source.optString(joinField, "Unknown");
            mapOne.computeIfAbsent(joinValue, k -> new ArrayList<>()).add(source);
        }
        
        // Build mapping for responseTwo ("collage"). Assume one record per join field.
        Map<String, JSONObject> mapTwo = new HashMap<>();
        for (int i = 0; i < hitsTwo.length(); i++) {
            JSONObject hit = hitsTwo.optJSONObject(i);
            if (hit == null) continue;
            JSONObject source = hit.optJSONObject("_source");
            if (source == null) continue;
            String joinValue = source.optString(joinField, "Unknown");
            mapTwo.put(joinValue, source);
        }
        
        // Separate the selected columns based on the mapping.
        List<String> collageFields = new ArrayList<>();
        List<String> studentFields = new ArrayList<>();
        for (Map.Entry<String, String> entry : columnTableMapping.entrySet()) {
            String col = entry.getKey();
            String table = entry.getValue();
          
            if ("collage".equalsIgnoreCase(table)) {
                collageFields.add(col);
            } else if ("students".equalsIgnoreCase(table)) {
                studentFields.add(col);
            }
        }
        
        // Build headers: first the collage fields, then the student fields.
        List<String> headers = new ArrayList<>();
        headers.addAll(collageFields);
        headers.addAll(studentFields);
        
        // Build the HTML table.
        StringBuilder htmlTable = new StringBuilder();
        htmlTable.append("<table border='1' cellpadding='5' cellspacing='0' style='border-collapse: collapse;'>\n");
        // Header row.
        htmlTable.append("<tr>");
        for (String header : headers) {
            htmlTable.append("<th>").append(header).append("</th>");
        }
        htmlTable.append("</tr>\n");
        
        // For INNER join: only output rows that have matching student records.
        for (Map.Entry<String, JSONObject> entry : mapTwo.entrySet()) {
            String joinValue = entry.getKey();
            JSONObject collageRecord = entry.getValue();
            List<JSONObject> studentRecords = mapOne.get(joinValue);
            
            // Skip if there are no matching student records for an INNER join.
            if ("INNER".equalsIgnoreCase(joinType) && (studentRecords == null || studentRecords.isEmpty())) {
                continue;
            }
            
            // For each matching student record, produce a separate row.
            if (studentRecords != null && !studentRecords.isEmpty()) {
                for (JSONObject studentRecord : studentRecords) {
                    htmlTable.append("<tr>");
                    // Add collage fields.
                    for (String field : collageFields) {
                        htmlTable.append("<td>").append(collageRecord.optString(field, "")).append("</td>");
                    }
                    // Add student fields.
                    for (String field : studentFields) {
                        htmlTable.append("<td>").append(studentRecord.optString(field, "")).append("</td>");
                    }
                    htmlTable.append("</tr>\n");
                }
            }
            // For a LEFT join, if no student record exists, output the collage record with empty student columns.
            else if ("LEFT".equalsIgnoreCase(joinType)) {
                htmlTable.append("<tr>");
                for (String field : collageFields) {
                    htmlTable.append("<td>").append(collageRecord.optString(field, "")).append("</td>");
                }
                for (String field : studentFields) {
                    htmlTable.append("<td></td>");
                }
                htmlTable.append("</tr>\n");
            }
        }
        
        htmlTable.append("</table>\n");
        return htmlTable.toString();
    }


    public static String tableCreation(String response) {
    	
    	if (response == null || response.trim().isEmpty()) {
    	    return "Error: Empty response received.";
    	}

    	if(response.contains("{")) {
    		String jsonResponseStr = response.trim();
    		int jsonStartIndex = jsonResponseStr.indexOf("{");

    		if (jsonStartIndex != -1) {
    			jsonResponseStr = jsonResponseStr.substring(jsonStartIndex); 
    		}

    		JSONObject jsonResponse = new JSONObject(jsonResponseStr);


    		if (jsonResponse.has("error")) {
    			JSONObject errorObject = jsonResponse.getJSONObject("error");
    			String reasonType = errorObject.optString("type", "Unknown error");
    			String reasonMessage = errorObject.optString("reason", "No reason provided.");
    			return "Error: " + reasonType + " - " + reasonMessage;
    		}


    		if (jsonResponse.has("aggregations")) {
    			return formatAggregationResponse(jsonResponseStr);
    		}


    		return formatJsonResponse(jsonResponseStr);
    	}
    	else {
    		return formatIndexTable(response);
    	}
    }

    private static String formatJsonResponse(String response) {
        JSONObject jsonResponse = new JSONObject(response);
        JSONArray hits = jsonResponse.getJSONObject("hits").getJSONArray("hits");

        if (hits.length() == 0) {
            return "<p>No data available</p>";
        }

        JSONObject firstHit = hits.getJSONObject(0).getJSONObject("_source");
        JSONArray keys = firstHit.names();

        StringBuilder htmlTable = new StringBuilder();
        htmlTable.append("<table border='1' cellpadding='5' cellspacing='0' style='border-collapse: collapse;'>\n");

        htmlTable.append("<tr>");
        for (int i = 0; i < keys.length(); i++) {
            htmlTable.append("<th>").append(keys.getString(i)).append("</th>");
        }
        htmlTable.append("</tr>\n");

        for (int i = 0; i < hits.length(); i++) {
            JSONObject source = hits.getJSONObject(i).getJSONObject("_source");
            htmlTable.append("<tr>");
            for (int j = 0; j < keys.length(); j++) {
                String key = keys.getString(j);
                htmlTable.append("<td>").append(source.optString(key, "N/A")).append("</td>");
            }
            htmlTable.append("</tr>\n");
        }

        htmlTable.append("</table>\n");
        return htmlTable.toString();
    }

    private static String formatIndexTable(String response) {
        List<String> indexNames = extractIndexNames(response);

        if (indexNames.isEmpty()) {
            return "<p>No indices found</p>";
        }

        StringBuilder htmlTable = new StringBuilder();
        htmlTable.append("<table border='1' cellpadding='5' cellspacing='0' style='border-collapse: collapse;'>\n");
        htmlTable.append("<tr><th>Index Name</th></tr>\n");

        for (String index : indexNames) {
            htmlTable.append("<tr><td>").append(index).append("</td></tr>\n");
        }

        htmlTable.append("</table>\n");
        return htmlTable.toString();
    }

    public static List<String> extractIndexNames(String response) {
        List<String> indexNames = new ArrayList<>();
        int indexPos = response.indexOf("index");

        if (indexPos == -1) {
            return indexNames;
        }

        String dataPart = response.substring(indexPos + 5).trim();
        String[] records = dataPart.split("yellow open");

        for (String record : records) {
            record = record.trim();
            if (!record.isEmpty()) {
                String[] columns = record.split("\\s+");
                if (columns.length > 1) {
                    indexNames.add(columns[0]);
                }
            }
        }
        return indexNames;
    }

    private static String formatAggregationResponse(String response) {
        JSONObject jsonResponse = new JSONObject(response);
        JSONObject aggregations = jsonResponse.optJSONObject("aggregations");

        if (aggregations == null || aggregations.isEmpty()) {
            return "<p>No aggregation data available</p>";
        }

        StringBuilder htmlTable = new StringBuilder();
        htmlTable.append("<table border='1' cellpadding='5' cellspacing='0' style='border-collapse: collapse;'>\n");

     
        htmlTable.append("<tr>");
        for (String key : aggregations.keySet()) {
            htmlTable.append("<th>").append(key).append("</th>");
        }
        htmlTable.append("</tr>\n");

  
        htmlTable.append("<tr>");
        for (String key : aggregations.keySet()) {
            JSONObject aggField = aggregations.getJSONObject(key);
            String value = aggField.opt("value").toString();
            htmlTable.append("<td>").append(value).append("</td>");
        }
        htmlTable.append("</tr>\n");

        htmlTable.append("</table>\n");
        return htmlTable.toString();
    }

}