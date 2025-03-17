package com.elastic;

import org.json.JSONArray;
import org.json.JSONObject;
import java.util.HashSet;
import java.util.Set;

public class TableClass {

	public static String formatAsHtmlTable(String jsonResponse) {
	    StringBuilder htmlTable = new StringBuilder();

	    try {
	        JSONObject jsonObj = new JSONObject(jsonResponse);

	      
	        if (jsonObj.has("error")) {
	            return formatErrorMessage(jsonObj);
	        }

	        Set<String> fieldNames = new HashSet<>();
	        JSONArray hitsArray = new JSONArray();
	        JSONObject aggregations = new JSONObject();

	        // Extracting hits data
	        if (jsonObj.has("hits") && jsonObj.getJSONObject("hits").has("hits")) {
	            hitsArray = jsonObj.getJSONObject("hits").getJSONArray("hits");
	            for (int i = 0; i < hitsArray.length(); i++) {
	                JSONObject hit = hitsArray.getJSONObject(i);
	                if (hit.has("_source")) {
	                    JSONObject source = hit.getJSONObject("_source");
	                    for (String key : source.keySet()) {
	                        fieldNames.add(key); 
	                    }
	                }
	            }
	        }
	        System.out.println("Extracted field names: " + fieldNames);
	        System.out.println("Total hits processed: " + hitsArray.length());

	
	        if (jsonObj.has("aggregations")) {
	            aggregations = jsonObj.getJSONObject("aggregations");
	            extractAggregationFields(aggregations, fieldNames);
	        }


	        htmlTable.append("<table border='1' cellpadding='5' cellspacing='0' style='border-collapse: collapse;'>\n");


	        htmlTable.append("<tr>\n");
	        for (String field : fieldNames) {
	            htmlTable.append("<th>").append(field).append("</th>\n");
	        }
	        htmlTable.append("</tr>\n");

	
	        for (int i = 0; i < hitsArray.length(); i++) {
	            JSONObject hit = hitsArray.getJSONObject(i);
	            if (hit.has("_source")) {
	                JSONObject source = hit.getJSONObject("_source");

	                htmlTable.append("<tr>\n");
	                boolean hasData = false;
	                for (String field : fieldNames) {
	                    String value = source.optString(field, "N/A");
	                    if (!value.equals("N/A")) {
	                        hasData = true;
	                    }
	                    htmlTable.append("<td>").append(value).append("</td>\n");
	                }
	                if (hasData) {
	                    htmlTable.append("</tr>\n");
	                } else {
	                    System.out.println("Skipping empty row...");
	                }
	            }
	        }   
	    
	        if (!aggregations.isEmpty()) {
	            htmlTable.append(formatAggregationsAsHtmlTable(aggregations, fieldNames));
	        }

	 
	        htmlTable.append("</table>\n");

	    } catch (Exception e) {
	        e.printStackTrace();
	        return "<p style='color: red; background-color: #ffe6e6; padding: 10px;'>Error processing JSON response: " + e.getMessage() + "</p>";
	    }

	    return htmlTable.toString();
	}
	
	private static String formatErrorMessage(JSONObject jsonObj) {
	    StringBuilder errorMessage = new StringBuilder();
	    errorMessage.append("<div style='background-color: #ffcccc; color: red; font-weight: bold; padding: 10px; border: 1px solid red;'>");
	    errorMessage.append("<p>Elasticsearch Error:</p><ul>");

	    if (jsonObj.has("error")) {
	        JSONObject errorObj = jsonObj.getJSONObject("error");
	        errorMessage.append("<li><strong>Type:</strong> ").append(errorObj.optString("type", "Unknown")).append("</li>");
	        errorMessage.append("<li><strong>Reason:</strong> ").append(errorObj.optString("reason", "No reason provided")).append("</li>");

	        if (errorObj.has("root_cause")) {
	            JSONArray rootCauses = errorObj.getJSONArray("root_cause");
	            for (int i = 0; i < rootCauses.length(); i++) {
	                JSONObject cause = rootCauses.getJSONObject(i);
	                errorMessage.append("<li><strong>Root Cause:</strong> ")
	                        .append(cause.optString("reason", "No details")).append("</li>");
	            }
	        }
	    }

	    errorMessage.append("</ul></div>"); 
	    return errorMessage.toString();
	}



    private static void extractAggregationFields(JSONObject aggregations, Set<String> fieldNames) {
        for (String aggKey : aggregations.keySet()) {
            JSONObject aggObject = aggregations.getJSONObject(aggKey);

            if (aggObject.has("buckets")) {
                JSONArray buckets = aggObject.getJSONArray("buckets");
                for (int i = 0; i < buckets.length(); i++) {
                    JSONObject bucket = buckets.getJSONObject(i);
                    fieldNames.add(aggKey);

                    for (String bucketKey : bucket.keySet()) {
                        if (!bucketKey.equals("key") && !bucketKey.equals("doc_count")) {
                            fieldNames.add(bucketKey);
                        }
                    }
                }
            } else if (aggObject.has("value")) {
                fieldNames.add(aggKey);
            }
        }
    }

    private static String formatAggregationsAsHtmlTable(JSONObject aggregations, Set<String> fieldNames) {
        StringBuilder htmlTable = new StringBuilder();

        for (String aggKey : aggregations.keySet()) {
            JSONObject aggObject = aggregations.getJSONObject(aggKey);

            if (aggObject.has("buckets")) {
                JSONArray buckets = aggObject.getJSONArray("buckets");

                for (int i = 0; i < buckets.length(); i++) {
                    JSONObject bucket = buckets.getJSONObject(i);
                    String key = bucket.optString("key", "N/A");
                    long docCount = bucket.optLong("doc_count", 0);

                    htmlTable.append("<tr>\n");
                    for (String field : fieldNames) {
                        if (field.equals(aggKey)) {
                            htmlTable.append("<td>").append(key).append("</td>\n");
                        } else if (field.equals("doc_count")) {
                            htmlTable.append("<td>").append(docCount).append("</td>\n");
                        } else if (bucket.has(field)) {
                            JSONObject nestedAgg = bucket.getJSONObject(field);
                            if (nestedAgg.has("value")) {
                                htmlTable.append("<td>").append(nestedAgg.optDouble("value", 0)).append("</td>\n");
                            } else if (nestedAgg.has("hits")) {
                    
                                JSONArray hits = nestedAgg.getJSONObject("hits").getJSONArray("hits");
                                StringBuilder selectedFields = new StringBuilder();
                                for (int j = 0; j < hits.length(); j++) {
                                    JSONObject hit = hits.getJSONObject(j);
                                    if (hit.has("_source")) {
                                        JSONObject source = hit.getJSONObject("_source");
                                        selectedFields.append(source.toString(2)).append("<br>");
                                    }
                                }
                                htmlTable.append("<td>").append(selectedFields.toString()).append("</td>\n");
                            } else {
                                htmlTable.append("<td>N/A</td>\n");
                            }
                        } else {
                            htmlTable.append("<td>N/A</td>\n");
                        }
                    }
                    htmlTable.append("</tr>\n");
                }
            } else if (aggObject.has("value")) {
                htmlTable.append("<tr>\n");
                for (String field : fieldNames) {
                    if (field.equals(aggKey)) {
                        htmlTable.append("<td>").append(aggObject.optDouble("value", 0)).append("</td>\n");
                    } else {
                        htmlTable.append("<td>N/A</td>\n");
                    }
                }
                htmlTable.append("</tr>\n");
            }
        }

        return htmlTable.toString();
    }
}