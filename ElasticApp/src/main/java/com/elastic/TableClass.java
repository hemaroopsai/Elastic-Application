package com.elastic;
import org.json.JSONArray;
import org.json.JSONObject;
import java.util.HashSet;
import java.util.Set;

public class TableClass {
	public static String formatAsHtmlTable(String jsonResponse) {
        StringBuilder htmlTable = new StringBuilder();
        
        // Start the HTML table
        htmlTable.append("<table border='1' cellpadding='5' cellspacing='0' style='border-collapse: collapse;'>\n");
        
        try {
            JSONObject jsonObj = new JSONObject(jsonResponse);
            
            // Check if the response contains hits
            if (jsonObj.has("hits")) {
                JSONObject hits = jsonObj.getJSONObject("hits");
                JSONArray hitsArray = hits.getJSONArray("hits");
                
                if (hitsArray.length() > 0) {
                    // Collect all unique field names from the documents
                    Set<String> fieldNames = new HashSet<>();
                    for (int i = 0; i < hitsArray.length(); i++) {
                        JSONObject hit = hitsArray.getJSONObject(i);
                        JSONObject source = hit.getJSONObject("_source");
                        source.keySet().forEach(field -> {
                            // Normalize field names (e.g., convert to lowercase and replace spaces)
                            String normalizedField = field.toLowerCase().replace(" ", "_");
                            fieldNames.add(normalizedField);
                        });
                    }
                    
                    // Print header row
                    htmlTable.append("<tr>\n");
                    for (String field : fieldNames) {
                        htmlTable.append("<th>").append(field).append("</th>\n");
                    }
                    htmlTable.append("</tr>\n");
                    
                    // Print data rows
                    for (int i = 0; i < hitsArray.length(); i++) {
                        JSONObject hit = hitsArray.getJSONObject(i);
                        JSONObject source = hit.getJSONObject("_source");
                        
                        htmlTable.append("<tr>\n");
                        for (String field : fieldNames) {
                            // Normalize field names in the source as well
                            String value = "N/A";
                            for (String sourceField : source.keySet()) {
                                String normalizedSourceField = sourceField.toLowerCase().replace(" ", "_");
                                if (normalizedSourceField.equals(field)) {
                                    value = source.optString(sourceField, "N/A");
                                    break;
                                }
                            }
                            htmlTable.append("<td>").append(value).append("</td>\n");
                        }
                        htmlTable.append("</tr>\n");
                    }
                } else {
                    htmlTable.append("<tr><td colspan='").append("Error").append("'>No results found.</td></tr>\n");
                }
            } else {
                htmlTable.append("<tr><td colspan='1'>Invalid response format.</td></tr>\n");
            }
        } catch (Exception e) {
            htmlTable.append("<tr><td colspan='1'>Error parsing JSON response: ").append(e.getMessage()).append("</td></tr>\n");
        }
        
        // End the HTML table
        htmlTable.append("</table>");
        
        return htmlTable.toString();
    }
}
