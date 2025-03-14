package com.elastic;



import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json.JSONArray;
import org.json.JSONObject;

public class QueryProcessor {
    private ElasticsearchClient esClient;
    
    
    public QueryProcessor() {
        this.esClient = new ElasticsearchClient();
    }
    
    
    //Extracting Droptable name
    private String extractIndexNameFromDropTable(String query) {
        query = query.toLowerCase().trim();

        if (query.startsWith("drop table")) {
            // Extract table name
            Pattern pattern = Pattern.compile("drop table\\s+(\\w+)", Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(query);
            return matcher.find() ? matcher.group(1) : null;
        }
        return null;
    }
    
  
    

 
    
    
    
    //Create Table Method
    public String createTable(String index,JSONObject mappings) {
        try {
            
                String response = esClient.createIndex(index, mappings.toString());
                return "Table created successfully: " + response;
            
        } catch (Exception e) {
            e.printStackTrace();
            return "Error creating table: " + e.getMessage();
        }
    }

    
    
    private JSONObject extractIndexandDoc(String query) {
        Pattern pattern = Pattern.compile(
            "DELETE\\s+FROM\\s+(\\w+)\\s+WHERE\\s+(\\w+)\\s*=\\s*'([^']*)'",
            Pattern.CASE_INSENSITIVE
        );

        Matcher matcher = pattern.matcher(query);

        if (matcher.find()) {
            String index = matcher.group(1);   
            String fieldName = matcher.group(2);
            String fieldValue = matcher.group(3); 

            JSONObject properties = new JSONObject();
            properties.put("index", index);
            properties.put("fieldName", fieldName);
            properties.put("fieldValue", fieldValue);

            return properties;
        }
        return null;
    }

    
    public static JSONObject extractInsertQueryDetails(String query) {
        JSONObject queryDetails = new JSONObject();

        if (query == null || query.trim().isEmpty()) {
            return createErrorJSON("Query is empty or null.");
        } else {
            query = query.trim();
            if (query.endsWith(";")) {
                query = query.substring(0, query.length() - 1);
            }

             {
                String[] parts = query.split("(?i)\\bvalues\\b", 2);
                if (parts.length < 2) {
                    return createErrorJSON("Invalid query format: Missing VALUES keyword.");
                } else {
                    String tablePart = parts[0].trim().replaceFirst("(?i)insert into", "").trim();
                    int openParenIndex = tablePart.indexOf("(");
                    int closeParenIndex = tablePart.indexOf(")");

                    if (openParenIndex == -1 || closeParenIndex == -1) {
                        return createErrorJSON("Invalid query format: Missing column parentheses.");
                    } else {
                        String tableName = tablePart.substring(0, openParenIndex).trim();
                        String[] fields = tablePart.substring(openParenIndex + 1, closeParenIndex).split("\\s*,\\s*");

                        String valuesPart = parts[1].trim();
                        int valOpen = valuesPart.indexOf("(");
                        int valClose = valuesPart.lastIndexOf(")");

                        if (valOpen == -1 || valClose == -1 || valOpen >= valClose) {
                            return createErrorJSON("Invalid query format: Missing values parentheses.");
                        } else {
                            valuesPart = valuesPart.substring(valOpen + 1, valClose);
                            String[] values = valuesPart.split("\\s*,\\s*");

                            if (fields.length != values.length) {
                                return createErrorJSON("Mismatch between fields and values.");
                            } else {
                                queryDetails.put("table", tableName);

                                JSONArray fieldsArray = new JSONArray();
                                for (int i = 0; i < fields.length; i++) {
                                    JSONObject fieldObject = new JSONObject();
                                    fieldObject.put("field", fields[i].trim());
                                    fieldObject.put("value", convertValue(values[i].replaceAll("['\"]", "").trim()));
                                    fieldsArray.put(fieldObject);
                                }

                                queryDetails.put("fields", fieldsArray);
                            }
                        }
                    }
                }
            }
        }
        return queryDetails;
    }
    
    public static Object convertValue(String value) {
        if (value.matches("-?\\d+")) {  
            return Integer.parseInt(value);
        } else if (value.matches("-?\\d+\\.\\d+")) { 
            return Double.parseDouble(value);
        } else {
            return value; 
        }
    }
    
    
    private static JSONObject createErrorJSON(String message) {
        JSONObject errorResponse = new JSONObject();
        errorResponse.put("error", message);
        return errorResponse;
    }





	    public String processQuery(String query) {
	        StringBuilder result = new StringBuilder();

	        try {
	            // INSERT INTO Operation
	            if (query.toLowerCase().trim().startsWith("insert into")) {
	                JSONObject values = extractInsertQueryDetails(query);
	                if (values == null || values.has("error")) {
	                    result.append("Invalid INSERT query format.");
	                } else {
	                    String index = values.getString("table");

	                    if (!esClient.indexExists(index)) {
	                        result.append("No index named ").append(index).append(" in the db");
	                    } else {
	                        String response = esClient.insertIntoIndex(values);
	                        result.append("Inserted into: ").append(index).append(response);
	                    }
	                }
	            }
	            // DROP TABLE Operation
	            else if (query.toLowerCase().trim().startsWith("drop table")) {
	                String indexName = extractIndexNameFromDropTable(query);
	                if (indexName != null) {
	                    if (esClient.indexExists(indexName)) {
	                        String response = esClient.deleteIndex(indexName);
	                        result.append("Deleted index: ").append(indexName).append("\nResponse: ").append(response);
	                    } else {
	                        result.append("Index ").append(indexName).append(" does not exist.");
	                    }
	                }
	            }
	            // DELETE Operation
	            else if (query.trim().toUpperCase().startsWith("DELETE")) {
	                JSONObject extractedData = extractIndexandDoc(query);
	                if (extractedData != null) {
	                    String index = extractedData.getString("index");
	                    String fieldName = extractedData.getString("fieldName");
	                    String fieldValue = extractedData.getString("fieldValue");
	                    String deleteResponse = esClient.deleteByQuery(index, fieldName, fieldValue);
	                    if (deleteResponse != null) {
	                        result.append("Deleted document from index: ").append(index)
	                                .append(", FieldName: ").append(fieldName)
	                                .append(", FieldValue: ").append(fieldValue)
	                                .append("\nResponse: ").append(deleteResponse);
	                    } else {
	                        result.append("Index ").append(index).append(" or Doc_ID ").append(fieldValue).append(" does not exist.");
	                    }
	                } else {
	                    result.append("No table or document data found.");
	                }
	            }
	            // CREATE TABLE Operation
	            else if (query.toLowerCase().trim().startsWith("create table")) {
	                JSONObject tableDetails = CreateTable.extractTableDetails(query);
	                
	                if (tableDetails == null) {
	                    result.append("Null value");
	                    return result.toString();
	                }
	                
	                String index = tableDetails.getString("index");
	                JSONObject mappings = tableDetails.getJSONObject("mappings");

	                if (esClient.indexExists(index)) {
	                    result.append("Index already exists: " + index);
	                } else {
	                    String response = createTable(index, mappings);
	                    result.append(response);
	                }
	            }

	            // SELECT Query Processing
	            else if (query.toLowerCase().trim().startsWith("select ")) { 
	            	if (query.toLowerCase().contains("group by")){
	            		result.append(GroupByFunc.parseAggregationQuery(query));
	            	}
	                JSONObject selectObj = SelectOperation.parseSelectQuery(query);
	                String response = esClient.selectOperation(selectObj);
	                String table = TableClass.formatAsHtmlTable(response);
	                result.append(table);
	                
	            }
	        }catch(Exception e){
	        	e.printStackTrace();
	        }
	        return result.toString();
	    }
}

    
