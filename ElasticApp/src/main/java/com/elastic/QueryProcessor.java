package com.elastic;




import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
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
    
  
    
    private String extractIndexFromDelete(String query) {
    	String result = "";
    	query = query.toLowerCase().trim().replace(";", "");
    	 Pattern pattern = Pattern.compile("DELETE\\s+FROM\\s+(\\w+)\\s*", Pattern.CASE_INSENSITIVE);
         Matcher matcher = pattern.matcher(query);
         
         if (matcher.find()) {
             String index =  matcher.group(1); 
             result = index;
         } 
         
         return result;
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
    		    "DELETE\\s+FROM\\s+(\\w+)\\s+WHERE\\s+(.+)", 
    		    Pattern.CASE_INSENSITIVE
    		);

        Matcher matcher = pattern.matcher(query);

        if (matcher.find()) {
            String index = matcher.group(1);   
            String whereClause = matcher.group(2);
            
            JSONObject queryJson = SelectOperation.parseWhereClause(whereClause);
           
            JSONObject properties = new JSONObject();
            properties.put("index", index);
            properties.put("query", queryJson);
       

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
                    String fieldsStr = tablePart.substring(openParenIndex + 1, closeParenIndex).trim();
                    String[] fields = fieldsStr.split("\\s*,\\s*");

                
                    String valuesPart = parts[1].trim();
          
                    Pattern pattern = Pattern.compile("\\(([^\\)]+)\\)");
                    Matcher matcher = pattern.matcher(valuesPart);
                    
                    JSONArray rowsArray = new JSONArray();
                    while (matcher.find()) {
                        String rowValuesStr = matcher.group(1).trim();
                        String[] rowValues = rowValuesStr.split("\\s*,\\s*");
                        
                        if (rowValues.length != fields.length) {
                            return createErrorJSON("Mismatch between number of fields and values in one of the rows.");
                        }
                        
                        JSONArray rowArray = new JSONArray();
                        for (String val : rowValues) {
                          
                            String cleanVal = val.replaceAll("[']", "").trim();
                            rowArray.put(convertValue(cleanVal));
                        }
                        rowsArray.put(rowArray);
                    }

           
                    queryDetails.put("table", tableName);
                    
                    JSONArray fieldsArray = new JSONArray();
                    for (String field : fields) {
                        fieldsArray.put(field.trim());
                    }
                    queryDetails.put("fields", fieldsArray);
                    queryDetails.put("values", rowsArray);
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
    
    public JSONObject processQueryFields(JSONObject extractedFields) throws IOException {
        JSONObject responses = new JSONObject(); 

        Set<String> tableNames = extractedFields.keySet();

        for (String table : tableNames) {
            JSONArray fieldsArray = extractedFields.getJSONArray(table);

     
            JSONObject queryBody = new JSONObject();
            queryBody.put("_source", fieldsArray);
            queryBody.put("query", new JSONObject().put("match_all", new JSONObject()));

            JSONObject selectObj = new JSONObject();
            selectObj.put("table", table);
            selectObj.put("body", queryBody);

     
            String response = esClient.selectOperation(selectObj);
            responses.put(table, new JSONObject(response));
        }

        return responses; 
    }



	    public String processQuery(String query) {
	        StringBuilder result = new StringBuilder();

	        try {


	        	if (query.toLowerCase().trim().startsWith("alter table")) {
	        	    if (query.toLowerCase().contains("add")) {
	        	        JSONObject values = AlterClass.extractForAddAlterQuery(query);
	        	        String response = esClient.alterOperation(values);
	        	        result.append(response);
	        	    } else if (query.toLowerCase().contains("modify")) {
	        	    	JSONObject values = AlterClass.extractForModifyAlterQuery(query);
	        	    	result.append(values);
	        	    } else {
	        	        
	        	    }
	        	}
	            // INSERT INTO Operation
	        	else if (query.toLowerCase().trim().startsWith("insert into")) {
	                JSONObject values = extractInsertQueryDetails(query);
	                System.out.println(values);
	                if (values == null || values.has("error")) {
	                    result.append("Invalid INSERT query format."+values);
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
	            	if (query.trim().toUpperCase().contains("WHERE")) {
	            		JSONObject extractedData = extractIndexandDoc(query);
	            		System.out.println(extractedData);
		                if (extractedData != null) {
		                    String index = extractedData.getString("index");
		                    extractedData.remove("index");
		                   
		                    String deleteResponse = esClient.deleteByQuery(index,extractedData);
		                    	if (deleteResponse != null) {
		                    			if (deleteResponse.contains("error")) {
		                    				result.append(deleteResponse);
		                    				}
		                    			else {
		                    				result.append("Deleted document from index: ").append(index)
		                    				.append("\nResponse: ").append(deleteResponse);
		                    				}
		                    	} else {
		                    		result.append("Index ").append(index).append(" or Doc_ID ").append("column value").append(" does not exist.");
		                    	}
		                } else {
		                	result.append("No table or document data found.");
		                	}
		            	}
	            	else {
	            		
	            		 String index = extractIndexFromDelete(query);
	            		 if (index!=null) {
	            			 System.out.println(index);
	            			 String response = esClient.deleteByIndex(index);
	            			 if (response != null) {
	            				 result.append("successfull deleted").append(response);
	            			 }
	            		 }	else {
	            			 result.append("index do not exsits or wrong format");
	            		 }
	            		 
	            	
	            		 
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
	            	if (query.toLowerCase().contains("distinct")) {
	            		if (query.endsWith(";")) {
	            			query = query.substring(0, query.length() - 1);
	            		}
	            		JSONObject parseQuery = SelectOperation.parseDistinctQuery(query);
	            		String response = esClient.selectOperation(parseQuery);
	            		String table = TableClass.formatAsHtmlTable(response);
	            		result.append(table);
	            	}
	            	else if (query.toLowerCase().contains("group by")) {
	                	
	                	if(query.endsWith(";")) {
	            			query = query.substring(0, query.length() - 1);
	            		}
	                    JSONObject parsedQuery = GroupByFunc.parseAggregationQuery(query);
	                    if (parsedQuery != null) {
	                    	String index = parsedQuery.getString("table");
	                    	if (index!=null) {
	                    		if(esClient.indexExists(index)) {
	                    			System.out.println(index);
		                    		parsedQuery.remove("table");
		                    		System.out.println(parsedQuery);
		                    
		                    		String response = esClient.groupByOperation(index, parsedQuery);
		                    		System.out.println(response);
		                    		String table = TableClass.formatAsHtmlTable(response);
		                    		result.append(table);
	                    		}
	                    		else {
	                    			result.append("No index named "+index+" exsists");
	                    		}
	                    	}
	                    	else {
	                    		result.append("Index value is'nt formatted correctly");
	                    	}
	                    }
	                    else {
	                    	result.append("wrong format of query");
	                    }
	                
	                }else if (query.toLowerCase().contains("schemaname")) {
	                   
	                    if (query.endsWith(";")) {
	                        query = query.substring(0, query.length() - 1);
	                    }

	           
	                    if (SelectOperation.parseSchemaNames(query)) {
	                        String response = esClient.SchemaRepresentation();
	                        String table = TableCreation.tableCreation(response);
                    		result.append(table);
	                    } else {
	                        result.append("Wrong Schema or Wrong query");
	                    }
	                }
	                else if (query.toLowerCase().contains("join")) {  
	                	
	                    if (query.endsWith(";")) {
	                        query = query.substring(0, query.length() - 1);
	                    }

	                     JSONObject joinQuery = SelectOperation.parseJoinQuery(query); 
	                     	if (joinQuery == null) {
                     			result.append("Invalid JOIN Query");
                     		}
	                     	String tableOne=joinQuery.getString("table1");
	                     	String tableTwo = joinQuery.getString("table2");
	                     	JSONObject mappingObject = joinQuery.getJSONObject("columnTableMapping");
	                     	Map<String, String> selectedFieldString = new HashMap<>();

	                     	Iterator<String> keys = mappingObject.keys();
	                     	while (keys.hasNext()) {
	                     	    String key = keys.next();
	                     	    selectedFieldString.put(key, mappingObject.getString(key));
	                     	}
	                     	String joinType = joinQuery.getString("joinType");
	                     	JSONObject ans = SelectOperation.extractQueryFields(joinQuery);
	                     	JSONObject onMapping = joinQuery.getJSONObject("onMapping");
	                        String condition = onMapping.getString("leftColumn");
	                        System.out.print(condition);
	                     	JSONObject responses = processQueryFields(ans);

	                     	
	                     	String responseOne = responses.get(tableOne).toString();
	                     	String responseTwo = responses.get(tableTwo).toString();
	                     	
	       
	                                        
	                     	String joinedTable = TableCreation.createJoinedTable(responseOne, responseTwo,condition,joinType,selectedFieldString);
	                     	result.append(joinedTable);
	                    
	                }
	                else {
	                
	                	if(query.endsWith(";")) {
	                		
	            			query = query.substring(0, query.length() - 1);
	            		}
	                	if (query.equals("")) {
		                    result.append("Empty Query");
	                	}
	                	else {
	                		
	                		JSONObject selectObj = SelectOperation.parseSelectQuery(query);
	                		   if (selectObj==null) {
		   	                    	result.append("Invalid Query");
		   	                    }
	                		   else {
	                		
	                			   String response = esClient.selectOperation(selectObj);
	                			   System.out.println(response);
	                			   String table = TableCreation.tableCreation(response);
	                			   result.append(table);
	                		}
	                	}
	                }
	            }

	        }catch(Exception e){
	        	e.printStackTrace();
	        }
	      return result.toString();
	    }
}

    
