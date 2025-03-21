package com.elastic;


import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json.JSONArray;
import org.json.JSONObject;

public class SelectOperation {

    public static JSONObject parseSelectQuery(String query) {
    	
        JSONObject result = new JSONObject();
        JSONObject esBody = new JSONObject();

        Pattern pattern = Pattern.compile(
        		"SELECT\\s+(.+?)\\s+FROM\\s+(\\w+)" +
        			    "(?:\\s+WHERE\\s+((?:(?!\\s+GROUP\\s+BY|\\s+ORDER\\s+BY|\\s+LIMIT).)+))?" +
        			    "(?:\\s+ORDER\\s+BY\\s+((?:(?!\\s+LIMIT).)+))?" +
        			    "(?:\\s+LIMIT\\s+(\\d+))?" +
        			    "\\s*;?",
                Pattern.CASE_INSENSITIVE
        );
        Matcher matcher = pattern.matcher(query);

        if (matcher.find()) {
            String fieldsPart = matcher.group(1).trim();
            System.out.println(fieldsPart);
            String tableName = matcher.group(2).trim().replace(";", "");
            String whereClause = matcher.group(3) != null ? matcher.group(3).trim() : null;
            System.out.println(whereClause);
            String orderByPart = matcher.group(4) != null ? matcher.group(4).trim() : null;
            System.out.println(orderByPart);
            String limitPart = matcher.group(5) != null ? matcher.group(5).trim() : null;
            
            System.out.println(limitPart);

            result.put("table", tableName);

            // Process SELECT clause
            if (fieldsPart.equals("*")) {
                esBody.put("query", new JSONObject().put("match_all", new JSONObject()));
            } else {
                JSONArray sourceIncludes = new JSONArray();
                JSONObject aggregations = new JSONObject();
                String[] fieldArray = fieldsPart.split("\\s*,\\s*");

                for (String field : fieldArray) {
                    field = field.trim();
                    String alias = null;

                    // Check for as
                    if (field.toLowerCase().contains(" as ")) {
                        String[] parts = field.split("(?i)\\s+as\\s+");
                        field = parts[0].trim();
                        alias = parts[1].trim();
                    }

                    // Handle aggregate functions
                    if (field.toLowerCase().startsWith("count(")) {
                        String countField = field.replaceAll("(?i)count\\(|\\)", "").trim();
                        JSONObject countAgg = new JSONObject();
                        if (countField.equals("*")) {
                            countAgg.put("value_count", new JSONObject().put("field", "_index"));
                        } else {
                            countAgg.put("value_count", new JSONObject().put("field", countField));
                        }
                        String aggName = (alias != null) ? alias : countField + "_count";
                        aggregations.put(aggName, countAgg);

                    } else if (field.toLowerCase().startsWith("sum(")) {
                        String sumField = field.replaceAll("(?i)sum\\(|\\)", "").trim();
                        JSONObject sumAgg = new JSONObject().put("sum", new JSONObject().put("field", sumField));
                        String aggName = (alias != null) ? alias : sumField + "_sum";
                        aggregations.put(aggName, sumAgg);

                    } else if (field.toLowerCase().startsWith("avg(")) {
                        String avgField = field.replaceAll("(?i)avg\\(|\\)", "").trim();
                        JSONObject avgAgg = new JSONObject().put("avg", new JSONObject().put("field", avgField));
                        String aggName = (alias != null) ? alias : avgField + "_avg";
                        aggregations.put(aggName, avgAgg);

                    } else {
                        sourceIncludes.put(field);
                    }
                }

                if (sourceIncludes.length() > 0) {
                    esBody.put("_source", sourceIncludes);
                }
                if (aggregations.length() > 0) {
                    esBody.put("aggs", aggregations);
                }
            }

            // Process WHERE clause
            if (whereClause != null) {
                JSONObject whereQuery = parseWhereClause(whereClause);
                esBody.put("query", whereQuery);
            } else if (!esBody.has("query")) {
                esBody.put("query", new JSONObject().put("match_all", new JSONObject()));
            }

            // Process ORDER BY clause
            if (orderByPart != null) {
                JSONArray sortArray = new JSONArray();
                String[] orderComponents = orderByPart.split("\\s*,\\s*");
                for (String component : orderComponents) {
                    component = component.trim();
                    String[] parts = component.split("\\s+");
                    String field = parts[0];
                    String direction = parts.length > 1 ? parts[1].toLowerCase() : "asc";
                    JSONObject sortObj = new JSONObject();
                    sortObj.put(field, new JSONObject().put("order", direction));
                    sortArray.put(sortObj);
                }
                esBody.put("sort", sortArray);
                
            }
            if (limitPart!=null) {
            	int limitInt = Integer.parseInt(limitPart);
            	esBody.put("size", limitInt);
            }
            result.put("body", esBody);
            
        }

        return result;
    }

    public static JSONObject parseWhereClause(String whereClause) {
        JSONObject boolQuery = new JSONObject().put("bool", new JSONObject());
        JSONArray must = new JSONArray();
        JSONArray should = new JSONArray();

        String[] tokens = whereClause.split("(?i)\\s+(?=(AND|OR))|(?<=(AND|OR))\\s+");
        String currentOp = "AND";

        for (String token : tokens) {
            token = token.trim();
            if (token.equalsIgnoreCase("AND")) {
                currentOp = "AND";
            } else if (token.equalsIgnoreCase("OR")) {
                currentOp = "OR";
            } else {
                JSONObject condition = parseCondition(token);
                if (condition != null) {
                    if ("AND".equals(currentOp)) {
                        must.put(condition);
                    } else {
                        should.put(condition);
                    }
                }
            }
        }

        JSONObject boolClauses = new JSONObject();
        if (must.length() > 0) {
            boolClauses.put("must", must);
        }
        if (should.length() > 0) {
            boolClauses.put("should", should);
            boolClauses.put("minimum_should_match", 1);
        }

        boolQuery.put("bool", boolClauses);
        return boolQuery;
    }

    private static JSONObject parseCondition(String condition) {
        JSONObject clause = new JSONObject();

   
        if (condition.toUpperCase().contains(" LIKE ") || condition.toUpperCase().contains(" ILIKE ")) {
            boolean isCaseInsensitive = condition.toUpperCase().contains(" ILIKE ");
            String[] parts = condition.split("(?i)\\s+LIKE\\s+|\\s+ILIKE\\s+");
            if (parts.length == 2) {
                String field = parts[0].trim();
                String value = parts[1].trim().replaceAll("['\";]", ""); 
 
                if (isCaseInsensitive) {
                  
                    String regexPattern = value
                        .replace("%", ".*")    
                        .replace("_", ".")   
                        .replaceAll("([.\\\\+*?\\[\\^\\]$(){}=!<>|:\\-])", "\\\\$1"); // Escape special chars
                    
                    JSONObject queryString = new JSONObject();
                    queryString.put("query", regexPattern);
                    queryString.put("analyze_wildcard", true);
                    queryString.put("default_field", field);
                    
                    clause.put("query_string", queryString);
                } else {
               
                    String wildcardValue = value
                        .replace("%", "*")    
                        .replace("_", "?");  
                    
                    clause.put("wildcard", new JSONObject().put(field, wildcardValue));                }
            }
        }

   
        else if (condition.contains("=")) {
            String[] parts = condition.split("\\s*=\\s*");
            String field = parts[0].trim();
            String value = parts[1].trim().replaceAll("['\";]", "");

            if (value.matches("-?\\d+(\\.\\d+)?")) { // Numeric values
                clause.put("term", new JSONObject().put(field, value));
            } else { // String values
                clause.put("match", new JSONObject().put(field, value));
            }
        }

        // Handle range conditions
        else if (condition.contains(">=")) {
            String[] parts = condition.split(">=");
            String field = parts[0].trim();
            double value = Double.parseDouble(parts[1].trim());
            clause.put("range", new JSONObject().put(field, new JSONObject().put("gte", value)));
            
        } else if (condition.contains("<=")) {
            String[] parts = condition.split("<=");
            String field = parts[0].trim();
            double value = Double.parseDouble(parts[1].trim());
            clause.put("range", new JSONObject().put(field, new JSONObject().put("lte", value)));
            
        } else if (condition.contains(">")) {
            String[] parts = condition.split(">");
            String field = parts[0].trim();
            double value = Double.parseDouble(parts[1].trim());
            clause.put("range", new JSONObject().put(field, new JSONObject().put("gt", value)));
            
        } else if (condition.contains("<")) {
            String[] parts = condition.split("<");
            String field = parts[0].trim();
            double value = Double.parseDouble(parts[1].trim());
            clause.put("range", new JSONObject().put(field, new JSONObject().put("lt", value)));
        }

        return clause;
    }
    
    public static JSONObject parseDistinctQuery(String query) {
        JSONObject result = new JSONObject();

        Pattern distinctPattern = Pattern.compile(
        	    "(?i)SELECT\\s+DISTINCT\\s+([\\w, ]+)\\s+FROM\\s+(\\w+)" +
        	    "(?:\\s+WHERE\\s+((?:(?!\\s+ORDER\\s+BY|\\s+LIMIT).)+))?" +
        	    "(?:\\s+ORDER\\s+BY\\s+((?:(?!\\s+LIMIT).)+))?" +
        	    "(?:\\s+LIMIT\\s+(\\d+))?",
        	    Pattern.CASE_INSENSITIVE | Pattern.DOTALL
        	);
        Pattern countDistinctPattern = Pattern.compile(
            "(?i)SELECT\\s+COUNT\\(\\s*DISTINCT\\s+(\\w+)\\s*\\)\\s+FROM\\s+(\\w+)",Pattern.CASE_INSENSITIVE
        );

        Matcher distinctMatcher = distinctPattern.matcher(query);
        Matcher countDistinctMatcher = countDistinctPattern.matcher(query);

        if (distinctMatcher.find()) {

        	String columnName = distinctMatcher.group(1).trim();    
        	System.out.println("columnName: "+columnName);
            String tableName =distinctMatcher.group(2).trim();
            String whereClause = distinctMatcher.group(3) != null ?distinctMatcher.group(3).trim() : null;
            System.out.println("whereClause: "+whereClause);
            String orderByColumn = distinctMatcher.group(4) != null ? distinctMatcher.group(4).trim() : null;
            System.out.println("orderByColumn: "+orderByColumn);


            JSONObject terms = new JSONObject();
            terms.put("field", columnName);
            terms.put("size", 100);

            JSONObject unq = new JSONObject();
            unq.put("terms", terms);

            JSONObject aggs = new JSONObject();
            aggs.put("unique_values", unq);

            JSONObject body = new JSONObject();
            body.put("size", 0);
            body.put("aggs", aggs);

     
            if (whereClause != null) {
            	 JSONObject whereQuery = parseWhereClause(whereClause);
            	 System.out.println(whereQuery);
                 body.put("query", whereQuery);
             } else if (!result.has("query")) {
                 body.put("query", new JSONObject().put("match_all", new JSONObject()));
             
            }


            if (orderByColumn != null) {
                JSONArray sortArray = new JSONArray();
                String[] orderComponents = orderByColumn.split("\\s*,\\s*");
                for (String component : orderComponents) {
                    component = component.trim();
                    String[] parts = component.split("\\s+");
                    String field = parts[0];
                    String direction = parts.length > 1 ? parts[1].toLowerCase() : "asc";
                    JSONObject sortObj = new JSONObject();
                    sortObj.put(field, new JSONObject().put("order", direction));
                    sortArray.put(sortObj);
                }
                body.put("sort", sortArray);
                
            }

            result.put("table", tableName);
            result.put("body", body);
        }

         else if (countDistinctMatcher.find()) {
     
            String columnName = countDistinctMatcher.group(1).trim();
            String tableName = countDistinctMatcher.group(2).trim();

            JSONObject cardinality = new JSONObject();
            cardinality.put("field", columnName);
            cardinality.put("precision_threshold", 10000); 

            JSONObject aggs = new JSONObject();
            aggs.put("distinct_count", new JSONObject().put("cardinality", cardinality));

            JSONObject body = new JSONObject();
            body.put("size", 0);
            body.put("aggs", aggs);

            result.put("table", tableName);
            result.put("body", body);
        }

        return result;
    }

    public static boolean parseSchemaNames(String query) {


        boolean result = false;
        Pattern pattern = Pattern.compile(
            "SELECT\\s+tablename\\s+FROM\\s+pg_tables\\s+WHERE\\s+schemaname\\s*=\\s*'([\\w]+)'\\s*",
            Pattern.CASE_INSENSITIVE
        );
        
        Matcher matcher = pattern.matcher(query);
        if (matcher.find()) {
            String schema = matcher.group(1);
            result = schema.equals("elasticsearch");
        }
        
        return result;
    }
    
    
    public static JSONObject parseJoinQuery(String query) {
        JSONObject result = new JSONObject();
        JSONObject columnTableMap = new JSONObject();


        Pattern selectPattern = Pattern.compile("SELECT\\s+(.+?)\\s+FROM", Pattern.CASE_INSENSITIVE);
        Matcher selectMatcher = selectPattern.matcher(query);
        if (selectMatcher.find()) {
            String columns = selectMatcher.group(1).trim();
            result.put("columns", columns);


            String[] columnArray = columns.split("\\s*,\\s*");

            for (String column : columnArray) {
                if (column.contains(".")) {
          
                    String[] parts = column.split("\\.");
                    columnTableMap.put(parts[1], parts[0]);
                } else {

                    columnTableMap.put(column, "table1");
                }
            }
        }


        Pattern fromPattern = Pattern.compile("FROM\\s+(\\w+)", Pattern.CASE_INSENSITIVE);
        Matcher fromMatcher = fromPattern.matcher(query);
        if (fromMatcher.find()) {
            String table1 = fromMatcher.group(1).trim();
            result.put("table1", table1);
        }


        Pattern joinPattern = Pattern.compile("(INNER|LEFT|RIGHT|FULL)\\s+JOIN\\s+(\\w+)", Pattern.CASE_INSENSITIVE);
        Matcher joinMatcher = joinPattern.matcher(query);
        if (joinMatcher.find()) {
            result.put("joinType", joinMatcher.group(1).trim());
            result.put("table2", joinMatcher.group(2).trim());
        }

        Pattern onPattern = Pattern.compile("ON\\s+(.+?)\\s*(?:WHERE|GROUP BY|ORDER BY|$)", Pattern.CASE_INSENSITIVE);
        Matcher onMatcher = onPattern.matcher(query);
        if (onMatcher.find()) {
            String onCondition = onMatcher.group(1).trim();
            result.put("onCondition", onCondition);

          
            String[] parts = onCondition.split("\\s*=\\s*");
            if (parts.length == 2) {
                JSONObject onMapping = new JSONObject();

    
                String[] left = parts[0].split("\\.");
                if (left.length == 2) {
                    onMapping.put("leftTable", left[0].trim());
                    onMapping.put("leftColumn", left[1].trim());
                }

     
                String[] right = parts[1].split("\\.");
                if (right.length == 2) {
                    onMapping.put("rightTable", right[0].trim());
                    onMapping.put("rightColumn", right[1].trim());
                }

                result.put("onMapping", onMapping);
            }
        }
        

        result.put("columnTableMapping", columnTableMap);
        System.out.println(result.toString());
        
        
        return result;
    }
    
    public static JSONObject extractQueryFields(JSONObject result) {
        JSONObject queryFieldsJson = new JSONObject();

        String table1 = result.getString("table1");
        String table2 = result.getString("table2");

        JSONObject columnTableMapping = result.getJSONObject("columnTableMapping");

        List<String> table1Fields = new ArrayList<>();
        List<String> table2Fields = new ArrayList<>();

   
        for (String column : columnTableMapping.keySet()) {
            String table = columnTableMapping.getString(column);
            if (table.equals(table1)) {
                table1Fields.add(column);
            } else if (table.equals(table2)) {
                table2Fields.add(column);
            }
        }

   
        JSONObject onMapping = result.getJSONObject("onMapping");
        String leftColumn = onMapping.getString("leftColumn");
        String rightColumn = onMapping.getString("rightColumn");


        if (!table1Fields.contains(leftColumn)) {
            table1Fields.add(leftColumn);
        }
        if (!table2Fields.contains(rightColumn)) {
            table2Fields.add(rightColumn);
        }

        queryFieldsJson.put(table1, new JSONArray(table1Fields));
        queryFieldsJson.put(table2, new JSONArray(table2Fields));

        System.out.println(queryFieldsJson);
        return queryFieldsJson;
    }

    

}
