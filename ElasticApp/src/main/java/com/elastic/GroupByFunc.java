package com.elastic;

import org.json.JSONArray;
import org.json.JSONObject;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GroupByFunc {

    public static JSONObject parseAggregationQuery(String query) {
        JSONObject esQuery = new JSONObject();

       System.out.println(query);
        Pattern pattern = Pattern.compile(
//        		"SELECT\\s+(.+?)\\s+FROM\\s+(\\w+)" +
//        	               "(?:\\s+WHERE\\s+((?:(?!\\bGROUP\\s+BY\\b|\\bORDER\\s+BY\\b|\\bLIMIT\\b).)*))?" +
//        	               "(?:\\s+GROUP\\s+BY\\s+((?:(?!\\bORDER\\s+BY\\b|\\bLIMIT\\b).)*))?" +
//        	               "(?:\\s+ORDER\\s+BY\\s+((?:(?!\\bLIMIT\\b).)*))?" +
//        	               "(?:\\s+LIMIT\\s+(\\d+))?",
        		
        			    "SELECT\\s+(.+?)\\s+FROM\\s+(\\w+)" +
        			    "(?:\\s+WHERE\\s+((?:(?!\\s+GROUP\\s+BY|\\s+ORDER\\s+BY|\\s+LIMIT).)+))?" +
        			    "(?:\\s+GROUP\\s+BY\\s+((?:(?!\\s+ORDER\\s+BY|\\s+LIMIT).)+))?" +
        			    "(?:\\s+ORDER\\s+BY\\s+((?:(?!\\s+LIMIT).)+))?" +
        			    "(?:\\s+LIMIT\\s+(\\d+))?" +
        			    "\\s*;?",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL
            );
        Matcher matcher = pattern.matcher(query);

        if (matcher.find()) {
            String fieldsPart = matcher.group(1).trim();
            System.out.println(fieldsPart);
            String table = matcher.group(2).trim(); // Index name
            
            String wherePart = matcher.group(3) != null ? matcher.group(3).trim() : null; // WHERE clause
            System.out.println(wherePart);
            String groupByPart = matcher.group(4) != null ? matcher.group(4).trim() : null; // GROUP BY clause
            System.out.println(groupByPart);
            String orderByPart = matcher.group(5) != null ? matcher.group(5).trim() : null; // ORDER BY clause
            System.out.println(orderByPart);
            String limitPart = matcher.group(6) != null ? matcher.group(6).trim() : null;
            System.out.println(limitPart);
            
            esQuery.put("table", table);
            if (wherePart != null) {
                JSONObject whereClauseQuery = SelectOperation.parseWhereClause(wherePart);
                esQuery.put("query", whereClauseQuery);
            }

            JSONObject aggregations = new JSONObject();

     
            if (!fieldsPart.equals("*")) {
                JSONArray sourceFields = new JSONArray();
                String[] fieldArray = fieldsPart.split("\\s*,\\s*");

                for (String field : fieldArray) {
                    field = field.trim();

            
                    String alias = null;
                    if (field.toLowerCase().contains(" as ")) {
                        String[] parts = field.split("(?i)\\s+as\\s+");
                        field = parts[0].trim(); 
                        alias = parts[1].trim(); 
                    }

                    if (field.toLowerCase().startsWith("count(")) {
                        String countField = field.replaceAll("(?i)count\\(|\\)", "").trim();
                        JSONObject countAgg = new JSONObject();
                        countAgg.put("value_count", new JSONObject().put("field", countField.equals("*") ? "_index" : countField));

                   
                        String aggName = (alias != null) ? alias : countField + "_count";
                        aggregations.put(aggName, countAgg);

                    } else if (field.toLowerCase().startsWith("sum(")) {
                        String sumField = field.replaceAll("(?i)sum\\(|\\)", "").trim();
                        JSONObject sumAgg = new JSONObject();
                        sumAgg.put("sum", new JSONObject().put("field", sumField));

                        String aggName = (alias != null) ? alias : sumField + "_sum";
                        aggregations.put(aggName, sumAgg);

                    } else if (field.toLowerCase().startsWith("avg(")) {
                        String avgField = field.replaceAll("(?i)avg\\(|\\)", "").trim();
                        JSONObject avgAgg = new JSONObject();
                        avgAgg.put("avg", new JSONObject().put("field", avgField));

                        String aggName = (alias != null) ? alias : avgField + "_avg";
                        aggregations.put(aggName, avgAgg);
                        
                    } else if (field.toLowerCase().startsWith("min(")) {
                        String minField = field.replaceAll("(?i)min\\(|\\)", "").trim();
                        JSONObject minAgg = new JSONObject();
                        minAgg.put("min", new JSONObject().put("field", minField));
                        String aggName = (alias != null) ? alias : minField + "_min";
                        aggregations.put(aggName, minAgg);

                    } else if (field.toLowerCase().startsWith("max(")) {
                        String maxField = field.replaceAll("(?i)max\\(|\\)", "").trim();
                        JSONObject maxAgg = new JSONObject();
                        maxAgg.put("max", new JSONObject().put("field", maxField));
                        String aggName = (alias != null) ? alias : maxField + "_max";
                        aggregations.put(aggName, maxAgg);


                    } else {
                        sourceFields.put(field);
                    }
                }

                if (!sourceFields.isEmpty()) {
                    JSONObject topHitsAgg = new JSONObject();
                    topHitsAgg.put("top_hits", new JSONObject().put("_source", sourceFields).put("size", 10));
                    aggregations.put("selected_fields", topHitsAgg);
                }
            }

            // Handle GROUP BY clause
            if (groupByPart != null) {
            	
                esQuery.put("size", 0);
                

                JSONObject groupAgg = new JSONObject();
                JSONObject terms = new JSONObject();
                terms.put("field", groupByPart);
                if (orderByPart != null) {
                	JSONObject sortObject = parseOrderClause(orderByPart);
                    terms.put("order", sortObject);
                }
 
                if (limitPart != null) {
                    terms.put("size", Integer.parseInt(limitPart));
                } else {
                    terms.put("size", 10000);
                }
                groupAgg.put("terms", terms);
                
          
                if (aggregations.length() > 0) {
                    groupAgg.put("aggs", aggregations);
                }

                JSONObject groupByAggs = new JSONObject();
                groupByAggs.put("group_by_" + groupByPart, groupAgg);
          
                esQuery.put("aggs", groupByAggs);
            }
        }
         return esQuery;
     }

    public static JSONObject parseOrderClause(String orderByPart) {
        JSONObject orderObject = new JSONObject();
        
        if (orderByPart != null && !orderByPart.trim().isEmpty()) {
            String[] parts = orderByPart.trim().split("\\s+");
            if (parts.length > 0) {
                String field = parts[0].trim();
                String order = (parts.length > 1) ? parts[1].trim().toLowerCase() : "asc";
                
                
                if (field.equalsIgnoreCase("count") || field.equalsIgnoreCase("count(*)")) {
                    orderObject.put("_count", order);
                } else {
                 
                    orderObject.put("_term", order);
                }
            }
        }
        return orderObject;
    }



}
