package com.elastic;

import org.json.JSONArray;
import org.json.JSONObject;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GroupByFunc {

    public static JSONObject parseAggregationQuery(String query) {
        JSONObject esQuery = new JSONObject();

       
        Pattern pattern = Pattern.compile(
            "SELECT\\s+(\\*|.*?)\\s+FROM\\s+(\\w+)(?:\\s+WHERE\\s+(.*?))?(?:\\s+GROUP BY\\s+(.*?))?(?:\\s+ORDER BY\\s+(.*?))?;?$",
      
            Pattern.CASE_INSENSITIVE
        );
        Matcher matcher = pattern.matcher(query);

        if (matcher.find()) {
            String fieldsPart = matcher.group(1).trim();
            String table = matcher.group(2).trim(); // Index name
            String wherePart = matcher.group(3) != null ? matcher.group(3).trim() : null; // WHERE clause
            String groupByPart = matcher.group(4) != null ? matcher.group(4).trim() : null; // GROUP BY clause
            String orderByPart = matcher.group(5) != null ? matcher.group(5).trim() : null; // ORDER BY clause

            esQuery.put("table", table);
            esQuery.put("size", 0); 

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
                String[] groupByFields = groupByPart.split("\\s*,\\s*");
                JSONObject nestedAgg = aggregations;
                JSONObject parentGroupBy = new JSONObject();

                for (int i = groupByFields.length - 1; i >= 0; i--) {
                    String groupByField = groupByFields[i].trim().replace(";", "");

                    JSONObject groupByAgg = new JSONObject();
                    JSONObject terms = new JSONObject();
                    terms.put("field", groupByField);
                    terms.put("size", 100);

               
                    if (orderByPart != null) {
                        JSONObject sortOrder = parseOrderByClause(orderByPart);
                        terms.put("order", sortOrder);
                    }


                    groupByAgg.put("terms", terms);

                    if (!nestedAgg.isEmpty()) {
                        groupByAgg.put("aggs", nestedAgg);
                    }

                    parentGroupBy = new JSONObject();
                    parentGroupBy.put(groupByField + "_group", groupByAgg);
                    nestedAgg = parentGroupBy;
                }

                esQuery.put("aggs", parentGroupBy);
            } else if (!aggregations.isEmpty()) {
                esQuery.put("aggs", aggregations);

                // If there's no GROUP BY, place ORDER BY at root level
                if (orderByPart != null) {
                    JSONObject sortArray = parseOrderByClause(orderByPart);
                    esQuery.put("sort", sortArray);
                }
            }
        }

        return esQuery;
    }

    public static JSONObject parseOrderByClause(String orderByPart) {
        JSONObject orderObject = new JSONObject();

        String[] parts = orderByPart.split("\\s+");
        String field = parts[0].trim();
        String order = parts.length > 1 ? parts[1].trim().toLowerCase() : "asc"; 

    
        if (field.equalsIgnoreCase("desc") || field.equalsIgnoreCase("asc")) {
            orderObject.put("_term", order);
        } else if (field.equalsIgnoreCase("count")) {
            orderObject.put("_count", order);
        } else {

            orderObject.put(field, new JSONObject().put("order", order));
        }

        return orderObject;
    }

}
