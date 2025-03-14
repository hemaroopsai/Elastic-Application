package com.elastic;

import org.json.JSONArray;
import org.json.JSONObject;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GroupByFunc {

    public static JSONObject parseAggregationQuery(String query) {
        JSONObject result = new JSONObject();
        JSONObject esQuery = new JSONObject();

        
        Pattern pattern = Pattern.compile(
            "SELECT\\s+(.*?)\\s+FROM\\s+(\\w+)(?:\\s+GROUP BY\\s+(.*))?",
            Pattern.CASE_INSENSITIVE
        );
        Matcher matcher = pattern.matcher(query);

        if (matcher.find()) {
            String fieldsPart = matcher.group(1).trim(); // Fields and aggregation functions
            String tableName = matcher.group(2).trim(); // Table name (index)
            String groupByPart = matcher.group(3) != null ? matcher.group(3).trim() : null; // GROUP BY fields (if exists)

            result.put("table", tableName);

            // Parse fields and aggregation functions
            JSONObject aggregations = new JSONObject();
            String[] fieldArray = fieldsPart.split("\\s*,\\s*");

            for (String field : fieldArray) {
                if (field.toLowerCase().startsWith("count(")) {
                    String countField = field.replaceAll("(?i)count\\(|\\)", "").trim();
                    if (countField.equals("*")) {
                        aggregations.put("total_count", new JSONObject().put("value_count", new JSONObject().put("field", "_index")));
                    } else {
                        aggregations.put(countField + "_count", new JSONObject().put("value_count", new JSONObject().put("field", countField)));
                    }
                } else if (field.toLowerCase().startsWith("sum(")) {
                    String sumField = field.replaceAll("(?i)sum\\(|\\)", "").trim();
                    aggregations.put(sumField + "_sum", new JSONObject().put("sum", new JSONObject().put("field", sumField)));
                } else if (field.toLowerCase().startsWith("avg(")) {
                    String avgField = field.replaceAll("(?i)avg\\(|\\)", "").trim();
                    aggregations.put(avgField + "_avg", new JSONObject().put("avg", new JSONObject().put("field", avgField)));
                }
            }

            // Parse GROUP BY fields
            if (groupByPart != null) {
                String groupByField = groupByPart.split("\\s*,\\s*")[0].trim().replace(";", ""); // Assuming single GROUP BY for now
                JSONObject groupByAgg = new JSONObject();
                groupByAgg.put("terms", new JSONObject().put("field", groupByField));
                
                // Nesting aggregation inside GROUP BY
                if (!aggregations.isEmpty()) {
                    groupByAgg.put("aggregations", aggregations);
                }
                
                esQuery.put("aggregations", new JSONObject().put(groupByField + "_group", groupByAgg));
            } else {
                esQuery.put("aggregations", aggregations);
            }
        }

        return esQuery;
    }

}
