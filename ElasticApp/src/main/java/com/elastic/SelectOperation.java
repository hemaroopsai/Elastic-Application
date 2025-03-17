package com.elastic;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json.JSONArray;
import org.json.JSONObject;

public class SelectOperation {

    public static JSONObject parseSelectQuery(String query) {
        JSONObject result = new JSONObject();
        JSONObject esBody = new JSONObject();

        Pattern pattern = Pattern.compile(
        		"SELECT\\s+([^\\s,]+(?:\\s*,\\s*[^\\s,]+)*)\\s+FROM\\s+([\\w_]+)(?:\\s+WHERE\\s+(.+?))?(?:\\s+ORDER\\s+BY\\s+(.+?))?",
                Pattern.CASE_INSENSITIVE
        );
        Matcher matcher = pattern.matcher(query);

        if (matcher.find()) {
            String fieldsPart = matcher.group(1).trim();
            String tableName = matcher.group(2).trim().replace(";", "");
            String whereClause = matcher.group(3) != null ? matcher.group(3).trim() : null;
            String orderByPart = matcher.group(4) != null ? matcher.group(4).trim() : null;
            System.out.println(orderByPart);

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

                    // Check for aliases
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
                            countAgg.put("value_count", new JSONObject().put("field", "_index")); // Using _id for COUNT(*)
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
                        sourceIncludes.put(field); // Add non-aggregate field to _source
                    }
                }

                // Add _source and aggregations to esBody
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

            result.put("body", esBody);
        }

        return result;
    }

    public static JSONObject parseWhereClause(String whereClause) {
        JSONObject boolQuery = new JSONObject().put("bool", new JSONObject());
        JSONArray must = new JSONArray();
        JSONArray should = new JSONArray();

        // Split conditions by AND/OR, keeping the operators
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
                if ("AND".equals(currentOp)) {
                    must.put(condition);
                } else {
                    should.put(condition);
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

        // Ensure "bool" exists in boolQuery
        boolQuery.put("bool", boolClauses);

        return boolQuery;

    }

    private static JSONObject parseCondition(String condition) {
        JSONObject clause = new JSONObject();

        if (condition.contains("=")) {
            String[] parts = condition.split("\\s*=\\s*");
            String field = parts[0].trim();
            String value = parts[1].trim().replaceAll("['\";]", "");

            if (value.matches("-?\\d+(\\.\\d+)?")) {
                // Numeric value, use term query
                clause.put("term", new JSONObject().put(field + ".keyword", value)); // Assuming keyword mapping for exact match
            } else {
                // Text value, use match query
                clause.put("match", new JSONObject().put(field, value));
            }

        } else if (condition.contains(">=")) {
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
}