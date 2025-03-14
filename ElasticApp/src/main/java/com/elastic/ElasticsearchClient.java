package com.elastic;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpHead;
import org.elasticsearch.client.RestClient;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;

public class ElasticsearchClient {
    private RestClient client;


    public ElasticsearchClient() {
        this.client = RestClient.builder(
                new HttpHost("localhost", 9200, "http")
        ).build();
    }
    
    public String insertIntoIndex(JSONObject fields) throws IOException {
        if (fields == null) {
            throw new IOException("Fields object is null. Cannot insert.");
        }

        if (!fields.has("table")) {
            throw new IOException("Fields object does not contain 'table' key.");
        }

        String index = fields.getString("table");
        String type = "doc";
        
        

        JSONArray fieldsArray = fields.getJSONArray("fields");
        System.out.println(fieldsArray);
        JSONObject esDoc = new JSONObject();
        for (int i = 0; i < fieldsArray.length(); i++) {
            JSONObject fieldObj = fieldsArray.getJSONObject(i);
            esDoc.put(fieldObj.getString("field"), fieldObj.get("value"));
        }
        System.out.println(esDoc);
        String endpoint = "/" + index + "/" + type + "/";
        
        try {
            HttpEntity entity = new NStringEntity(esDoc.toString(), ContentType.APPLICATION_JSON);
            Response response = client.performRequest("POST", endpoint, Collections.emptyMap(), entity);
            
            BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            StringBuilder result = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                result.append(line);
            }
            return result.toString();
            
        } catch (Exception e) {
            throw new IOException("Failed to insert document: " + e.getMessage(), e);
        }
    }


    
    public String deleteByQuery(String index,  String fieldName, String fieldValue) throws IOException {
        try {
        	String type ="doc";
            String endpoint = "/" + index + "/" + type + "/_delete_by_query";
          
      
            JSONObject matchQuery = new JSONObject();
            matchQuery.put(fieldName, fieldValue);

            JSONObject query = new JSONObject();
            query.put("match", matchQuery);

            JSONObject requestBody = new JSONObject();
            requestBody.put("query", query);

            // Convert JSON object to string
            String jsonBody = requestBody.toString();
            System.out.println(index+","+fieldName+","+fieldValue);
          
            HttpEntity entity = new NStringEntity(jsonBody, ContentType.APPLICATION_JSON);
            Response response = client.performRequest("POST", endpoint, Collections.emptyMap(), entity);

            BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            StringBuilder result = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                result.append(line);
            }

            return result.toString();
        } catch (Exception e) {
            throw new IOException("Failed to delete documents by query: " + e.getMessage(), e);
        }
    }



    
    
    public String deleteIndex(String index) throws IOException {
        try {
            String endpoint = "/" + index;

        
            Response response = client.performRequest(
                    "DELETE",
                    endpoint,
                    Collections.emptyMap()
            );

         
            BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            StringBuilder result = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                result.append(line);
            }

            return result.toString();
        } catch (Exception e) {
            throw new IOException("Failed to delete index: " + e.getMessage(), e);
        }
    }


    
    
    public boolean indexExists(String index) {
        String url = "http://localhost:9200/" + index;
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpHead request = new HttpHead(url);
            try (CloseableHttpResponse response = httpClient.execute(request)) {
                int statusCode = response.getStatusLine().getStatusCode();
                return statusCode == 200 || statusCode == 403 || statusCode == 401;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }



    
    
    //create table
    public String createIndex(String index, String mappings) throws IOException {
        try {
            JSONObject requestBody = new JSONObject();
            requestBody.put("mappings", new JSONObject(mappings));
            System.out.println(mappings);
            
            HttpEntity entity = new NStringEntity(requestBody.toString(), ContentType.APPLICATION_JSON);
            System.out.println(index);
            Response response = client.performRequest(
                    "PUT",
                    "/" + index,
                    Collections.emptyMap(),
                    entity
            );

          
            BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            StringBuilder result = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                result.append(line);
            }

            return result.toString();
        } catch (Exception e) {
            throw new IOException("Failed to create index: " + e.getMessage(), e);
        }
    }

    //workon
    //select operations
    public String selectOperation(JSONObject selectObj) throws IOException {
        String index = selectObj.getString("table"); 
        System.out.println(index);
        JSONObject queryBody = selectObj.getJSONObject("query"); 
        queryBody.put("size", 10000);



        System.out.println(queryBody);

        HttpEntity entity = new NStringEntity(queryBody.toString(), ContentType.APPLICATION_JSON);


        Response response = client.performRequest(
        		"GET", "/" + index + "/_search", 
            Collections.emptyMap(),
            entity
        );


        BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
        StringBuilder result = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            result.append(line);
        }
        System.out.println(result);
        return result.toString();
    }
    



    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }
}