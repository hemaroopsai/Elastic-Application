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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;



public class ElasticsearchClient {
	private static ElasticsearchClient instance = null;
    private RestClient client;


    public ElasticsearchClient() {
        this.client = RestClient.builder(
                new HttpHost("localhost", 9200, "http")
        ).build();
        

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down Elasticsearch client...");
            close(); 
        }));
    }

    
    public static synchronized ElasticsearchClient getInstance() {
        if (instance == null) {
            instance = new ElasticsearchClient();
        } else {
            instance.close(); // Ensure old instance is closed
            instance = new ElasticsearchClient();
        }
        return instance;
    }
    
    //builder
    private String processResponse(Response response) throws IOException {
        BufferedReader reader = new BufferedReader(
            new InputStreamReader(response.getEntity().getContent())
        );
        StringBuilder result = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            result.append(line);
        }
        
        System.out.println("Raw Elasticsearch Response: " + result.toString());
        System.out.println();
        return result.toString();
    }
    
    public String updateOperation(JSONObject parsedQuery) throws Exception{
    	String index = parsedQuery.getString("index");
    	parsedQuery.remove("index");
    	System.out.println(parsedQuery);
    	HttpEntity entity = new NStringEntity(parsedQuery.toString(), ContentType.APPLICATION_JSON);
    	
    	Response response = client.performRequest(
				"POST",
				index+"/_update_by_query",
				Collections.emptyMap(),
				entity
			);

    	return processResponse(response).toString();
    }
    
  

    public String alterOperation(JSONObject parsedQuery) throws Exception{
    	JSONObject mappings = parsedQuery.getJSONObject("mappings");
    	String index = parsedQuery.getString("index");
    	System.out.println(mappings);
    	HttpEntity entity = new NStringEntity(mappings.toString(), ContentType.APPLICATION_JSON);
    	
    	Response response = client.performRequest(
    				"PUT",
    				"/"+index+"/_mappings"+"/"+"doc",
    				Collections.emptyMap(),
    				entity
    			);
    	return processResponse(response).toString();
    	
    }

    
    public String groupByOperation(String index,JSONObject query) throws Exception{
    	
    try {
    		String queryString = query.toString();
    	
    		System.out.println(queryString);
    		HttpEntity entity = new NStringEntity(queryString, ContentType.APPLICATION_JSON);

    		Response response = client.performRequest(
    				"GET",
    				"/" + index + "/_search", 
    				Collections.emptyMap(), 
    				entity 
    				);
        
    		return processResponse(response).toString();
    } catch(Exception e) {
    	e.printStackTrace();
    	return e.getMessage();
    }
    		
    }
    
    public String insertIntoIndex(JSONObject fields) throws IOException {


        String index = fields.getString("table"); 
        String endpoint = "/" + index + "/_bulk";

        JSONArray fieldNames = fields.getJSONArray("fields"); // Column names
        JSONArray valuesArray = fields.getJSONArray("values"); // Data values
        
        StringBuilder bulkData = new StringBuilder();

        // Iterate through each row
        for (int i = 0; i < valuesArray.length(); i++) {
            JSONArray rowValues = valuesArray.getJSONArray(i);
            JSONObject document = new JSONObject();

            for (int j = 0; j < fieldNames.length(); j++) {
                document.put(fieldNames.getString(j), rowValues.get(j));
            }

            // Append to bulk request
            bulkData.append("{\"index\": { \"_index\": \"").append(index).append("\" , \"_type\":\"doc\"").append("}}\n");
            bulkData.append(document.toString()).append("\n");
        }

        if (bulkData.length() == 0) {
            throw new IOException("No valid documents to insert.");
        }
        	System.out.println(bulkData.toString());
        try {
            HttpEntity entity = new NStringEntity(bulkData.toString(), ContentType.APPLICATION_JSON);
            Response response = client.performRequest(
            		"POST", 
            		endpoint, 
            		Collections.emptyMap(), 
            		entity);
            
            return processResponse(response).toString();
            
        } catch (Exception e) {
            throw new IOException("Failed to insert document: " + e.getMessage(), e);
        }
    }


    
    public String deleteByQuery(String index,  JSONObject query ) throws IOException {
        try {
        	String type ="doc";
            String endpoint = "/" + index + "/" + type + "/_delete_by_query";
            
            HttpEntity entity = new NStringEntity(query.toString(), ContentType.APPLICATION_JSON);
            Response response = client.performRequest(
            		"POST", 
            		endpoint, 
            		Collections.emptyMap(),
            		entity);

            return processResponse(response).toString();
        } catch (Exception e) {
            throw new IOException("Failed to delete documents by query: " + e.getMessage(), e);
        }
    }
    
    public String deleteByIndex(String index) {
    	 try {
    	    
    	        String jsonQuery = "{ \"query\": { \"match_all\": {} } }";
    	        String type ="doc";
    	        String endpoint = "/" + index + "/" + type + "/_delete_by_query";
    	        HttpEntity entity = new NStringEntity(jsonQuery, ContentType.APPLICATION_JSON);
    	        Response response = client.performRequest(
                		"POST", 
                		endpoint, 
                		Collections.emptyMap(),
                		entity);

                return processResponse(response).toString();

    	    } catch (Exception e) {
    	        e.printStackTrace();
    	        return "Error deleting documents: " + e.getMessage();
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

         
            return processResponse(response).toString();
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

          
            return processResponse(response).toString();
            
        } catch (Exception e) {
            throw new IOException("Failed to create index: " + e.getMessage(), e);
        }
    }

    //workon
    //select operations
    public String selectOperation(JSONObject selectObj) throws IOException {
    	try {
        String index = selectObj.getString("table"); 
        

        JSONObject queryBody = selectObj.getJSONObject("body"); 
        if (queryBody.has("aggs")) {
        	queryBody.put("size",0);
        }
   
        if (!queryBody.has("aggs") && !queryBody.has("size")) {
            queryBody.put("size", 10000);
        }


        HttpEntity entity = new NStringEntity(queryBody.toString(), ContentType.APPLICATION_JSON);

        
        Response response = client.performRequest(
        		"GET", "/" + index + "/_search", 
            Collections.emptyMap(),
            entity
        );

        
        return processResponse(response).toString();
    	}
    	catch(Exception e) {
    		e.printStackTrace();
    		return e.getMessage();
    	}
    }
    public String SchemaRepresentation() throws IOException {
    	 HttpEntity entity = new NStringEntity("{}", ContentType.APPLICATION_JSON);


         Response response = client.performRequest(
             "GET", "_cat/indices?v",
             Collections.emptyMap(), entity
         );
         return processResponse(response).toString();
    }



    public void close() {
        if (client != null) {
            try {
                client.close();
                System.out.println("Elasticsearch client closed successfully.");
            } catch (IOException e) {
                System.err.println("Error closing Elasticsearch client: " + e.getMessage());
            } finally {
                client = null; 
            }
        }
    }


}