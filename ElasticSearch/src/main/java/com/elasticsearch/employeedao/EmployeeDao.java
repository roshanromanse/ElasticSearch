package com.elasticsearch.employeedao;


import com.elasticsearch.dto.Employee;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class EmployeeDao {
    private EmployeeDao employeeDao;
    private static RestHighLevelClient restHighLevelClient;
    private static SearchRequest searchRequest;
    private static SearchSourceBuilder searchSourceBuilder;
    private static SearchResponse searchResponse = null;
    private static IndexRequest indexRequest;
    private static IndexResponse indexResponse = null;
    ObjectMapper objectMapper = new ObjectMapper();

    private static synchronized RestHighLevelClient getConnection() throws IOException {
        if(restHighLevelClient == null) {
            Properties properties = new Properties();
            InputStream inputStream = new FileInputStream("D:\\workspace\\maven-examples\\ElasticSearch\\src\\main\\resources\\application.properties");
            properties.load(inputStream);
            String host = properties.getProperty("hostName");
            int port = Integer.parseInt(properties.getProperty("port"));
            String scheme = properties.getProperty("scheme");
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(host, port, scheme)));
        }
        return restHighLevelClient;
    }

    public void getEmployee()  {
        try {
            restHighLevelClient = employeeDao.getConnection();
            searchRequest = new SearchRequest("test");
            searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());                            //to get all data
            //searchSourceBuilder.query(QueryBuilders.matchQuery("name", "Jefri"));     //to get specific data
            //searchSourceBuilder.query(QueryBuilders.rangeQuery("id").from(2).to(5));
            searchRequest.source(searchSourceBuilder);
            searchResponse =restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits hits = searchResponse.getHits();
            String source="";
            Employee employee = new Employee();
            for (SearchHit hit : hits.getHits())
            {
                 source = hit.getSourceAsString();
                 employee=objectMapper.readValue(source, Employee.class);
                System.out.println(employee.getId()+" "+employee.getName());
            }
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    public void insertEmployee()  {
        try{
            restHighLevelClient = employeeDao.getConnection();
            Employee employee = new Employee();
            employee.setId(11);
            employee.setName("Titian");
            indexRequest = new IndexRequest("test");
            indexRequest.source(objectMapper.writeValueAsString(employee), XContentType.JSON);
            indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
            System.out.println("response id: " + indexResponse.getId());
            System.out.println("response name: " + indexResponse.getResult().name());
        }catch (Exception e) {
            System.out.println(e);
        }
    }
}
