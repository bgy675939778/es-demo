package com.ctyun.devops.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.RequiredArgsConstructor;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.stereotype.Component;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class ESSyncDataManager {
    private final RestHighLevelClient highLevelClient;

    public void syncAllData() {
        try {

            Map<String, Object> settings = this.getESConfigMap("src/main/resources/es/settings/settings.json");
            Map<String, Object> departmentMappings = this.getESConfigMap("src/main/resources/es/mappings/department-mappings.json");
            Map<String, Object> productMappings = this.getESConfigMap("src/main/resources/es/mappings/product-mappings.json");
            Map<String, Object> employeeMappings = this.getESConfigMap("src/main/resources/es/mappings/employee-mappings.json");

            long time = System.currentTimeMillis();
            String departmentIndex = "department_" + time;
            String productIndex = "product_" + time;
            String employeeIndex = "employee_" + time;

            System.out.println("time: " + time);
//            // 创建 department 索引
//            CreateIndexRequest departmentCreateIndexRequest = new CreateIndexRequest(departmentIndex).mapping(departmentMappings).settings(settings);
//            highLevelClient.indices().create(departmentCreateIndexRequest, RequestOptions.DEFAULT);
//
//            // 创建 product 索引
//            CreateIndexRequest productCreateIndexRequest = new CreateIndexRequest(productIndex).mapping(productMappings).settings(settings);
//            highLevelClient.indices().create(productCreateIndexRequest, RequestOptions.DEFAULT);

            // 创建 employee 索引
            CreateIndexRequest employeeCreateIndexRequest = new CreateIndexRequest(employeeIndex).mapping(employeeMappings).settings(settings);
            highLevelClient.indices().create(employeeCreateIndexRequest, RequestOptions.DEFAULT);

            // 组织数据格式
            List<JSONObject> employees = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                JSONObject employee = new JSONObject();
                employee.put("id", i);
                employee.put("name", "name" + i);
                employee.put("describe", "desc" + i);
                employees.add(employee);
            }

            BulkRequest request = new BulkRequest();
            for (JSONObject employee : employees) {
                request.add(new IndexRequest(employeeIndex).id(employee.getString("id"))
                        .opType("create").source(employee.toJSONString(), XContentType.JSON));
            }
            BulkResponse response = highLevelClient.bulk(request, RequestOptions.DEFAULT);
            System.out.println();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Map<String, Object> getESConfigMap(String path) throws IOException {
        FileInputStream fileInputStream = new FileInputStream(path);
        String data = IOUtils.toString(fileInputStream);
        return (Map<String, Object>) JSON.parse(data);
    }
}
