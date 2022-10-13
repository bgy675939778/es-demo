package com.ctyun.devops.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.ctyun.devops.model.index.Department;
import com.ctyun.devops.model.index.Employee;
import com.ctyun.devops.model.index.Product;
import lombok.RequiredArgsConstructor;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.RefreshPolicy;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.UpdateQuery;
import org.springframework.data.elasticsearch.core.query.UpdateResponse;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.ctyun.devops.constants.ESConstants.*;

@Component
@RequiredArgsConstructor
public class ESIndexRelationshipManager {
    private final ElasticsearchRestTemplate esRestTemplate;

    private static final String UPDATE_DOCUMENT_SCRIPT = "if (ctx._source.id == params.id) { ctx._source.name = params.name;ctx._source.describe = params.describe;}";

    private static final String ADD_EMPLOYEE_SCRIPT = "if (ctx._source.employees != null) { ctx._source.employees.addAll(params.employees)} else {ctx._source.employees = [];ctx._source.employees.addAll(params.employees);}";
    private static final String DELETE_EMPLOYEE_SCRIPT = "if (ctx._source.employees != null) { ctx._source.employees.removeIf(it -> it.id == params.id);}";
    private static final String UPDATE_EMPLOYEE_SCRIPT = "if (ctx._source.employees != null) {for (item in ctx._source.employees) {if (item.id == params.id) {item.name = params.name;item.describe = params.describe;}}}";

    private static final String ADD_DEPARTMENT_SCRIPT = "if (ctx._source.departments != null) { ctx._source.departments.addAll(params.departments)} else {ctx._source.departments = [];ctx._source.departments.addAll(params.departments);}";
    private static final String DELETE_DEPARTMENT_SCRIPT = "if (ctx._source.departments != null) { ctx._source.departments.removeIf(it -> it.id == params.id);}";
    private static final String UPDATE_DEPARTMENT_SCRIPT = "if (ctx._source.departments != null) {for (item in ctx._source.departments) {if (item.id == params.id) {item.name = params.name;item.describe = params.describe;}}}";

    private static final String ADD_PRODUCT_SCRIPT = "if (ctx._source.products != null) { ctx._source.products.addAll(params.products)} else {ctx._source.products = [];ctx._source.products.addAll(params.products);}";
    private static final String DELETE_PRODUCT_SCRIPT = "if (ctx._source.products != null) { ctx._source.products.removeIf(it -> it.id == params.id);}";
    private static final String UPDATE_PRODUCT_SCRIPT = "if (ctx._source.products != null) {for (item in ctx._source.products) {if (item.id == params.id) {item.name = params.name;item.describe = params.describe;}}}";

    public void addEmployeeAndDepartmentRelationship(Department department, JSONArray employeeJSONArray) {
        // 添加 employees 到 department 中
        this.addEmployeesToTargetIndex(String.valueOf(department.getId()), DEPARTMENT_FIELD, employeeJSONArray);

        // 添加 department 到 employees 中，需要一个一个的添加
        for (Object employee : employeeJSONArray) {
            this.addDepartmentToTargetIndex(((JSONObject) employee).getString(ID_FIELD), EMPLOYEE_FIELD, department);
        }
    }

    public void deleteEmployeeAndDepartmentRelationship(String departmentId, JSONArray employeeJSONArray) {
        for (Object employee : employeeJSONArray) {
            String employeeId = ((JSONObject) employee).getString(ID_FIELD);
            // 删除 department 中的 employees
            this.deleteEmployeeFromTargetIndex(departmentId, DEPARTMENT_FIELD, Long.parseLong(employeeId));
            // 删除 employees 中的 department
            this.deleteDepartmentFromTargetIndex(employeeId, EMPLOYEE_FIELD, Long.parseLong(departmentId));
        }
    }

    public void addEmployeeAndProductRelationship(Product product, JSONArray employeeJSONArray) {
        // 添加 employees 到 product 中
        this.addEmployeesToTargetIndex(String.valueOf(product.getId()), PRODUCT_FIELD, employeeJSONArray);

        // 添加 product 到 employees 中，需要一个一个的添加
        for (Object employee : employeeJSONArray) {
            this.addProductToTargetIndex(((JSONObject) employee).getString(ID_FIELD), EMPLOYEE_FIELD, product);
        }
    }

    public void deleteEmployeeAndProductRelationship(String productId, JSONArray employeeJSONArray) {
        for (Object employee : employeeJSONArray) {
            String employeeId = ((JSONObject) employee).getString(ID_FIELD);
            // 删除 product 中的 employees
            this.deleteEmployeeFromTargetIndex(productId, PRODUCT_FIELD, Long.parseLong(employeeId));
            // 删除 employees 中的 product
            this.deleteProductFromTargetIndex(employeeId, EMPLOYEE_FIELD, Long.parseLong(productId));
        }
    }

    public void addProductAndDepartmentRelationship(Product product, Object department) {
        // 添加 product 到 department 中
        this.addProductToTargetIndex(((JSONObject) department).getString(ID_FIELD), DEPARTMENT_FIELD, product);
        // 添加 department 到 product 中
        this.addDepartmentToTargetIndex(String.valueOf(product.getId()), PRODUCT_FIELD, department);
    }

    public void deleteProductAndDepartmentRelationship(String productId, String departmentId) {
        // 删除 product 中的 department
        this.deleteDepartmentFromTargetIndex(productId, PRODUCT_FIELD, Long.parseLong(departmentId));
        // 删除 department 中的 product
        this.deleteProductFromTargetIndex(departmentId, DEPARTMENT_FIELD, Long.parseLong(productId));
    }

    public void updateTargetDocument(String targetDocumentId, String targetIndexName, Object targetObject) {
        Map<String, Object> params = JSON.parseObject(JSON.toJSONString(targetObject), new TypeReference<Map<String, Object>>() {
        });

        UpdateQuery updateQuery = UpdateQuery.builder(String.valueOf(targetDocumentId))
                .withRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .withAbortOnVersionConflict(false)
                .withRetryOnConflict(10)
                .withScript(UPDATE_DOCUMENT_SCRIPT)
                .withParams(params)
                .build();
        UpdateResponse response = esRestTemplate.update(updateQuery, IndexCoordinates.of(targetIndexName));
        System.out.println(response.getResult());
    }

    public void addEmployeesToTargetIndex(String targetDocumentId, String targetIndexName, JSONArray employeeJSONArray) {
        Map<String, Object> params = new HashMap<>();
        params.put(EMPLOYEES_FIELD, employeeJSONArray);

        UpdateQuery updateQuery = UpdateQuery.builder(targetDocumentId)
                .withRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .withAbortOnVersionConflict(false)
                .withRetryOnConflict(10)
                .withScript(ADD_EMPLOYEE_SCRIPT)
                .withParams(params)
                .build();
        UpdateResponse response = esRestTemplate.update(updateQuery, IndexCoordinates.of(targetIndexName));
        System.out.println(response.getResult());
    }

    public void deleteEmployeeFromTargetIndex(String targetDocumentId, String targetIndexName, long employeeId) {
        Map<String, Object> params = new HashMap<>();
        params.put(ID_FIELD, employeeId);

        UpdateQuery updateQuery = UpdateQuery.builder(targetDocumentId)
                .withRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .withAbortOnVersionConflict(false)
                .withRetryOnConflict(10)
                .withScript(DELETE_EMPLOYEE_SCRIPT)
                .withParams(params)
                .build();
        UpdateResponse response = esRestTemplate.update(updateQuery, IndexCoordinates.of(targetIndexName));
        System.out.println(response.getResult());
    }

    public void updateEmployeeFromTargetIndex(String targetDocumentId, String targetIndexName, Employee employee) {
        Map<String, Object> params = JSON.parseObject(JSON.toJSONString(employee), new TypeReference<Map<String, Object>>() {
        });
        UpdateQuery updateQuery = UpdateQuery.builder(targetDocumentId)
                .withRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .withAbortOnVersionConflict(false)
                .withRetryOnConflict(10)
                .withScript(UPDATE_EMPLOYEE_SCRIPT)
                .withParams(params)
                .build();
        UpdateResponse response = esRestTemplate.update(updateQuery, IndexCoordinates.of(targetIndexName));
        System.out.println(response.getResult());
    }


    public void addDepartmentToTargetIndex(String targetDocumentId, String targetIndexName, Object department) {
        Map<String, Object> params = new HashMap<>();
        params.put(DEPARTMENTS_FIELD, Collections.singletonList(JSONObject.toJSON(department)));

        UpdateQuery updateQuery = UpdateQuery.builder(targetDocumentId)
                .withRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .withAbortOnVersionConflict(false)
                .withRetryOnConflict(10)
                .withScript(ADD_DEPARTMENT_SCRIPT)
                .withParams(params)
                .build();
        UpdateResponse response = esRestTemplate.update(updateQuery, IndexCoordinates.of(targetIndexName));
        System.out.println(response.getResult());
    }


    public void deleteDepartmentFromTargetIndex(String targetDocumentId, String targetIndexName, long departmentId) {
        Map<String, Object> params = new HashMap<>();
        params.put(ID_FIELD, departmentId);

        UpdateQuery updateQuery = UpdateQuery.builder(targetDocumentId)
                .withRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .withAbortOnVersionConflict(false)
                .withRetryOnConflict(10)
                .withScript(DELETE_DEPARTMENT_SCRIPT)
                .withParams(params)
                .build();
        UpdateResponse response = esRestTemplate.update(updateQuery, IndexCoordinates.of(targetIndexName));
        System.out.println(response.getResult());
    }

    public void updateDepartmentFromTargetIndex(String targetDocumentId, String targetIndexName, Department department) {
        Map<String, Object> params = JSON.parseObject(JSON.toJSONString(department), new TypeReference<Map<String, Object>>() {
        });
        UpdateQuery updateQuery = UpdateQuery.builder(targetDocumentId)
                .withRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .withAbortOnVersionConflict(false)
                .withRetryOnConflict(10)
                .withScript(UPDATE_DEPARTMENT_SCRIPT)
                .withParams(params)
                .build();
        UpdateResponse response = esRestTemplate.update(updateQuery, IndexCoordinates.of(targetIndexName));
        System.out.println(response.getResult());
    }


    public void addProductToTargetIndex(String targetDocumentId, String targetIndexName, Object product) {
        Map<String, Object> params = new HashMap<>();
        params.put(PRODUCTS_FIELD, Collections.singletonList(JSONObject.toJSON(product)));

        UpdateQuery updateQuery = UpdateQuery.builder(targetDocumentId)
                .withRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .withAbortOnVersionConflict(false)
                .withRetryOnConflict(10)
                .withScript(ADD_PRODUCT_SCRIPT)
                .withParams(params)
                .build();
        UpdateResponse response = esRestTemplate.update(updateQuery, IndexCoordinates.of(targetIndexName));
        System.out.println(response.getResult());
    }

    public void deleteProductFromTargetIndex(String targetDocumentId, String targetIndexName, long productId) {
        Map<String, Object> params = new HashMap<>();
        params.put(ID_FIELD, productId);

        UpdateQuery updateQuery = UpdateQuery.builder(targetDocumentId)
                .withRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .withAbortOnVersionConflict(false)
                .withRetryOnConflict(10)
                .withScript(DELETE_PRODUCT_SCRIPT)
                .withParams(params)
                .build();
        UpdateResponse response = esRestTemplate.update(updateQuery, IndexCoordinates.of(targetIndexName));
        System.out.println(response.getResult());
    }


    public void updateProductFromTargetIndex(String targetDocumentId, String targetIndexName, Product product) {
        Map<String, Object> params = JSON.parseObject(JSON.toJSONString(product), new TypeReference<Map<String, Object>>() {
        });
        UpdateQuery updateQuery = UpdateQuery.builder(targetDocumentId)
                .withRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .withAbortOnVersionConflict(false)
                .withRetryOnConflict(10)
                .withScript(UPDATE_PRODUCT_SCRIPT)
                .withParams(params)
                .build();
        UpdateResponse response = esRestTemplate.update(updateQuery, IndexCoordinates.of(targetIndexName));
        System.out.println(response.getResult());
    }
}
