package com.ctyun.devops.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ctyun.devops.enums.OperatorTypeEnum;
import com.ctyun.devops.model.TargetObject;
import com.ctyun.devops.model.index.Department;
import com.ctyun.devops.model.index.Employee;
import com.ctyun.devops.model.index.Product;
import com.ctyun.devops.repository.DepartmentRepository;
import com.ctyun.devops.repository.EmployeeRepository;
import com.ctyun.devops.repository.ProductRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.UpdateQuery;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author bgy
 * @date 2022/8/13 16:37
 */
@Service
public class EsOperatorService {
    @Autowired
    private EmployeeRepository employeeRepository;
    @Autowired
    private DepartmentRepository departmentRepository;
    @Autowired
    private ProductRepository productRepository;

    @Qualifier("elasticsearchRestTemplate")
    @Autowired
    private ElasticsearchRestTemplate esRestTemplate;

    private static final String EMPLOYEE_INDEX_NAME = "employee";
    private static final String DEPARTMENT_INDEX_NAME = "department";
    private static final String PRODUCT_INDEX_NAME = "product";

    public void processKafkaMessage(String message) {
        TargetObject targetObject = JSONObject.parseObject(message).toJavaObject(TargetObject.class);

        switch (targetObject.getTarget()) {
            case EMPLOYEE:
                this.processEmployeeOperator(targetObject);
                break;
            case DEPARTMENT:
                this.processDepartmentOperator(targetObject);
                break;
            case PRODUCT:
                this.processProductOperator(targetObject);
                break;
            default:
                break;
        }
    }

    private void processEmployeeOperator(TargetObject targetObject) {
        JSONArray jsonArray = targetObject.getData().getJSONArray("employees");
        List<Employee> employees = jsonArray.toJavaList(Employee.class);

        OperatorTypeEnum operator = targetObject.getOperator();
        if (OperatorTypeEnum.INSERT.equals(operator)) {
            employeeRepository.saveAll(employees);
        } else if (OperatorTypeEnum.UPDATE.equals(operator)) {
            Employee employee = employees.get(0);
            // 编辑
            employeeRepository.save(employee);

            // 编辑 department、product 索引的内容
        } else {
            for (Employee employee : employees) {
                // 删除
                employeeRepository.deleteById(employee.getId());

                // 从所有的 department 文档中删除包含这个 employee 的数据
                this.deleteEmployeeFromTargetIndex(null, DEPARTMENT_INDEX_NAME, String.valueOf(employee.getId()));
                // 从所有的 product 文档中删除包含这个 employee 的数据
                this.deleteEmployeeFromTargetIndex(null, PRODUCT_INDEX_NAME, String.valueOf(employee.getId()));
            }
        }
    }

    private void processDepartmentOperator(TargetObject targetObject) {
        JSONObject data = targetObject.getData();
        long departmentId = data.getLong("id");

        OperatorTypeEnum operator = targetObject.getOperator();
        if (OperatorTypeEnum.INSERT.equals(operator)) {
            Department department = data.getJSONObject("department").toJavaObject(Department.class);
            department.setId(departmentId);
            departmentRepository.save(department);

            if (!CollectionUtils.isEmpty(data.getJSONArray("addEmployees"))) {
                // 插入 employee 到 department 中
                this.addEmployeeAndDepartmentRelationship(department, data.getJSONArray("addEmployees"));
            }
        } else if (OperatorTypeEnum.UPDATE.equals(operator)) {
            if (!ObjectUtils.isEmpty(data.getJSONObject("department"))) {
                // 编辑 department 本身
                Department department = data.getJSONObject("department").toJavaObject(Department.class);
                department.setId(departmentId);
                departmentRepository.save(department);
            }
            if (!CollectionUtils.isEmpty(data.getJSONArray("addEmployees"))) {
                Department department = departmentRepository.findById(departmentId).orElseThrow(() -> new RuntimeException("error."));
                // 插入 employees 到 department 中
                this.addEmployeeAndDepartmentRelationship(department, data.getJSONArray("addEmployees"));
            }
            if (!CollectionUtils.isEmpty(data.getJSONArray("deleteEmployees"))) {
                // 从 department 中删除 employees
                this.deleteEmployeeAndDepartmentRelationship(String.valueOf(departmentId), data.getJSONArray("deleteEmployees"));
            }
        } else {
            // 删除（删除部门的前提是，属于这个部门的 employees、products 先要被删完，所以这里不用处理 employees 和 products）
            departmentRepository.deleteById(departmentId);
        }
    }

    private void processProductOperator(TargetObject targetObject) {
        JSONObject data = targetObject.getData();
        long productId = data.getLong("id");

        OperatorTypeEnum operator = targetObject.getOperator();
        if (OperatorTypeEnum.INSERT.equals(operator)) {
            Product product = data.getJSONObject("product").toJavaObject(Product.class);
            product.setId(productId);
            productRepository.save(product);

            if (!CollectionUtils.isEmpty(data.getJSONArray("addEmployees"))) {
                // 插入 employees 到 product 中
                this.addEmployeeAndProductRelationship(product, data.getJSONArray("addEmployees"));
            }
            if (!ObjectUtils.isEmpty(data.getJSONObject("belongDepartment"))) {
                // 插入 product 与 department 的关系
                this.addProductAndDepartmentRelationship(product, data.getJSONObject("belongDepartment"));
            }
        } else if (OperatorTypeEnum.UPDATE.equals(operator)) {
            if (!ObjectUtils.isEmpty(data.getJSONObject("product"))) {
                // 编辑 product 本身
                Product product = data.getJSONObject("product").toJavaObject(Product.class);
                product.setId(productId);
                productRepository.save(product);
            }
            if (!CollectionUtils.isEmpty(data.getJSONArray("addEmployees"))) {
                Product product = productRepository.findById(productId).orElseThrow(() -> new RuntimeException("error."));
                // 插入 employees 到 product 中
                this.addEmployeeAndProductRelationship(product, data.getJSONArray("addEmployees"));
            }
            if (!CollectionUtils.isEmpty(data.getJSONArray("deleteEmployees"))) {
                // 从 product 中 删除 employees
                this.deleteEmployeeAndProductRelationship(String.valueOf(productId), data.getJSONArray("deleteEmployees"));
            }
            if (!ObjectUtils.isEmpty(data.getJSONObject("belongDepartment"))) {
                // 编辑 product 与 department 的关系
            }
        } else {
            // 删除
            productRepository.deleteById(productId);

            // 从所有的 department 文档中删除包含这个 product 的数据
            this.deleteProductFromTargetIndex(null, DEPARTMENT_INDEX_NAME, String.valueOf(productId));

            // 从所有的 employee 文档中删除包含这个 product 的数据
            this.deleteProductFromTargetIndex(null, EMPLOYEE_INDEX_NAME, String.valueOf(productId));
        }
    }

    private void addEmployeeAndDepartmentRelationship(Object department, JSONArray employeeJSONArray) {
        // 添加 employees 到 department 中
        this.addEmployeesToTargetIndex(((JSONObject) department).getString("id"), DEPARTMENT_INDEX_NAME, employeeJSONArray);

        // 添加 department 到 employees 中，需要一个一个的添加
        for (Object employee : employeeJSONArray) {
            this.addDepartmentToTargetIndex(((JSONObject) employee).getString("id"), EMPLOYEE_INDEX_NAME, department);
        }
    }

    private void deleteEmployeeAndDepartmentRelationship(String departmentId, JSONArray employeeJSONArray) {
        for (Object employee : employeeJSONArray) {
            String employeeId = ((JSONObject) employee).getString("id");
            // 删除 department 中的 employees
            this.deleteEmployeeFromTargetIndex(departmentId, DEPARTMENT_INDEX_NAME, employeeId);
            // 删除 employees 中的 department
            this.deleteDepartmentFromTargetIndex(employeeId, EMPLOYEE_INDEX_NAME, departmentId);
        }
    }

    private void addEmployeeAndProductRelationship(Object product, JSONArray employeeJSONArray) {
        // 添加 employees 到 product 中
        this.addEmployeesToTargetIndex(((JSONObject) product).getString("id"), PRODUCT_INDEX_NAME, employeeJSONArray);

        // 添加 product 到 employees 中，需要一个一个的添加
        for (Object employee : employeeJSONArray) {
            this.addProductToTargetIndex(((JSONObject) employee).getString("id"), EMPLOYEE_INDEX_NAME, product);
        }
    }

    private void deleteEmployeeAndProductRelationship(String productId, JSONArray employeeJSONArray) {
        for (Object employee : employeeJSONArray) {
            String employeeId = ((JSONObject) employee).getString("id");
            // 删除 product 中的 employees
            this.deleteEmployeeFromTargetIndex(productId, PRODUCT_INDEX_NAME, employeeId);
            // 删除 employees 中的 product
            this.deleteProductFromTargetIndex(employeeId, EMPLOYEE_INDEX_NAME, productId);
        }
    }

    private void addProductAndDepartmentRelationship(Object product, Object department) {
        // 添加 product 到 department 中
        this.addProductToTargetIndex(((JSONObject) department).getString("id"), DEPARTMENT_INDEX_NAME, product);

        // 添加 department 到 product 中
        this.addDepartmentToTargetIndex(((JSONObject) product).getString("id"), PRODUCT_INDEX_NAME, department);
    }

    private void deleteProductAndDepartmentRelationship(String productId, String departmentId) {
        // 删除 product 中的 department
        this.deleteDepartmentFromTargetIndex(productId, PRODUCT_INDEX_NAME, departmentId);
        // 删除 department 中的 product
        this.deleteProductFromTargetIndex(departmentId, DEPARTMENT_INDEX_NAME, productId);
    }

    private void addEmployeesToTargetIndex(String targetDocumentId, String targetIndexName, JSONArray employeeJSONArray) {
        Map<String, Object> employeeParams = new HashMap<>();
        employeeParams.put("employees", employeeJSONArray);

        UpdateQuery updateQuery = UpdateQuery.builder(targetDocumentId)
                .withScript(
                        "if (ctx._source.employees != null) { ctx._source.employees.addAll(params.employees)} else {ctx._source.employees = [];ctx._source.employees.addAll(params.employees)}")
                .withParams(employeeParams)
                .build();

        esRestTemplate.update(updateQuery, IndexCoordinates.of(targetIndexName));
    }

    private void deleteEmployeeFromTargetIndex(String targetDocumentId, String targetIndexName, String employeeId) {
        Map<String, Object> employeeIdParams = new HashMap<>();
        employeeIdParams.put("id", employeeId);

        UpdateQuery updateQuery = UpdateQuery.builder(targetDocumentId)
                .withScript(
                        "if (ctx._source.employees != null) { int i = 0; for (;i<ctx._source.employees.length;i++) { if (ctx._source.employees[i].id==params.id) {break;}} ctx._source.employees.remove(i)}")
                .withParams(employeeIdParams)
                .build();
        esRestTemplate.update(updateQuery, IndexCoordinates.of(targetIndexName));
    }

    private void addDepartmentToTargetIndex(String targetDocumentId, String targetIndexName, Object department) {
        Map<String, Object> departmentParams = new HashMap<>();
        departmentParams.put("departments", Collections.singletonList(department));

        UpdateQuery updateQuery = UpdateQuery.builder(targetDocumentId)
                .withScript(
                        "if (ctx._source.departments != null) { ctx._source.departments.addAll(params.departments)} else {ctx._source.departments = [];ctx._source.departments.addAll(params.departments)}")
                .withParams(departmentParams)
                .build();
        esRestTemplate.update(updateQuery, IndexCoordinates.of(targetIndexName));
    }

    private void deleteDepartmentFromTargetIndex(String targetDocumentId, String targetIndexName, String departmentId) {
        Map<String, Object> employeeIdParams = new HashMap<>();
        employeeIdParams.put("id", departmentId);

        UpdateQuery updateQuery = UpdateQuery.builder(targetDocumentId)
                .withScript(
                        "if (ctx._source.departments != null) { int i = 0; for (;i<ctx._source.departments.length;i++) { if (ctx._source.departments[i].id==params.id) {break;}} ctx._source.departments.remove(i)}")
                .withParams(employeeIdParams)
                .build();
        esRestTemplate.update(updateQuery, IndexCoordinates.of(targetIndexName));
    }

    private void addProductToTargetIndex(String targetDocumentId, String targetIndexName, Object product) {
        Map<String, Object> productParams = new HashMap<>();
        productParams.put("products", Collections.singletonList(product));

        UpdateQuery updateQuery = UpdateQuery.builder(targetDocumentId)
                .withScript(
                        "if (ctx._source.products != null) { ctx._source.products.addAll(params.products)} else {ctx._source.products = [];ctx._source.products.addAll(params.products)}")
                .withParams(productParams)
                .build();
        esRestTemplate.update(updateQuery, IndexCoordinates.of(targetIndexName));
    }

    private void deleteProductFromTargetIndex(String targetDocumentId, String targetIndexName, String productId) {
        Map<String, Object> employeeIdParams = new HashMap<>();
        employeeIdParams.put("id", productId);

        UpdateQuery updateQuery = UpdateQuery.builder(targetDocumentId)
                .withScript(
                        "if (ctx._source.products != null) { int i = 0; for (;i<ctx._source.products.length;i++) { if (ctx._source.products[i].id==params.id) {break;}} ctx._source.products.remove(i)}")
                .withParams(employeeIdParams)
                .build();
        esRestTemplate.update(updateQuery, IndexCoordinates.of(targetIndexName));
    }
}
