package com.ctyun.devops.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.ctyun.devops.enums.OperatorTypeEnum;
import com.ctyun.devops.model.TargetObject;
import com.ctyun.devops.model.index.Department;
import com.ctyun.devops.model.index.Employee;
import com.ctyun.devops.model.index.Product;
import com.ctyun.devops.repository.DepartmentRepository;
import com.ctyun.devops.repository.EmployeeRepository;
import com.ctyun.devops.repository.ProductRepository;
import org.elasticsearch.index.query.QueryBuilder;
import org.omg.PortableServer.THREAD_POLICY_ID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.RefreshPolicy;
import org.springframework.data.elasticsearch.core.ScriptType;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.*;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

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
            Employee reqEmployee = employees.get(0);
            long employeeId = reqEmployee.getId();

            // ?????? employee??????
            Employee targetEmployee = employeeRepository.findById(employeeId).orElseThrow(() -> new RuntimeException("error."));
            if (StringUtils.hasText(reqEmployee.getName())) {
                targetEmployee.setName(reqEmployee.getName());
            }
            if (StringUtils.hasText(reqEmployee.getDescribe())) {
                targetEmployee.setDescribe(reqEmployee.getDescribe());
            }
            this.updateTargetDocument(String.valueOf(targetEmployee.getId()), EMPLOYEE_INDEX_NAME, targetEmployee);

            // ???????????????????????? employee ??? department???????????????
//            List<Department> departments = departmentRepository.findDepartmentsByEmployeeId(employeeId);
//            for (Department department : departments) {
//                this.updateEmployeeFromTargetIndex(String.valueOf(department.getId()), DEPARTMENT_INDEX_NAME, targetEmployee);
//            }
            this.updateEmployeeFromTargetIndex(DEPARTMENT_INDEX_NAME, targetEmployee);

            // ???????????????????????? employee ??? product???????????????
//            List<Product> products = productRepository.findDepartmentsByEmployeeId(employeeId);
//            for (Product product : products) {
//                this.updateEmployeeFromTargetIndex(String.valueOf(product.getId()), PRODUCT_INDEX_NAME, targetEmployee);
//            }
            this.updateEmployeeFromTargetIndex(PRODUCT_INDEX_NAME, targetEmployee);
        } else {
            for (Employee employee : employees) {
                long employeeId = employee.getId();
                // ?????? employee??????
                employeeRepository.deleteById(employeeId);

                // ???????????? department ??????????????????????????? employee ?????????
//                List<Department> departments = departmentRepository.findDepartmentsByEmployeeId(employeeId);
//                for (Department department : departments) {
//                    this.deleteEmployeeFromTargetIndex(String.valueOf(department.getId()), DEPARTMENT_INDEX_NAME, employeeId);
//                }
                this.deleteEmployeeFromTargetIndex(DEPARTMENT_INDEX_NAME, employeeId);

                // ???????????? product ??????????????????????????? employee ?????????
//                List<Product> products = productRepository.findDepartmentsByEmployeeId(employeeId);
//                for (Product product : products) {
//                    this.deleteEmployeeFromTargetIndex(String.valueOf(product.getId()), PRODUCT_INDEX_NAME, employeeId);
//                }
                this.deleteEmployeeFromTargetIndex(PRODUCT_INDEX_NAME, employeeId);
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
                // ?????? employee ??? department ?????????
                this.addEmployeeAndDepartmentRelationship(department, data.getJSONArray("addEmployees"));
            }
        } else if (OperatorTypeEnum.UPDATE.equals(operator)) {
            if (!ObjectUtils.isEmpty(data.getJSONObject("department"))) {
                // ?????? department ??????
                Department targetDepartment = departmentRepository.findById(departmentId).orElseThrow(() -> new RuntimeException("error."));

                JSONObject reqDepartment = data.getJSONObject("department");
                if (StringUtils.hasText(reqDepartment.getString("name"))) {
                    targetDepartment.setName(reqDepartment.getString("name"));
                }
                if (StringUtils.hasText(reqDepartment.getString("describe"))) {
                    targetDepartment.setDescribe(reqDepartment.getString("describe"));
                }
                this.updateTargetDocument(String.valueOf(targetDepartment.getId()), DEPARTMENT_INDEX_NAME, targetDepartment);

                // ???????????????????????? department ??? employee???????????????
//                List<Employee> employees = employeeRepository.findEmployeesByDepartmentId(departmentId);
//                for (Employee employee : employees) {
//                    this.updateDepartmentFromTargetIndex(String.valueOf(employee.getId()), EMPLOYEE_INDEX_NAME, targetDepartment);
//                }
                this.updateDepartmentFromTargetIndex(EMPLOYEE_INDEX_NAME, targetDepartment);

                // ???????????????????????? department ??? product???????????????
//                List<Product> products = productRepository.findDepartmentsByDepartmentId(departmentId);
//                for (Product product : products) {
//                    this.updateDepartmentFromTargetIndex(String.valueOf(product.getId()), PRODUCT_INDEX_NAME, targetDepartment);
//                }
                this.updateDepartmentFromTargetIndex(PRODUCT_INDEX_NAME, targetDepartment);
            }
            if (!CollectionUtils.isEmpty(data.getJSONArray("addEmployees"))) {
                Department department = departmentRepository.findById(departmentId).orElseThrow(() -> new RuntimeException("error."));
                // ?????? employees ??? department ?????????
                this.addEmployeeAndDepartmentRelationship(department, data.getJSONArray("addEmployees"));
            }
            if (!CollectionUtils.isEmpty(data.getJSONArray("deleteEmployees"))) {
                // ?????? employees ??? department ?????????
                this.deleteEmployeeAndDepartmentRelationship(String.valueOf(departmentId), data.getJSONArray("deleteEmployees"));
            }
        } else {
            // ????????????????????????????????????????????????????????? employees???products ?????????????????????????????????????????? employees ??? products???
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
                // ?????? employees ??? product ???
                this.addEmployeeAndProductRelationship(product, data.getJSONArray("addEmployees"));
            }
            if (!ObjectUtils.isEmpty(data.getJSONObject("belongDepartment"))) {
                // ?????? product ??? department ?????????
                this.addProductAndDepartmentRelationship(product, data.getJSONObject("belongDepartment"));
            }
        } else if (OperatorTypeEnum.UPDATE.equals(operator)) {
            if (!ObjectUtils.isEmpty(data.getJSONObject("product"))) {
                // ?????? product ??????
                Product targetProduct = productRepository.findById(productId).orElseThrow(() -> new RuntimeException("error."));

                JSONObject reqProduct = data.getJSONObject("product");
                if (StringUtils.hasText(reqProduct.getString("name"))) {
                    targetProduct.setName(reqProduct.getString("name"));
                }
                if (StringUtils.hasText(reqProduct.getString("describe"))) {
                    targetProduct.setDescribe(reqProduct.getString("describe"));
                }
                this.updateTargetDocument(String.valueOf(targetProduct.getId()), PRODUCT_INDEX_NAME, targetProduct);

                // ???????????????????????? product ??? employee???????????????
//                List<Employee> employees = employeeRepository.findEmployeesByProductId(productId);
//                for (Employee employee : employees) {
//                    this.updateProductFromTargetIndex(String.valueOf(employee.getId()), EMPLOYEE_INDEX_NAME, targetProduct);
//                }
                this.updateProductFromTargetIndex(EMPLOYEE_INDEX_NAME, targetProduct);
                // ???????????????????????? product ??? department???????????????
//                List<Department> departments = departmentRepository.findDepartmentsByProductId(productId);
//                for (Department department : departments) {
//                    this.updateProductFromTargetIndex(String.valueOf(department.getId()), DEPARTMENT_INDEX_NAME, targetProduct);
//                }
                this.updateProductFromTargetIndex(DEPARTMENT_INDEX_NAME, targetProduct);
            }
            if (!CollectionUtils.isEmpty(data.getJSONArray("addEmployees"))) {
                Product product = productRepository.findById(productId).orElseThrow(() -> new RuntimeException("error."));
                // ?????? employees ??? product ???
                this.addEmployeeAndProductRelationship(product, data.getJSONArray("addEmployees"));
            }
            if (!CollectionUtils.isEmpty(data.getJSONArray("deleteEmployees"))) {
                // ??? product ??? ?????? employees
                this.deleteEmployeeAndProductRelationship(String.valueOf(productId), data.getJSONArray("deleteEmployees"));
            }
            if (!ObjectUtils.isEmpty(data.getJSONObject("belongDepartment"))) {
                // ?????? product ??? ???department ?????????
                Department oldDepartment = departmentRepository.findDepartmentsByProductId(productId).get(0);
                this.deleteProductAndDepartmentRelationship(String.valueOf(productId), String.valueOf(oldDepartment.getId()));

                // ?????? product ??? ???department ?????????
                Product product = productRepository.findById(productId).orElseThrow(() -> new RuntimeException("error."));
                this.addProductAndDepartmentRelationship(product, data.getJSONObject("belongDepartment"));
            }
        } else {
            // ??????
            productRepository.deleteById(productId);

            // ???????????? department?????????????????????????????? product ?????????
//            List<Department> departments = departmentRepository.findDepartmentsByProductId(productId);
//            for (Department department : departments) {
//                this.deleteProductFromTargetIndex(String.valueOf(department.getId()), DEPARTMENT_INDEX_NAME, productId);
//            }
            this.deleteProductFromTargetIndex(DEPARTMENT_INDEX_NAME, productId);

            // ???????????? employee?????????????????????????????? product ?????????
//            List<Employee> employees = employeeRepository.findEmployeesByProductId(productId);
//            for (Employee employee : employees) {
//                this.deleteProductFromTargetIndex(String.valueOf(employee.getId()), EMPLOYEE_INDEX_NAME, productId);
//            }
            this.deleteProductFromTargetIndex(EMPLOYEE_INDEX_NAME, productId);
        }
    }

    private void addEmployeeAndDepartmentRelationship(Department department, JSONArray employeeJSONArray) {
        // ?????? employees ??? department ???
        this.addEmployeesToTargetIndex(String.valueOf(department.getId()), DEPARTMENT_INDEX_NAME, employeeJSONArray);

        // ?????? department ??? employees ?????????????????????????????????
        for (Object employee : employeeJSONArray) {
            this.addDepartmentToTargetIndex(((JSONObject) employee).getString("id"), EMPLOYEE_INDEX_NAME, department);
        }
    }

    private void deleteEmployeeAndDepartmentRelationship(String departmentId, JSONArray employeeJSONArray) {
        for (Object employee : employeeJSONArray) {
            String employeeId = ((JSONObject) employee).getString("id");
            // ?????? department ?????? employees
            this.deleteEmployeeFromTargetIndex(DEPARTMENT_INDEX_NAME, Long.parseLong(employeeId));
        }
        // ?????? employees ?????? department
        this.deleteDepartmentFromTargetIndex(EMPLOYEE_INDEX_NAME, Long.parseLong(departmentId));
    }

    private void addEmployeeAndProductRelationship(Product product, JSONArray employeeJSONArray) {
        // ?????? employees ??? product ???
        this.addEmployeesToTargetIndex(String.valueOf(product.getId()), PRODUCT_INDEX_NAME, employeeJSONArray);

        // ?????? product ??? employees ?????????????????????????????????
        for (Object employee : employeeJSONArray) {
            this.addProductToTargetIndex(((JSONObject) employee).getString("id"), EMPLOYEE_INDEX_NAME, product);
        }
    }

    private void deleteEmployeeAndProductRelationship(String productId, JSONArray employeeJSONArray) {
        for (Object employee : employeeJSONArray) {
            String employeeId = ((JSONObject) employee).getString("id");
            // ?????? product ?????? employees
            this.deleteEmployeeFromTargetIndex(PRODUCT_INDEX_NAME, Long.parseLong(employeeId));
        }
        // ?????? employees ?????? product
        this.deleteProductFromTargetIndex(EMPLOYEE_INDEX_NAME, Long.parseLong(productId));
    }

    private void addProductAndDepartmentRelationship(Product product, Object department) {
        // ?????? product ??? department ???
        this.addProductToTargetIndex(((JSONObject) department).getString("id"), DEPARTMENT_INDEX_NAME, product);
        // ?????? department ??? product ???
        this.addDepartmentToTargetIndex(String.valueOf(product.getId()), PRODUCT_INDEX_NAME, department);
    }

    private void deleteProductAndDepartmentRelationship(String productId, String departmentId) {
        // ?????? product ?????? department
        this.deleteDepartmentFromTargetIndex(PRODUCT_INDEX_NAME, Long.parseLong(departmentId));
        // ?????? department ?????? product
        this.deleteProductFromTargetIndex(DEPARTMENT_INDEX_NAME, Long.parseLong(productId));
    }

    private void updateTargetDocument(String targetDocumentId, String targetIndexName, Object targetObject) {
        Map<String, Object> params = JSON.parseObject(JSON.toJSONString(targetObject), new TypeReference<Map<String, Object>>() {
        });

        UpdateQuery updateQuery = UpdateQuery.builder(String.valueOf(targetDocumentId))
                .withScript(
                        "if (ctx._source.id == params.id) { ctx._source.name = params.name;ctx._source.describe = params.describe }")
                .withParams(params)
                .build();
        UpdateResponse response = esRestTemplate.update(updateQuery, IndexCoordinates.of(targetIndexName));
        System.out.println(response.getResult());
    }

    private void addEmployeesToTargetIndex(String targetDocumentId, String targetIndexName, JSONArray employeeJSONArray) {
        Map<String, Object> params = new HashMap<>();
        params.put("employees", employeeJSONArray);

        UpdateQuery updateQuery = UpdateQuery.builder(targetDocumentId)
                .withScript(
                        "if (ctx._source.employees != null) { ctx._source.employees.addAll(params.employees)} else {ctx._source.employees = [];ctx._source.employees.addAll(params.employees)}")
                .withParams(params)
                .build();
        UpdateResponse response = esRestTemplate.update(updateQuery, IndexCoordinates.of(targetIndexName));
        System.out.println(response.getResult());
    }

    private void deleteEmployeeFromTargetIndex(String targetIndexName, long employeeId) {
        Map<String, Object> params = new HashMap<>();
        params.put("id", employeeId);

        NativeSearchQuery query = new NativeSearchQueryBuilder().withQuery(matchAllQuery()).build();
        UpdateQuery updateQuery = UpdateQuery.builder(query)
                .withScriptType(ScriptType.INLINE)
                .withRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .withAbortOnVersionConflict(false)
                .withRetryOnConflict(20)
                .withLang("painless")
                .withScript(
                        "if (ctx._source.employees != null) { ctx._source.employees.removeIf(it -> it.id == params.id) }")
                .withParams(params)
                .build();
        ByQueryResponse response = esRestTemplate.updateByQuery(updateQuery, IndexCoordinates.of(targetIndexName));
        System.out.println("delete employee from: " + targetIndexName + ", total: " + response.getTotal() + ", conflicts: " + response.getVersionConflicts() + ", failed: " + response.getFailures());
    }

    private void updateEmployeeFromTargetIndex(String targetIndexName, Employee employee) {
        Map<String, Object> params = JSON.parseObject(JSON.toJSONString(employee), new TypeReference<Map<String, Object>>() {
        });

        NativeSearchQuery query = new NativeSearchQueryBuilder().withQuery(matchAllQuery()).build();
        UpdateQuery updateQuery = UpdateQuery.builder(query)
                .withScriptType(ScriptType.INLINE)
                .withRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .withRetryOnConflict(20)
                .withAbortOnVersionConflict(false)
                .withLang("painless")
                .withScript("if (ctx._source.employees != null) {for (item in ctx._source.employees) {if (item.id == params.id) {item.name = params.name;item.describe= params.describe;}}}")
                .withParams(params)
                .build();
        ByQueryResponse response = esRestTemplate.updateByQuery(updateQuery, IndexCoordinates.of(targetIndexName));
        System.out.println("update employee from: " + targetIndexName + ", total: " + response.getTotal() + ", conflicts: " + response.getVersionConflicts() + ", failed: " + response.getFailures());
    }


//    private void deleteEmployeeFromTargetIndex(String targetDocumentId, String targetIndexName, long employeeId) {
//        Map<String, Object> params = new HashMap<>();
//        params.put("id", employeeId);
//
//        UpdateQuery updateQuery = UpdateQuery.builder(targetDocumentId)
//                .withScript(
//                        "if (ctx._source.employees != null) { ctx._source.employees.removeIf(it -> it.id == params.id) }")
//                .withParams(params)
//                .build();
//        esRestTemplate.update(updateQuery, IndexCoordinates.of(targetIndexName));
//    }
//
//    private void updateEmployeeFromTargetIndex(String targetDocumentId, String targetIndexName, Employee employee) {
//        Map<String, Object> params = JSON.parseObject(JSON.toJSONString(employee), new TypeReference<Map<String, Object>>() {
//        });
//        UpdateQuery updateQuery = UpdateQuery.builder(targetDocumentId)
//                .withScript(
//                        "if (ctx._source.employees != null) {for (item in ctx._source.employees) {if (item.id == params.id) {item.name = params.name;item.describe= params.describe;}}}")
//                .withParams(params)
//                .build();
//        esRestTemplate.update(updateQuery, IndexCoordinates.of(targetIndexName));
//    }


    private void addDepartmentToTargetIndex(String targetDocumentId, String targetIndexName, Object department) {
        Map<String, Object> params = new HashMap<>();
        params.put("departments", Collections.singletonList(JSONObject.toJSON(department)));

        UpdateQuery updateQuery = UpdateQuery.builder(targetDocumentId)
                .withScript(
                        "if (ctx._source.departments != null) { ctx._source.departments.addAll(params.departments)} else {ctx._source.departments = [];ctx._source.departments.addAll(params.departments)}")
                .withParams(params)
                .build();
        UpdateResponse response = esRestTemplate.update(updateQuery, IndexCoordinates.of(targetIndexName));
        System.out.println(response.getResult());
    }


    private void deleteDepartmentFromTargetIndex(String targetIndexName, long departmentId) {
        Map<String, Object> params = new HashMap<>();
        params.put("id", departmentId);

        NativeSearchQuery query = new NativeSearchQueryBuilder().withQuery(matchAllQuery()).build();
        UpdateQuery updateQuery = UpdateQuery.builder(query)
                .withScriptType(ScriptType.INLINE)
                .withRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .withRetryOnConflict(20)
                .withAbortOnVersionConflict(false)
                .withLang("painless")
                .withScript(
                        "if (ctx._source.departments != null) { ctx._source.departments.removeIf(it -> it.id == params.id) }")
                .withParams(params)
                .build();
        ByQueryResponse response = esRestTemplate.updateByQuery(updateQuery, IndexCoordinates.of(targetIndexName));
        System.out.println("delete department from: " + targetIndexName + ", total: " + response.getTotal() + ", conflicts: " + response.getVersionConflicts() + ", failed: " + response.getFailures());
    }

    private void updateDepartmentFromTargetIndex(String targetIndexName, Department department) {
        Map<String, Object> params = JSON.parseObject(JSON.toJSONString(department), new TypeReference<Map<String, Object>>() {
        });

        NativeSearchQuery query = new NativeSearchQueryBuilder().withQuery(matchAllQuery()).build();
        UpdateQuery updateQuery = UpdateQuery.builder(query)
                .withScriptType(ScriptType.INLINE)
                .withRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .withRetryOnConflict(20)
                .withAbortOnVersionConflict(false)
                .withLang("painless")
                .withScript("if (ctx._source.departments != null) {for (item in ctx._source.departments) {if (item.id == params.id) {item.name = params.name;item.describe= params.describe;}}}")
                .withParams(params)
                .build();
        ByQueryResponse response = esRestTemplate.updateByQuery(updateQuery, IndexCoordinates.of(targetIndexName));
        System.out.println("update department from: " + targetIndexName + ", total: " + response.getTotal() + ", conflicts: " + response.getVersionConflicts() + ", failed: " + response.getFailures());
    }

//    private void deleteDepartmentFromTargetIndex(String targetDocumentId, String targetIndexName, long departmentId) {
//        Map<String, Object> params = new HashMap<>();
//        params.put("id", departmentId);
//
//        UpdateQuery updateQuery = UpdateQuery.builder(targetDocumentId)
//                .withScript(
//                        "if (ctx._source.departments != null) { ctx._source.departments.removeIf(it -> it.id == params.id) }")
//                .withParams(params)
//                .build();
//        esRestTemplate.update(updateQuery, IndexCoordinates.of(targetIndexName));
//    }
//
//    private void updateDepartmentFromTargetIndex(String targetDocumentId, String targetIndexName, Department department) {
//        Map<String, Object> params = JSON.parseObject(JSON.toJSONString(department), new TypeReference<Map<String, Object>>() {
//        });
//        UpdateQuery updateQuery = UpdateQuery.builder(targetDocumentId)
//                .withScript(
//                        "if (ctx._source.departments != null) {for (item in ctx._source.departments) {if (item.id == params.id) {item.name = params.name;item.describe= params.describe;}}}")
//                .withParams(params)
//                .build();
//        esRestTemplate.update(updateQuery, IndexCoordinates.of(targetIndexName));
//    }


    private void addProductToTargetIndex(String targetDocumentId, String targetIndexName, Object product) {
        Map<String, Object> params = new HashMap<>();
        params.put("products", Collections.singletonList(JSONObject.toJSON(product)));

        UpdateQuery updateQuery = UpdateQuery.builder(targetDocumentId)
                .withScript(
                        "if (ctx._source.products != null) { ctx._source.products.addAll(params.products)} else {ctx._source.products = [];ctx._source.products.addAll(params.products)}")
                .withParams(params)
                .build();
        UpdateResponse response = esRestTemplate.update(updateQuery, IndexCoordinates.of(targetIndexName));
        System.out.println(response.getResult());
    }

    private void deleteProductFromTargetIndex(String targetIndexName, long productId) {
        Map<String, Object> params = new HashMap<>();
        params.put("id", productId);

        NativeSearchQuery query = new NativeSearchQueryBuilder().withQuery(matchAllQuery()).build();
        UpdateQuery updateQuery = UpdateQuery.builder(query)
                .withScriptType(ScriptType.INLINE)
                .withRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .withRetryOnConflict(20)
                .withAbortOnVersionConflict(false)
                .withLang("painless")
                .withScript(
                        "if (ctx._source.products != null) { ctx._source.products.removeIf(it -> it.id == params.id) }")
                .withParams(params)
                .build();
        ByQueryResponse response = esRestTemplate.updateByQuery(updateQuery, IndexCoordinates.of(targetIndexName));
        System.out.println("delete department from: " + targetIndexName + ", total: " + response.getTotal() + ", conflicts: " + response.getVersionConflicts() + ", failed: " + response.getFailures());
    }

    private void updateProductFromTargetIndex(String targetIndexName, Product product) {
        Map<String, Object> params = JSON.parseObject(JSON.toJSONString(product), new TypeReference<Map<String, Object>>() {
        });

        NativeSearchQuery query = new NativeSearchQueryBuilder().withQuery(matchAllQuery()).build();
        UpdateQuery updateQuery = UpdateQuery.builder(query)
                .withScriptType(ScriptType.INLINE)
                .withRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .withRetryOnConflict(20)
                .withAbortOnVersionConflict(false)
                .withLang("painless")
                .withScript("if (ctx._source.products != null) {for (item in ctx._source.products) {if (item.id == params.id) {item.name = params.name;item.describe= params.describe;}}}")
                .withParams(params)
                .build();
        ByQueryResponse response = esRestTemplate.updateByQuery(updateQuery, IndexCoordinates.of(targetIndexName));
        System.out.println("update product from: " + targetIndexName + ", total: " + response.getTotal() + ", conflicts: " + response.getVersionConflicts() + ", failed: " + response.getFailures());
    }

//    private void deleteProductFromTargetIndex(String targetDocumentId, String targetIndexName, long productId) {
//        Map<String, Object> params = new HashMap<>();
//        params.put("id", productId);
//
//        UpdateQuery updateQuery = UpdateQuery.builder(targetDocumentId)
//                .withScript(
//                        "if (ctx._source.products != null) { ctx._source.products.removeIf(it -> it.id == params.id) }")
//                .withParams(params)
//                .build();
//        esRestTemplate.update(updateQuery, IndexCoordinates.of(targetIndexName));
//    }
//
//
//    private void updateProductFromTargetIndex(String targetDocumentId, String targetIndexName, Product product) {
//        Map<String, Object> params = JSON.parseObject(JSON.toJSONString(product), new TypeReference<Map<String, Object>>() {
//        });
//        UpdateQuery updateQuery = UpdateQuery.builder(targetDocumentId)
//                .withScript(
//                        "if (ctx._source.products != null) {for (item in ctx._source.products) {if (item.id == params.id) {item.name = params.name;item.describe= params.describe;}}}")
//                .withParams(params)
//                .build();
//        esRestTemplate.update(updateQuery, IndexCoordinates.of(targetIndexName));
//    }

}
