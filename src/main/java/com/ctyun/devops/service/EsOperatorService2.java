package com.ctyun.devops.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.ctyun.devops.enums.OperatorTypeEnum;
import com.ctyun.devops.model.CommonObject;
import com.ctyun.devops.model.TargetObject;
import com.ctyun.devops.model.index.Department;
import com.ctyun.devops.model.index.Employee;
import com.ctyun.devops.model.index.Product;
import com.ctyun.devops.repository.DepartmentRepository;
import com.ctyun.devops.repository.EmployeeRepository;
import com.ctyun.devops.repository.ProductRepository;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.RefreshPolicy;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.*;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.*;


/**
 * @author bgy
 * @date 2022/8/13 16:37
 */
@Service
public class EsOperatorService2 {
    @Autowired
    private EmployeeRepository employeeRepository;
    @Autowired
    private DepartmentRepository departmentRepository;
    @Autowired
    private ProductRepository productRepository;

    @Qualifier("elasticsearchRestTemplate")
    @Autowired
    private ElasticsearchRestTemplate esRestTemplate;

    @Qualifier("restHighLevelClient")
    @Autowired
    private RestHighLevelClient highLevelClient;

    private static final String ADD_EMPLOYEES_FIELD = "addEmployees";
    private static final String DELETE_EMPLOYEES_FIELD = "deleteEmployees";
    private static final String BELONG_DEPARTMENT_FIELD = "belongDepartment";

    private static final String ID_FIELD = "id";
    private static final String NAME_FIELD = "name";
    private static final String DESCRIBE_FIELD = "describe";

    private static final String EMPLOYEE_FIELD = "employee";
    private static final String DEPARTMENT_FIELD = "department";
    private static final String PRODUCT_FIELD = "product";

    private static final String EMPLOYEES_FIELD = "employees";
    private static final String DEPARTMENTS_FIELD = "departments";
    private static final String PRODUCTS_FIELD = "products";

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

    public void queryByPrefix(String prefix) throws IOException {
        HighlightBuilder highlightBuilder = new HighlightBuilder()
                .field("name")
                .preTags("<span style=\"color:red\">")
                .postTags("</span>")
                .numOfFragments(1)
                .fragmentSize(50);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .fetchSource("name", null)
                .query(QueryBuilders.matchPhrasePrefixQuery("name", prefix))
                .highlighter(highlightBuilder);

        SearchHits searchHits = highLevelClient.search(new SearchRequest()
                .indices(EMPLOYEE_FIELD, DEPARTMENT_FIELD, PRODUCT_FIELD)
                .source(searchSourceBuilder), RequestOptions.DEFAULT).getHits();

        List<JSONObject> result = new ArrayList<>();
        if (searchHits.getHits().length > 0) {
            for (SearchHit searchHit : searchHits.getHits()) {
                JSONObject jsonObject = JSONObject.parseObject(searchHit.getSourceAsString());
                jsonObject.put("highlight", searchHit.getHighlightFields().get("name").getFragments()[0].toString());
                jsonObject.remove("_class");
                result.add(jsonObject);
            }
        }

        for (JSONObject jsonObject : result) {
            System.out.println("{");
        }

        System.out.println();
    }

//    public void queryByPrefix(String prefix) {
//        IndexBoost employeeIndex = new IndexBoost(EMPLOYEE_FIELD, 2);
//        IndexBoost departmentIndex = new IndexBoost(DEPARTMENT_FIELD, 1);
//        IndexBoost productIndex = new IndexBoost(PRODUCT_FIELD, 1);
//
////        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(prefix, "name", "describe", "employees.name", "employees.describe", "dept.name", "dept.describe", "product.name", "product.describe");
////        NativeSearchQuery searchQuery = new NativeSearchQueryBuilder()
////                .withIndicesBoost(employeeIndex, departmentIndex, productIndex)
////                .withQuery(multiMatchQueryBuilder)
////                .withPageable(PageRequest.of(0, 10))
////                .build();
////
////        SearchHits searchHits = esRestTemplate.search(searchQuery, Employee.class);
////        System.out.println();
//
//        NativeSearchQuery searchQuery = new NativeSearchQueryBuilder()
//                .withIndicesBoost(employeeIndex, departmentIndex, productIndex)
//                .withQuery(QueryBuilders.prefixQuery("name", prefix))
//                .withHighlightFields(new HighlightBuilder.Field("name").preTags("<span style=\"color:red\">").postTags("</span>").fragmentOffset(50).numOfFragments(1))
//                .build();
//        SearchHits searchHits = esRestTemplate.search(searchQuery, JSONObject.class);
//        System.out.println();
//
//        RestHighLevelClient restHighLevelClient = new RestHighLevelClient();
//        SearchResponse response = restHighLevelClient.search(searchQuery, RequestOptions.DEFAULT)
//    }

    public void queryByFullText(String text) {

    }

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
        JSONArray jsonArray = targetObject.getData().getJSONArray(EMPLOYEES_FIELD);
        List<Employee> employees = jsonArray.toJavaList(Employee.class);

        OperatorTypeEnum operator = targetObject.getOperator();
        if (OperatorTypeEnum.INSERT.equals(operator)) {
            employeeRepository.saveAll(employees);
        } else if (OperatorTypeEnum.UPDATE.equals(operator)) {
            Employee reqEmployee = employees.get(0);
            long employeeId = reqEmployee.getId();

            // 编辑 employee本身
            Employee targetEmployee = employeeRepository.findById(employeeId).orElseThrow(() -> new RuntimeException("error."));
            if (StringUtils.hasText(reqEmployee.getName())) {
                targetEmployee.setName(reqEmployee.getName());
            }
            if (StringUtils.hasText(reqEmployee.getDescribe())) {
                targetEmployee.setDescribe(reqEmployee.getDescribe());
            }
            this.updateTargetDocument(String.valueOf(targetEmployee.getId()), EMPLOYEE_FIELD, targetEmployee);

            // 编辑所有包含这个 employee 的 department文档的数据
            List<Department> departments = departmentRepository.findDepartmentsByEmployeeId(employeeId);
            for (Department department : departments) {
                this.updateEmployeeFromTargetIndex(String.valueOf(department.getId()), DEPARTMENT_FIELD, targetEmployee);
            }

            // 编辑所有包含这个 employee 的 product文档的数据
            List<Product> products = productRepository.findDepartmentsByEmployeeId(employeeId);
            for (Product product : products) {
                this.updateEmployeeFromTargetIndex(String.valueOf(product.getId()), PRODUCT_FIELD, targetEmployee);
            }
        } else {
            for (Employee employee : employees) {
                long employeeId = employee.getId();
                // 删除 employee本身
                employeeRepository.deleteById(employeeId);

                // 从所有的 department 文档中删除包含这个 employee 的数据
                List<Department> departments = departmentRepository.findDepartmentsByEmployeeId(employeeId);
                for (Department department : departments) {
                    this.deleteEmployeeFromTargetIndex(String.valueOf(department.getId()), DEPARTMENT_FIELD, employeeId);
                }

                // 从所有的 product 文档中删除包含这个 employee 的数据
                List<Product> products = productRepository.findDepartmentsByEmployeeId(employeeId);
                for (Product product : products) {
                    this.deleteEmployeeFromTargetIndex(String.valueOf(product.getId()), PRODUCT_FIELD, employeeId);
                }
            }
        }
    }

    private void processDepartmentOperator(TargetObject targetObject) {
        JSONObject data = targetObject.getData();
        long departmentId = data.getLong(ID_FIELD);

        OperatorTypeEnum operator = targetObject.getOperator();
        if (OperatorTypeEnum.INSERT.equals(operator)) {
            Department department = data.getJSONObject(DEPARTMENT_FIELD).toJavaObject(Department.class);
            department.setId(departmentId);
            departmentRepository.save(department);

            if (!CollectionUtils.isEmpty(data.getJSONArray(ADD_EMPLOYEES_FIELD))) {
                // 插入 employee 和 department 的关系
                this.addEmployeeAndDepartmentRelationship(department, data.getJSONArray(ADD_EMPLOYEES_FIELD));
            }
        } else if (OperatorTypeEnum.UPDATE.equals(operator)) {
            if (!ObjectUtils.isEmpty(data.getJSONObject(DEPARTMENT_FIELD))) {
                // 编辑 department 本身
                Department targetDepartment = departmentRepository.findById(departmentId).orElseThrow(() -> new RuntimeException("error."));

                JSONObject reqDepartment = data.getJSONObject(DEPARTMENT_FIELD);
                if (StringUtils.hasText(reqDepartment.getString(NAME_FIELD))) {
                    targetDepartment.setName(reqDepartment.getString(NAME_FIELD));
                }
                if (StringUtils.hasText(reqDepartment.getString(DESCRIBE_FIELD))) {
                    targetDepartment.setDescribe(reqDepartment.getString(DESCRIBE_FIELD));
                }
                this.updateTargetDocument(String.valueOf(targetDepartment.getId()), DEPARTMENT_FIELD, targetDepartment);

                // 编辑所有包含这个 department 的 employee文档的数据
                List<Employee> employees = employeeRepository.findEmployeesByDepartmentId(departmentId);
                for (Employee employee : employees) {
                    this.updateDepartmentFromTargetIndex(String.valueOf(employee.getId()), EMPLOYEE_FIELD, targetDepartment);
                }

                // 编辑所有包含这个 department 的 product文档的数据
                List<Product> products = productRepository.findDepartmentsByDepartmentId(departmentId);
                for (Product product : products) {
                    this.updateDepartmentFromTargetIndex(String.valueOf(product.getId()), PRODUCT_FIELD, targetDepartment);
                }
            }
            if (!CollectionUtils.isEmpty(data.getJSONArray(ADD_EMPLOYEES_FIELD))) {
                Department department = departmentRepository.findById(departmentId).orElseThrow(() -> new RuntimeException("error."));
                // 插入 employees 和 department 的关系
                this.addEmployeeAndDepartmentRelationship(department, data.getJSONArray(ADD_EMPLOYEES_FIELD));
            }
            if (!CollectionUtils.isEmpty(data.getJSONArray(DELETE_EMPLOYEES_FIELD))) {
                // 删除 employees 和 department 的关系
                this.deleteEmployeeAndDepartmentRelationship(String.valueOf(departmentId), data.getJSONArray(DELETE_EMPLOYEES_FIELD));
            }
        } else {
            // 删除（删除部门的前提是，属于这个部门的 employees、products 先要被删完，所以这里不用处理 employees 和 products）
            departmentRepository.deleteById(departmentId);
        }
    }

    private void processProductOperator(TargetObject targetObject) {
        JSONObject data = targetObject.getData();
        long productId = data.getLong(ID_FIELD);

        OperatorTypeEnum operator = targetObject.getOperator();
        if (OperatorTypeEnum.INSERT.equals(operator)) {
            Product product = data.getJSONObject(PRODUCT_FIELD).toJavaObject(Product.class);
            product.setId(productId);
            productRepository.save(product);

            if (!CollectionUtils.isEmpty(data.getJSONArray(ADD_EMPLOYEES_FIELD))) {
                // 插入 employees 到 product 中
                this.addEmployeeAndProductRelationship(product, data.getJSONArray(ADD_EMPLOYEES_FIELD));
            }
            if (!ObjectUtils.isEmpty(data.getJSONObject(BELONG_DEPARTMENT_FIELD))) {
                // 插入 product 与 department 的关系
                this.addProductAndDepartmentRelationship(product, data.getJSONObject(BELONG_DEPARTMENT_FIELD));
            }
        } else if (OperatorTypeEnum.UPDATE.equals(operator)) {
            if (!ObjectUtils.isEmpty(data.getJSONObject(PRODUCT_FIELD))) {
                // 编辑 product 本身
                Product targetProduct = productRepository.findById(productId).orElseThrow(() -> new RuntimeException("error."));

                JSONObject reqProduct = data.getJSONObject(PRODUCT_FIELD);
                if (StringUtils.hasText(reqProduct.getString(NAME_FIELD))) {
                    targetProduct.setName(reqProduct.getString(NAME_FIELD));
                }
                if (StringUtils.hasText(reqProduct.getString(DESCRIBE_FIELD))) {
                    targetProduct.setDescribe(reqProduct.getString(DESCRIBE_FIELD));
                }
                this.updateTargetDocument(String.valueOf(targetProduct.getId()), PRODUCT_FIELD, targetProduct);

                // 编辑所有包含这个 product 的 employee文档的数据
                List<Employee> employees = employeeRepository.findEmployeesByProductId(productId);
                for (Employee employee : employees) {
                    this.updateProductFromTargetIndex(String.valueOf(employee.getId()), EMPLOYEE_FIELD, targetProduct);
                }
                // 编辑所有包含这个 product 的 department文档的数据
                List<Department> departments = departmentRepository.findDepartmentsByProductId(productId);
                for (Department department : departments) {
                    this.updateProductFromTargetIndex(String.valueOf(department.getId()), DEPARTMENT_FIELD, targetProduct);
                }
            }
            if (!CollectionUtils.isEmpty(data.getJSONArray(ADD_EMPLOYEES_FIELD))) {
                Product product = productRepository.findById(productId).orElseThrow(() -> new RuntimeException("error."));
                // 插入 employees 和 product 的关系
                this.addEmployeeAndProductRelationship(product, data.getJSONArray(ADD_EMPLOYEES_FIELD));
            }
            if (!CollectionUtils.isEmpty(data.getJSONArray(DELETE_EMPLOYEES_FIELD))) {
                // 删除 product 和 employees 的关系
                this.deleteEmployeeAndProductRelationship(String.valueOf(productId), data.getJSONArray(DELETE_EMPLOYEES_FIELD));
            }
            if (!ObjectUtils.isEmpty(data.getJSONObject(BELONG_DEPARTMENT_FIELD))) {
                // 删除 product 与 旧department 的关系
                List<Department> oldDepartments = departmentRepository.findDepartmentsByProductId(productId);
                if (CollectionUtils.isEmpty(oldDepartments)) {
                    throw new RuntimeException("error.");
                }
                Department oldDepartment = oldDepartments.get(0);
                this.deleteProductAndDepartmentRelationship(String.valueOf(productId), String.valueOf(oldDepartment.getId()));

                // 插入 product 与 新department 的关系
                Product product = productRepository.findById(productId).orElseThrow(() -> new RuntimeException("error."));
                this.addProductAndDepartmentRelationship(product, data.getJSONObject(BELONG_DEPARTMENT_FIELD));
            }
        } else {
            // 删除
            productRepository.deleteById(productId);

            // 从所有的 department文档中，删除包含这个 product 的数据
            List<Department> departments = departmentRepository.findDepartmentsByProductId(productId);
            for (Department department : departments) {
                this.deleteProductFromTargetIndex(String.valueOf(department.getId()), DEPARTMENT_FIELD, productId);
            }

            // 从所有的 employee文档中，删除包含这个 product 的数据
            List<Employee> employees = employeeRepository.findEmployeesByProductId(productId);
            for (Employee employee : employees) {
                this.deleteProductFromTargetIndex(String.valueOf(employee.getId()), EMPLOYEE_FIELD, productId);
            }
        }
    }

    private void addEmployeeAndDepartmentRelationship(Department department, JSONArray employeeJSONArray) {
        // 添加 employees 到 department 中
        this.addEmployeesToTargetIndex(String.valueOf(department.getId()), DEPARTMENT_FIELD, employeeJSONArray);

        // 添加 department 到 employees 中，需要一个一个的添加
        for (Object employee : employeeJSONArray) {
            this.addDepartmentToTargetIndex(((JSONObject) employee).getString(ID_FIELD), EMPLOYEE_FIELD, department);
        }
    }

    private void deleteEmployeeAndDepartmentRelationship(String departmentId, JSONArray employeeJSONArray) {
        for (Object employee : employeeJSONArray) {
            String employeeId = ((JSONObject) employee).getString(ID_FIELD);
            // 删除 department 中的 employees
            this.deleteEmployeeFromTargetIndex(departmentId, DEPARTMENT_FIELD, Long.parseLong(employeeId));
            // 删除 employees 中的 department
            this.deleteDepartmentFromTargetIndex(employeeId, EMPLOYEE_FIELD, Long.parseLong(departmentId));
        }
    }

    private void addEmployeeAndProductRelationship(Product product, JSONArray employeeJSONArray) {
        // 添加 employees 到 product 中
        this.addEmployeesToTargetIndex(String.valueOf(product.getId()), PRODUCT_FIELD, employeeJSONArray);

        // 添加 product 到 employees 中，需要一个一个的添加
        for (Object employee : employeeJSONArray) {
            this.addProductToTargetIndex(((JSONObject) employee).getString(ID_FIELD), EMPLOYEE_FIELD, product);
        }
    }

    private void deleteEmployeeAndProductRelationship(String productId, JSONArray employeeJSONArray) {
        for (Object employee : employeeJSONArray) {
            String employeeId = ((JSONObject) employee).getString(ID_FIELD);
            // 删除 product 中的 employees
            this.deleteEmployeeFromTargetIndex(productId, PRODUCT_FIELD, Long.parseLong(employeeId));
            // 删除 employees 中的 product
            this.deleteProductFromTargetIndex(employeeId, EMPLOYEE_FIELD, Long.parseLong(productId));
        }
    }

    private void addProductAndDepartmentRelationship(Product product, Object department) {
        // 添加 product 到 department 中
        this.addProductToTargetIndex(((JSONObject) department).getString(ID_FIELD), DEPARTMENT_FIELD, product);
        // 添加 department 到 product 中
        this.addDepartmentToTargetIndex(String.valueOf(product.getId()), PRODUCT_FIELD, department);
    }

    private void deleteProductAndDepartmentRelationship(String productId, String departmentId) {
        // 删除 product 中的 department
        this.deleteDepartmentFromTargetIndex(productId, PRODUCT_FIELD, Long.parseLong(departmentId));
        // 删除 department 中的 product
        this.deleteProductFromTargetIndex(departmentId, DEPARTMENT_FIELD, Long.parseLong(productId));
    }

    private void updateTargetDocument(String targetDocumentId, String targetIndexName, Object targetObject) {
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

    private void addEmployeesToTargetIndex(String targetDocumentId, String targetIndexName, JSONArray employeeJSONArray) {
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

    private void deleteEmployeeFromTargetIndex(String targetDocumentId, String targetIndexName, long employeeId) {
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

    private void updateEmployeeFromTargetIndex(String targetDocumentId, String targetIndexName, Employee employee) {
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


    private void addDepartmentToTargetIndex(String targetDocumentId, String targetIndexName, Object department) {
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


    private void deleteDepartmentFromTargetIndex(String targetDocumentId, String targetIndexName, long departmentId) {
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

    private void updateDepartmentFromTargetIndex(String targetDocumentId, String targetIndexName, Department department) {
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


    private void addProductToTargetIndex(String targetDocumentId, String targetIndexName, Object product) {
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

    private void deleteProductFromTargetIndex(String targetDocumentId, String targetIndexName, long productId) {
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


    private void updateProductFromTargetIndex(String targetDocumentId, String targetIndexName, Product product) {
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
