package com.ctyun.devops.service.strategy;

import com.alibaba.fastjson.JSONObject;
import com.ctyun.devops.enums.OperatorTypeEnum;
import com.ctyun.devops.model.TargetObject;
import com.ctyun.devops.model.index.Department;
import com.ctyun.devops.model.index.Employee;
import com.ctyun.devops.model.index.Product;
import com.ctyun.devops.repository.DepartmentRepository;
import com.ctyun.devops.repository.EmployeeRepository;
import com.ctyun.devops.repository.ProductRepository;
import com.ctyun.devops.service.ESIndexRelationshipManager;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import java.util.List;

import static com.ctyun.devops.constants.ESConstants.*;

@Component
@RequiredArgsConstructor
public class ProductOperatorStrategy implements OperatorStrategy {
    private final EmployeeRepository employeeRepository;
    private final DepartmentRepository departmentRepository;
    private final ProductRepository productRepository;
    private final ESIndexRelationshipManager esIndexRelationshipManager;

    @Override
    public void processOperator(TargetObject targetObject) {
        JSONObject data = targetObject.getData();
        long productId = data.getLong(ID_FIELD);

        OperatorTypeEnum operator = targetObject.getOperator();
        if (OperatorTypeEnum.INSERT.equals(operator)) {
            Product product = data.getJSONObject(PRODUCT_FIELD).toJavaObject(Product.class);
            product.setId(productId);
            productRepository.save(product);

            if (!CollectionUtils.isEmpty(data.getJSONArray(ADD_EMPLOYEES_FIELD))) {
                // 插入 employees 到 product 中
                esIndexRelationshipManager.addEmployeeAndProductRelationship(product, data.getJSONArray(ADD_EMPLOYEES_FIELD));
            }
            if (!ObjectUtils.isEmpty(data.getJSONObject(BELONG_DEPARTMENT_FIELD))) {
                // 插入 product 与 department 的关系
                esIndexRelationshipManager.addProductAndDepartmentRelationship(product, data.getJSONObject(BELONG_DEPARTMENT_FIELD));
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
                esIndexRelationshipManager.updateTargetDocument(String.valueOf(targetProduct.getId()), PRODUCT_FIELD, targetProduct);

                // 编辑所有包含这个 product 的 employee文档的数据
                List<Employee> employees = employeeRepository.findEmployeesByProductId(productId);
                for (Employee employee : employees) {
                    esIndexRelationshipManager.updateProductFromTargetIndex(String.valueOf(employee.getId()), EMPLOYEE_FIELD, targetProduct);
                }
                // 编辑所有包含这个 product 的 department文档的数据
                List<Department> departments = departmentRepository.findDepartmentsByProductId(productId);
                for (Department department : departments) {
                    esIndexRelationshipManager.updateProductFromTargetIndex(String.valueOf(department.getId()), DEPARTMENT_FIELD, targetProduct);
                }
            }
            if (!CollectionUtils.isEmpty(data.getJSONArray(ADD_EMPLOYEES_FIELD))) {
                Product product = productRepository.findById(productId).orElseThrow(() -> new RuntimeException("error."));
                // 插入 employees 和 product 的关系
                esIndexRelationshipManager.addEmployeeAndProductRelationship(product, data.getJSONArray(ADD_EMPLOYEES_FIELD));
            }
            if (!CollectionUtils.isEmpty(data.getJSONArray(DELETE_EMPLOYEES_FIELD))) {
                // 删除 product 和 employees 的关系
                esIndexRelationshipManager.deleteEmployeeAndProductRelationship(String.valueOf(productId), data.getJSONArray(DELETE_EMPLOYEES_FIELD));
            }
            if (!ObjectUtils.isEmpty(data.getJSONObject(BELONG_DEPARTMENT_FIELD))) {
                // 删除 product 与 旧department 的关系
                List<Department> oldDepartments = departmentRepository.findDepartmentsByProductId(productId);
                if (CollectionUtils.isEmpty(oldDepartments)) {
                    throw new RuntimeException("error.");
                }
                Department oldDepartment = oldDepartments.get(0);
                esIndexRelationshipManager.deleteProductAndDepartmentRelationship(String.valueOf(productId), String.valueOf(oldDepartment.getId()));

                // 插入 product 与 新department 的关系
                Product product = productRepository.findById(productId).orElseThrow(() -> new RuntimeException("error."));
                esIndexRelationshipManager.addProductAndDepartmentRelationship(product, data.getJSONObject(BELONG_DEPARTMENT_FIELD));
            }
        } else {
            // 删除
            productRepository.deleteById(productId);

            // 从所有的 department文档中，删除包含这个 product 的数据
            List<Department> departments = departmentRepository.findDepartmentsByProductId(productId);
            for (Department department : departments) {
                esIndexRelationshipManager.deleteProductFromTargetIndex(String.valueOf(department.getId()), DEPARTMENT_FIELD, productId);
            }

            // 从所有的 employee文档中，删除包含这个 product 的数据
            List<Employee> employees = employeeRepository.findEmployeesByProductId(productId);
            for (Employee employee : employees) {
                esIndexRelationshipManager.deleteProductFromTargetIndex(String.valueOf(employee.getId()), EMPLOYEE_FIELD, productId);
            }
        }
    }
}
