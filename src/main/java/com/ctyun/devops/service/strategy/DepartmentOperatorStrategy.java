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
public class DepartmentOperatorStrategy implements OperatorStrategy{
    private final EmployeeRepository employeeRepository;
    private final DepartmentRepository departmentRepository;
    private final ProductRepository productRepository;
    private final ESIndexRelationshipManager esIndexRelationshipManager;
    
    @Override
    public void processOperator(TargetObject targetObject) {
        JSONObject data = targetObject.getData();
        long departmentId = data.getLong(ID_FIELD);

        OperatorTypeEnum operator = targetObject.getOperator();
        if (OperatorTypeEnum.INSERT.equals(operator)) {
            Department department = data.getJSONObject(DEPARTMENT_FIELD).toJavaObject(Department.class);
            department.setId(departmentId);
            departmentRepository.save(department);

            if (!CollectionUtils.isEmpty(data.getJSONArray(ADD_EMPLOYEES_FIELD))) {
                // 插入 employee 和 department 的关系
                esIndexRelationshipManager.addEmployeeAndDepartmentRelationship(department, data.getJSONArray(ADD_EMPLOYEES_FIELD));
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
                esIndexRelationshipManager.updateTargetDocument(String.valueOf(targetDepartment.getId()), DEPARTMENT_FIELD, targetDepartment);

                // 编辑所有包含这个 department 的 employee文档的数据
                List<Employee> employees = employeeRepository.findEmployeesByDepartmentId(departmentId);
                for (Employee employee : employees) {
                    esIndexRelationshipManager.updateDepartmentFromTargetIndex(String.valueOf(employee.getId()), EMPLOYEE_FIELD, targetDepartment);
                }

                // 编辑所有包含这个 department 的 product文档的数据
                List<Product> products = productRepository.findDepartmentsByDepartmentId(departmentId);
                for (Product product : products) {
                    esIndexRelationshipManager.updateDepartmentFromTargetIndex(String.valueOf(product.getId()), PRODUCT_FIELD, targetDepartment);
                }
            }
            if (!CollectionUtils.isEmpty(data.getJSONArray(ADD_EMPLOYEES_FIELD))) {
                Department department = departmentRepository.findById(departmentId).orElseThrow(() -> new RuntimeException("error."));
                // 插入 employees 和 department 的关系
                esIndexRelationshipManager.addEmployeeAndDepartmentRelationship(department, data.getJSONArray(ADD_EMPLOYEES_FIELD));
            }
            if (!CollectionUtils.isEmpty(data.getJSONArray(DELETE_EMPLOYEES_FIELD))) {
                // 删除 employees 和 department 的关系
                esIndexRelationshipManager.deleteEmployeeAndDepartmentRelationship(String.valueOf(departmentId), data.getJSONArray(DELETE_EMPLOYEES_FIELD));
            }
        } else {
            // 删除（删除部门的前提是，属于这个部门的 employees、products 先要被删完，所以这里不用处理 employees 和 products）
            departmentRepository.deleteById(departmentId);
        }
    }
}
