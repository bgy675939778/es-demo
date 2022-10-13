package com.ctyun.devops.service.strategy;

import com.alibaba.fastjson.JSONArray;
import com.ctyun.devops.enums.OperatorTypeEnum;
import com.ctyun.devops.model.TargetObject;
import com.ctyun.devops.model.index.Department;
import com.ctyun.devops.model.index.Employee;
import com.ctyun.devops.model.index.Product;
import com.ctyun.devops.repository.DepartmentRepository;
import com.ctyun.devops.repository.EmployeeRepository;
import com.ctyun.devops.repository.ProductRepository;
import com.ctyun.devops.service.ESIndexRelationshipManager;
import com.ctyun.devops.utils.SpringUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.List;

import static com.ctyun.devops.constants.ESConstants.*;

@Component
@RequiredArgsConstructor
public class EmployeeOperatorStrategy implements OperatorStrategy {
    private final EmployeeRepository employeeRepository;
    private final DepartmentRepository departmentRepository;
    private final ProductRepository productRepository;
    private final ESIndexRelationshipManager esIndexRelationshipManager;

    @Override
    public void processOperator(TargetObject targetObject) {
        JSONArray jsonArray = targetObject.getData().getJSONArray(EMPLOYEES_FIELD);
        List<Employee> employees = jsonArray.toJavaList(Employee.class);

        OperatorTypeEnum operator = targetObject.getOperator();
        if (OperatorTypeEnum.INSERT.equals(operator)) {
            Department department = SpringUtils.getBean("department");
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
            esIndexRelationshipManager.updateTargetDocument(String.valueOf(targetEmployee.getId()), EMPLOYEE_FIELD, targetEmployee);

            // 编辑所有包含这个 employee 的 department文档的数据
            List<Department> departments = departmentRepository.findDepartmentsByEmployeeId(employeeId);
            for (Department department : departments) {
                esIndexRelationshipManager.updateEmployeeFromTargetIndex(String.valueOf(department.getId()), DEPARTMENT_FIELD, targetEmployee);
            }

            // 编辑所有包含这个 employee 的 product文档的数据
            List<Product> products = productRepository.findDepartmentsByEmployeeId(employeeId);
            for (Product product : products) {
                esIndexRelationshipManager.updateEmployeeFromTargetIndex(String.valueOf(product.getId()), PRODUCT_FIELD, targetEmployee);
            }
        } else {
            for (Employee employee : employees) {
                long employeeId = employee.getId();
                // 删除 employee本身
                employeeRepository.deleteById(employeeId);

                // 从所有的 department 文档中删除包含这个 employee 的数据
                List<Department> departments = departmentRepository.findDepartmentsByEmployeeId(employeeId);
                for (Department department : departments) {
                    esIndexRelationshipManager.deleteEmployeeFromTargetIndex(String.valueOf(department.getId()), DEPARTMENT_FIELD, employeeId);
                }

                // 从所有的 product 文档中删除包含这个 employee 的数据
                List<Product> products = productRepository.findDepartmentsByEmployeeId(employeeId);
                for (Product product : products) {
                    esIndexRelationshipManager.deleteEmployeeFromTargetIndex(String.valueOf(product.getId()), PRODUCT_FIELD, employeeId);
                }
            }
        }
    }
}
