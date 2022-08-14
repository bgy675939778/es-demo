package com.ctyun.devops.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.UpdateQuery;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

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
		OperatorTypeEnum operator = targetObject.getOperator();
		if (OperatorTypeEnum.INSERT_BATCH.equals(operator)) {
			JSONArray jsonArray = targetObject.getData().getJSONArray("employees");
			List<Employee> employees = jsonArray.toJavaList(Employee.class);

			employeeRepository.saveAll(employees);
		} else {
			JSONObject jsonObject = targetObject.getData().getJSONObject("employee");
			Employee employee = jsonObject.toJavaObject(Employee.class);

			if (OperatorTypeEnum.INSERT.equals(operator)) {
				employeeRepository.save(employee);
			} else if (OperatorTypeEnum.UPDATE.equals(operator)) {
				// 编辑
				employeeRepository.save(employee);

				// 编辑 department、product 索引的内容
			} else {
				// 删除
				employeeRepository.deleteById(employee.getId());

				// 从 department、product 索引中删除

			}
		}

	}

	private void processDepartmentOperator(TargetObject targetObject) {
		JSONObject data = targetObject.getData();
		Department department = data.getJSONObject("department").toJavaObject(Department.class);

		OperatorTypeEnum operator = targetObject.getOperator();
		if (OperatorTypeEnum.INSERT.equals(operator)) {
			departmentRepository.save(department);

			if (!CollectionUtils.isEmpty(data.getJSONArray("addEmployees"))) {
				// 插入 employee 到 department 中
				this.addEmployeesToDepartment(department, data.getJSONArray("addEmployees"));
			}
		} else if (OperatorTypeEnum.UPDATE.equals(operator)) {
			if (!StringUtils.isEmpty(department.getName()) || !StringUtils.isEmpty(department.getDescribe())) {
				// 编辑 department 本身
				departmentRepository.save(department);
			}
			if (!CollectionUtils.isEmpty(data.getJSONArray("addEmployees"))) {
				// 插入 employees 到 department 中
				this.addEmployeesToDepartment(department, data.getJSONArray("addEmployees"));
			}
			if (!CollectionUtils.isEmpty(data.getJSONArray("deleteEmployees"))) {
				// 从 department 中 删除 employees
			}
		} else {
			// 删除（删除部门的前提是，属于这个部门的 employees、products 先要被删完，所以这里不用处理 employees 和 products）
			departmentRepository.deleteById(department.getId());
		}
	}

	private void processProductOperator(TargetObject targetObject) {
		JSONObject jsonObject = targetObject.getData().getJSONObject("product");
		Product product = jsonObject.toJavaObject(Product.class);

		OperatorTypeEnum operator = targetObject.getOperator();
		if (OperatorTypeEnum.INSERT.equals(operator)) {
			productRepository.save(product);

			// 插入 employees 到 product 中

			// 插入 product 与 department 的关系
		} else if (OperatorTypeEnum.UPDATE.equals(operator)) {
			if (!StringUtils.isEmpty(product.getName()) || !StringUtils.isEmpty(product.getDescribe())) {
				// 编辑 product 本身
				productRepository.save(product);
			}
			if (!CollectionUtils.isEmpty(product.getAddEmployees())) {
				// 插入 employees 到 product 中
			}
			if (!CollectionUtils.isEmpty(product.getDeleteEmployees())) {
				// 从 product 中 删除 employees
			}
			if (!ObjectUtils.isEmpty(product.getBelongDepartment())) {
				// 编辑 product 与 department 的关系
			}
		} else {
			// 删除
			productRepository.deleteById(product.getId());

			// 删除 product 与 department 的关系

			// 删除 employees 中的 product

		}
	}

	private void addEmployeesToDepartment(Department department, JSONArray employeeJSONArray) {
		for (Object employee : employeeJSONArray) {
			Map<String, Object> params = new HashMap<>();
			params.put("employee", employee);

			UpdateQuery updateQuery = UpdateQuery.builder(department.getId().toString())
				.withScript(
					"if (ctx._source.employees != null) { def targets = ctx._source.employees.findAll(employee -> employee.id == params.employee.id);if(targets.size() == 0){ctx._source.employees.add(params.employee)} } else {ctx._source.employees = [params.employee]}")
				.withParams(params)
				.build();

			esRestTemplate.update(updateQuery, IndexCoordinates.of("department"));
		}
	}
}
