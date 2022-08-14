package com.ctyun.devops.repository;

import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import com.ctyun.devops.model.index.Employee;

/**
 * @author bgy
 * @date 2022/8/13 17:18
 */
@Repository
public interface EmployeeRepository extends ElasticsearchRepository<Employee, Long> {

	Employee findByName(String name);
}
