package com.ctyun.devops.repository;

import java.util.List;

import org.springframework.data.elasticsearch.annotations.Query;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import com.ctyun.devops.model.index.Department;

/**
 * @author bgy
 * @date 2022/8/13 17:18
 */
@Repository
public interface DepartmentRepository extends ElasticsearchRepository<Department, Long> {

	@Query("{\"match\":{\"employees.id\": \"?0\"}}")
	List<Department> findDepartmentsByEmployeeId(long employeeId);

	@Query("{\"match\":{\"products.id\": \"?0\"}}")
	List<Department> findDepartmentsByProductId(long productId);
}
