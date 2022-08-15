package com.ctyun.devops.repository;

import java.util.List;

import org.springframework.data.elasticsearch.annotations.Query;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import com.ctyun.devops.model.index.Product;

/**
 * @author bgy
 * @date 2022/8/13 17:18
 */
@Repository
public interface ProductRepository extends ElasticsearchRepository<Product, Long> {

	@Query("{\"match\":{\"employees.id\": \"?0\"}}")
	List<Product> findDepartmentsByEmployeeId(long employeeId);

	@Query("{\"match\":{\"departments.id\": \"?0\"}}")
	List<Product> findDepartmentsByDepartmentId(long departmentId);
}
