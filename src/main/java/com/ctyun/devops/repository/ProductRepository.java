package com.ctyun.devops.repository;

import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import com.ctyun.devops.model.index.Product;

/**
 * @author bgy
 * @date 2022/8/13 17:18
 */
@Repository
public interface ProductRepository extends ElasticsearchRepository<Product, Long> {
}
