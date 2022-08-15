package com.ctyun.devops.model.index;

import java.util.List;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author bgy
 * @date 2022/8/13 18:15
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Document(indexName = "product")
public class Product {
	@Id
	private Long id;

	@Field
	private String name;

	@Field
	private String describe;
}
