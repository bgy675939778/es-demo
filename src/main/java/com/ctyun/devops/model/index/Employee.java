package com.ctyun.devops.model.index;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.elasticsearch.annotations.Mapping;
import org.springframework.data.elasticsearch.annotations.Setting;

/**
 * @author bgy
 * @date 2022/8/13 17:15
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(indexName = "employee")
public class Employee {
	@Id
	private Long id;

	@Field
	private String name;

	@Field
	private String describe;
}
