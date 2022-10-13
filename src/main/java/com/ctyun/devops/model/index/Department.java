package com.ctyun.devops.model.index;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.Mapping;
import org.springframework.data.elasticsearch.annotations.Setting;

/**
 * @author bgy
 * @date 2022/8/13 18:10
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Document(indexName = "department")
public class Department {
	@Id
	private Long id;

	@Field
	private String name;

	@Field
	private String shortName;

	@Field
	private String describe;
}
