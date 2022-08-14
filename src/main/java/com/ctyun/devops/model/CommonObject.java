package com.ctyun.devops.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * @author bgy
 * @date 2022/8/13 16:01
 */
@Data
@Builder
@AllArgsConstructor
public class CommonObject {
	private Long id;
	private String name;
	private String describe;
}
