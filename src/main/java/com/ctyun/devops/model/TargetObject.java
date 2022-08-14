package com.ctyun.devops.model;

import com.alibaba.fastjson.JSONObject;
import com.ctyun.devops.enums.OperatorTypeEnum;
import com.ctyun.devops.enums.TargetTypeEnum;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author bgy
 * @date 2022/8/13 15:59
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class TargetObject {
	private TargetTypeEnum target;
	private OperatorTypeEnum operator;
	private JSONObject data;
}
