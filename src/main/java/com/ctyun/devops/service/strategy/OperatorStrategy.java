package com.ctyun.devops.service.strategy;

import com.ctyun.devops.model.TargetObject;

public interface OperatorStrategy {
    void processOperator(TargetObject targetObject);
}
