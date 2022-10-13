package com.ctyun.devops.dynamic.message;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ctyun.devops.enums.OperatorTypeEnum;
import com.ctyun.devops.enums.TargetTypeEnum;
import com.ctyun.devops.model.index.Employee;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.ctyun.devops.constants.KafkaJsonConstant.*;

public class PushMsgKafkaComponent {

    public static void main(String[] args) {
        PushMsgKafkaComponent component = new PushMsgKafkaComponent();
        Employee employee = Employee.builder().id(1L).build();
        Employee employee1 = Employee.builder().id(2L).build();
        List<Employee> employees = Arrays.asList(employee,employee1);
        component.syncDynamicMessage(1L, "张三", TargetTypeEnum.EMPLOYEE, "删除了李四", OperatorTypeEnum.DELETE, employees);
    }

    public void syncDynamicMessage(long operatorId, String operatorName, TargetTypeEnum target, String message, OperatorTypeEnum employeeOperatorType, List<Employee> employees) {
        JSONObject jsonObject = JSON.parseObject(DYNAMIC_MESSAGE_JSON);
        jsonObject.put(OPERATOR_ID, operatorId);
        jsonObject.put(OPERATOR_NAME, operatorName);
        jsonObject.put(TARGET, target);
        jsonObject.put(MESSAGE, message);

        JSONObject data = jsonObject.getJSONObject(DATA);
        data.put(EMPLOYEE_OPERATOR_TYPE, employeeOperatorType);
        data.put(EMPLOYEES, employees);

        System.out.println(jsonObject.toJSONString());
    }
}
