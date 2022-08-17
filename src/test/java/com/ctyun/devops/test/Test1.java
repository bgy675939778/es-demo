package com.ctyun.devops.test;

import java.io.IOException;
import java.util.List;

import com.ctyun.devops.service.EsOperatorService2;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import com.ctyun.devops.model.index.Department;
import com.ctyun.devops.repository.DepartmentRepository;
import com.ctyun.devops.service.EsOperatorService;

/**
 * @author bgy
 * @date 2022/8/13 16:12
 */
@SpringBootTest
public class Test1 {
//    @Autowired
//    private EsOperatorService esOperatorService;

    @Autowired
    private EsOperatorService2 esOperatorService;
    @Autowired
    private DepartmentRepository departmentRepository;

    @Test
    public void testQuery() throws IOException {
        esOperatorService.queryByPrefix("zhang");
    }

    @Test
    public void testCRUD() {
        testEmployeeInsert();
        testDepartmentInsert();
        testInsertEmployeesToDepartment();
        testProductContainsEmployeesAndDepartment();
        testUpdateTargetAndUpdateRelationship();
        testDeleteEmployeesAndDeleteRelationship();
    }

    @Test
    public void testEmployeeInsert() {
        String singleAddString1 = "{\"data\":{\"employees\":[{\"describe\":\"我是一个性格开朗的人\",\"id\":1,\"name\":\"张三\"}]},\"operator\":\"INSERT\",\"target\":\"EMPLOYEE\"}";
        String singleAddString2 = "{\"data\":{\"employees\":[{\"describe\":\"我不是一个性格开朗的人\",\"id\":2,\"name\":\"李四\"}]},\"operator\":\"INSERT\",\"target\":\"EMPLOYEE\"}";
        String batchAddString = "{\"data\":{\"employees\":[{\"describe\":\"我是王五\",\"id\":3,\"name\":\"王五\"},{\"describe\":\"小明是我\",\"id\":4,\"name\":\"小明\"},{\"describe\":\"小花小花小花\",\"id\":5,\"name\":\"小花\"},{\"describe\":\"我是晓东，晓东是我\",\"id\":6,\"name\":\"晓东\"},{\"describe\":\"小白白啊白\",\"id\":7,\"name\":\"小白\"},{\"describe\":\"我是张效能，名字有点奇怪\",\"id\":8,\"name\":\"张效能\"}]},\"operator\":\"INSERT\",\"target\":\"EMPLOYEE\"}";

        esOperatorService.processKafkaMessage(singleAddString1);
        esOperatorService.processKafkaMessage(singleAddString2);
        esOperatorService.processKafkaMessage(batchAddString);
    }

    @Test
    public void testDepartmentInsert() {
        String addString1 = "{\"data\":{\"id\":1,\"department\":{\"name\":\"研发一部\",\"describe\":\"这是研发一部的描述，研发一部有1000人，在成都、上海都有办公区\"}},\"operator\":\"INSERT\",\"target\":\"DEPARTMENT\"}";
        String addString2 = "{\"data\":{\"id\":2,\"department\":{\"name\":\"研发二部\",\"describe\":\"这是研发二部的描述，研发二部有2000人，在成都、北京、广州都有办公区\"}},\"operator\":\"INSERT\",\"target\":\"DEPARTMENT\"}";
        String addString3 = "{\"data\":{\"id\":3,\"department\":{\"name\":\"云数中心\",\"describe\":\"这个部门是属于研发一部的\"}},\"operator\":\"INSERT\",\"target\":\"DEPARTMENT\"}";
        String addString4 = "{\"data\":{\"id\":4,\"department\":{\"name\":\"业务效能团队\",\"describe\":\"这是一个关注于业务效能、技术效能的团队\"}},\"operator\":\"INSERT\",\"target\":\"DEPARTMENT\"}";

        esOperatorService.processKafkaMessage(addString1);
        esOperatorService.processKafkaMessage(addString2);
        esOperatorService.processKafkaMessage(addString3);
        esOperatorService.processKafkaMessage(addString4);
    }

    @Test
    public void testInsertEmployeesToDepartment() {
        String updateString1 = "{\"target\":\"DEPARTMENT\",\"operator\":\"UPDATE\",\"data\":{\"id\":1,\"addEmployees\":[{\"id\":1,\"name\":\"张三\",\"describe\":\"我是一个性格开朗的人\"},{\"id\":2,\"name\":\"李四\",\"describe\":\"我是一个性格开朗的人\"}]}}";
        String updateString2 = "{\"target\":\"DEPARTMENT\",\"operator\":\"UPDATE\",\"data\":{\"id\":2,\"addEmployees\":[{\"id\":3,\"name\":\"王五\",\"describe\":\"我是王五\"},{\"id\":4,\"name\":\"小明\",\"describe\":\"小明是我\"}]}}";
        String updateString3 = "{\"target\":\"DEPARTMENT\",\"operator\":\"UPDATE\",\"data\":{\"id\":3,\"addEmployees\":[{\"id\":5,\"name\":\"小花\",\"describe\":\"小花小花小花\"},{\"id\":6,\"name\":\"晓东\",\"describe\":\"我是晓东，晓东是我\"}]}}";
        String updateString4 = "{\"target\":\"DEPARTMENT\",\"operator\":\"UPDATE\",\"data\":{\"id\":4,\"addEmployees\":[{\"id\":7,\"name\":\"小白\",\"describe\":\"小白白啊白\"},{\"id\":8,\"name\":\"张效能\",\"describe\":\"我是张效能，名字有点奇怪\"}]}}";

        esOperatorService.processKafkaMessage(updateString1);
        esOperatorService.processKafkaMessage(updateString2);
        esOperatorService.processKafkaMessage(updateString3);
        esOperatorService.processKafkaMessage(updateString4);
    }

    @Test
    public void testProductContainsEmployeesAndDepartment() {
        String addString1 = "{\"target\":\"PRODUCT\",\"operator\":\"INSERT\",\"data\":{\"id\":1,\"product\":{\"name\":\"北极星\",\"describe\":\"北极星平台的描述\"},\"addEmployees\":[{\"id\":1,\"name\":\"张三\",\"describe\":\"我是一个性格开朗的人\"},{\"id\":2,\"name\":\"李四\",\"describe\":\"我是一个性格开朗的人\"}],\"belongDepartment\":{\"id\":1,\"name\":\"研发一部\",\"describe\":\"这是研发一部的描述，研发一部有1000人，在成都、上海都有办公区\"}}}";
        String addString2 = "{\"target\":\"PRODUCT\",\"operator\":\"INSERT\",\"data\":{\"id\":2,\"product\":{\"name\":\"观星台\",\"describe\":\"观星台是一个监控平台\"},\"addEmployees\":[{\"id\":2,\"name\":\"李四\",\"describe\":\"我是一个性格开朗的人\"},{\"id\":5,\"name\":\"小花\",\"describe\":\"小花小花小花\"}],\"belongDepartment\":{\"id\":1,\"name\":\"研发一部\",\"describe\":\"这是研发一部的描述，研发一部有1000人，在成都、上海都有办公区\"}}}";
        String addString3 = "{\"target\":\"PRODUCT\",\"operator\":\"INSERT\",\"data\":{\"id\":3,\"product\":{\"name\":\"业务技术效能平台\",\"describe\":\"业务技术效能产出的平台\"},\"addEmployees\":[{\"id\":7,\"name\":\"小白\",\"describe\":\"小白白啊白\"},{\"id\":8,\"name\":\"张效能\",\"describe\":\"我是张效能，名字有点奇怪\"}],\"belongDepartment\":{\"id\":4,\"name\":\"业务效能团队\",\"describe\":\"这是一个关注于业务效能、技术效能的团队\"}}}";

        esOperatorService.processKafkaMessage(addString1);
        esOperatorService.processKafkaMessage(addString2);
        esOperatorService.processKafkaMessage(addString3);
    }

    @Test
    public void testDeleteEmployeesAndDeleteRelationship() {
        String deleteString1 = "{\"data\":{\"employees\":[{\"id\":2},{\"id\":7}]},\"operator\":\"DELETE\",\"target\":\"EMPLOYEE\"}";
        esOperatorService.processKafkaMessage(deleteString1);
    }

    @Test
    public void testUpdateTargetAndUpdateRelationship() {
        String updateEmployeeString = "{\"data\":{\"employees\":[{\"describe\":\"我修改了张三的描述\",\"id\":1}]},\"operator\":\"UPDATE\",\"target\":\"EMPLOYEE\"}";
        String updateDepartmentString = "{\"data\":{\"id\":1,\"department\":{\"name\":\"研发一部-update\"}},\"operator\":\"UPDATE\",\"target\":\"DEPARTMENT\"}";
        String updateProductString = "{\"target\":\"PRODUCT\",\"operator\":\"UPDATE\",\"data\":{\"id\":1,\"product\":{\"name\":\"北极星-update\",\"describe\":\"北极星平台的描述-update\"}}}";

        esOperatorService.processKafkaMessage(updateEmployeeString);
        esOperatorService.processKafkaMessage(updateDepartmentString);
        esOperatorService.processKafkaMessage(updateProductString);
    }

    @Test
    public void testEmployeeCRUD() {
        String addString1 = "{\"data\":{\"employees\":[{\"describe\":\"我是小毕\",\"id\":9,\"name\":\"小毕\"}]},\"operator\":\"INSERT\",\"target\":\"EMPLOYEE\"}";
        esOperatorService.processKafkaMessage(addString1);

        String updateString1 = "{\"data\":{\"employees\":[{\"describe\":\"我是小毕asde\",\"id\":9,\"name\":\"小毕asde\"}]},\"operator\":\"UPDATE\",\"target\":\"EMPLOYEE\"}";
        esOperatorService.processKafkaMessage(updateString1);

        String deleteString = "{\"data\":{\"employees\":[{\"id\":9}]},\"operator\":\"DELETE\",\"target\":\"EMPLOYEE\"}";
        esOperatorService.processKafkaMessage(deleteString);
    }


    @Test
    public void testDepartmentCRUD() {
        // String addString1 = "{\"data\":{\"id\":1,\"department\":{\"name\":\"研发一部\",\"describe\":\"这是研发一部的描述，研发一部有1000人，在成都、上海都有办公区\"}},\"operator\":\"INSERT\",\"target\":\"DEPARTMENT\"}";
        // esOperatorService.processKafkaMessage(addString1);
        //
        // // 编辑 name
         String updateString1 = "{\"data\":{\"id\":1,\"department\":{\"name\":\"研发一部-update\"}},\"operator\":\"UPDATE\",\"target\":\"DEPARTMENT\"}";
        // esOperatorService.processKafkaMessage(updateString1);

//        // 都不编辑
//        String updateString2 = "{\"data\":{\"id\":1,\"operator\":\"UPDATE\",\"target\":\"DEPARTMENT\"}";
//        esOperatorService.processKafkaMessage(updateString2);

        // 添加成员到 department
        String updateEmployeesString = "{\"data\":{\"id\":1,\"addEmployees\":[{\"id\":3,\"name\":\"王五\",\"describe\":\"我是王五\"},{\"id\":4,\"name\":\"小明\",\"describe\":\"小明是我\"}]},\"operator\":\"UPDATE\",\"target\":\"DEPARTMENT\"}";
        esOperatorService.processKafkaMessage(updateEmployeesString);

        System.out.println();
    }



    @Test
    public void parseStringToObject() {

        // TargetObject targetObject1 = JSONObject.parseObject(SINGLE_INSERT_EMPLOYEE_STRING1).toJavaObject(TargetObject.class);
        //
        // TargetObject targetObject2 = JSONObject.parseObject(SINGLE_INSERT_EMPLOYEE_STRING2).toJavaObject(TargetObject.class);
        //
        // TargetObject targetObject3 = JSONObject.parseObject(BATCH_INSERT_EMPLOYEE_STRING).toJavaObject(TargetObject.class);

        System.out.println();
    }

    @Test
    public void buildEmployee() {
        // 构建 employee

//		CommonObject employee1 = CommonObject.builder().id(1L).name("张三").describe("我是一个性格开朗的人").build();
//		CommonObject employee2 = CommonObject.builder().id(2L).name("李四").describe("我不是一个性格开朗的人").build();
//		CommonObject employee3 = CommonObject.builder().id(3L).name("王五").describe("我是王五").build();
//		CommonObject employee4 = CommonObject.builder().id(4L).name("小明").describe("小明是我").build();
//		CommonObject employee5 = CommonObject.builder().id(5L).name("小花").describe("小花小花小花").build();
//		CommonObject employee6 = CommonObject.builder().id(6L).name("晓东").describe("我是晓东，晓东是我").build();
//		CommonObject employee7 = CommonObject.builder().id(7L).name("小白").describe("小白白啊白").build();
//		CommonObject employee8 = CommonObject.builder().id(8L).name("张效能").describe("我是张效能，名字有点奇怪").build();
//
//		JSONObject jsonObject1 = new JSONObject();
//		jsonObject1.put("employee", employee1);
//		TargetObject singleInsertEmployee1 = TargetObject.builder()
//			.target(TargetTypeEnum.EMPLOYEE)
//			.operator(OperatorTypeEnum.INSERT)
//			.data(jsonObject1)
//			.build();
//
//		JSONObject jsonObject2 = new JSONObject();
//		jsonObject2.put("employee", employee2);
//		TargetObject singleInsertEmployee2 = TargetObject.builder()
//			.target(TargetTypeEnum.EMPLOYEE)
//			.operator(OperatorTypeEnum.INSERT)
//			.data(jsonObject2)
//			.build();
//
//		JSONObject jsonObject3 = new JSONObject();
//		JSONArray jsonArray = new JSONArray();
//		jsonArray.addAll(Arrays.asList(employee3, employee4, employee5, employee6, employee7, employee8));
//		jsonObject3.put("employees", jsonArray);
//		TargetObject batchInsertEmployee = TargetObject.builder()
//			.target(TargetTypeEnum.EMPLOYEE)
//			.operator(OperatorTypeEnum.INSERT_BATCH)
//			.data(jsonObject3)
//			.build();
//
//		System.out.println(JSONObject.toJSONString(singleInsertEmployee1));
//		System.out.println(JSONObject.toJSONString(singleInsertEmployee2));
//		System.out.println(JSONObject.toJSONString(batchInsertEmployee));
    }

    @Test
    public void testGetByEmployeeId() {
        long employeeId = 3L;

        List<Department> departments = departmentRepository.findDepartmentsByEmployeeId(employeeId);

        System.out.println();
    }
}
