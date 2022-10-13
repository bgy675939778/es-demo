package com.ctyun.devops.constants;

/**
 * 用于kafka入队列使用对象模板
 *
 * @author ZhuYc
 * @date 2022/8/15
 */
public class KafkaJsonConstant {

    public static final String DATA = "data";
    public static final String EMPLOYEES = "employees";
    public static final String ID = "id";
    public static final String NAME = "name";
    public static final String DESCRIBE = "describe";
    public static final String DEPARTMENT = "department";
    public static final String ADD_EMPLOYEES = "addEmployees";
    public static final String DEL_EMPLOYEES = "deleteEmployees";
    public static final String PRODUCT = "product";
    public static final String BELONG_DEPARTMENT = "belongDepartment";

    public static final String OPERATOR_ID = "operatorId";
    public static final String OPERATOR_NAME = "operatorName";
    public static final String TARGET = "target";
    public static final String MESSAGE = "message";
    public static final String EMPLOYEE_OPERATOR_TYPE = "employeeOperatorType";



    public static final String EMPLOYEE_INSERT = "{\n" +
            "    \"target\":\"EMPLOYEE\",\n" +
            "    \"operator\":\"INSERT\",\n" +
            "    \"data\":{\n" +
            "        \"employees\": [\n" +
            "        ]\n" +
            "    }\n" +
            "}";
    public static final String EMPLOYEE_UPDATE = "{\n" +
            "    \"target\":\"EMPLOYEE\",\n" +
            "    \"operator\":\"UPDATE\",\n" +
            "    \"data\":{\n" +
            "        \"employees\": [\n" +
            "        ]\n" +
            "    }\n" +
            "}";

    public static final String DEPARTMENT_INSERT = "{\n" +
            "    \"target\":\"DEPARTMENT\",\n" +
            "    \"operator\":\"INSERT\",\n" +
            "    \"data\":{\n" +
            "        \"id\":-1,\n" +
            "        \"department\": {\n" +
            "          \"name\":\"\",\n" +
            "          \"describe\":\"\"\n" +
            "        },\n" +
            "        \"addEmployees\":[\n" +
            "        ],\n" +
            "        \"deleteEmployees\":[\n" +
            "        ]\n" +
            "    }\n" +
            "}";
    public static final String DEPARTMENT_UPDATE = "{\n" +
            "    \"target\":\"DEPARTMENT\",\n" +
            "    \"operator\":\"UPDATE\",\n" +
            "    \"data\":{\n" +
            "        \"id\":-1,\n" +
            "        \"department\": {\n" +
            "          \"name\":\"\",\n" +
            "          \"describe\":\"\"\n" +
            "        },\n" +
            "        \"addEmployees\":[\n" +
            "        ],\n" +
            "        \"deleteEmployees\":[\n" +
            "        ]\n" +
            "    }\n" +
            "}";
    public static final String DEPARTMENT_DELETE = "{\n" +
            "    \"target\":\"DEPARTMENT\",\n" +
            "    \"operator\":\"DELETE\",\n" +
            "    \"data\":{\n" +
            "        \"id\":-1\n" +
            "    }\n" +
            "}";

    public static final String PRODUCT_INSERT = "{\n" +
            "    \"target\":\"PRODUCT\",\n" +
            "    \"operator\":\"INSERT\",\n" +
            "    \"data\":{\n" +
            "       \"id\":-1,\n" +
            "       \"product\": {\n" +
            "          \"name\":\"\",\n" +
            "          \"describe\":\"\"\n" +
            "       },\n" +
            "        \"addEmployees\":[\n" +
            "        ],\n" +
            "        \"deleteEmployees\":[\n" +
            "        ],\n" +
            "        \"belongDepartment\":{\n" +
            "        }\n" +
            "    }\n" +
            "}";
    public static final String PRODUCT_UPDATE = "{\n" +
            "    \"target\":\"PRODUCT\",\n" +
            "    \"operator\":\"UPDATE\",\n" +
            "    \"data\":{\n" +
            "       \"id\":-1,\n" +
            "       \"product\": {\n" +
            "          \"name\":\"\",\n" +
            "          \"describe\":\"\"\n" +
            "       },\n" +
            "        \"addEmployees\":[\n" +
            "        ],\n" +
            "        \"deleteEmployees\":[\n" +
            "        ],\n" +
            "        \"belongDepartment\":{\n" +
            "        }\n" +
            "    }\n" +
            "}";
    public static final String PRODUCT_DELETE = "{\n" +
            "    \"target\":\"PRODUCT\",\n" +
            "    \"operator\":\"INSERT\",\n" +
            "    \"data\":{\n" +
            "       \"id\":-1\n" +
            "    }\n" +
            "}";

    public static final String EMP_INNER_JSON = " {\n" +
            "                \"id\":-1,\n" +
            "                \"name\":\"\"\n" +
            "            }";

    public static final String DEPT_INNER_JSON = "{\n" +
            "            \"id\":-1,\n" +
            "            \"name\":\"\"\n" +
            "        }";

    public static final String DYNAMIC_MESSAGE_JSON = "{\n" +
            "    \"operatorId\":-1,\n" +
            "    \"operatorName\":\"\",\n" +
            "    \"target\":\"\",\n" +
            "    \"message\":\"\",\n" +
            "    \"data\":{\n" +
            "        \"employeeOperatorType\":\"\",\n" +
            "        \"employees\":[" +
            "        ]\n" +
            "    }\n" +
            "}";
}
