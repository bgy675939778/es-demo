package com.ctyun.devops.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ctyun.devops.enums.TargetTypeEnum;
import com.ctyun.devops.model.TargetObject;
import com.ctyun.devops.service.strategy.DepartmentOperatorStrategy;
import com.ctyun.devops.service.strategy.EmployeeOperatorStrategy;
import com.ctyun.devops.service.strategy.OperatorStrategy;
import com.ctyun.devops.service.strategy.ProductOperatorStrategy;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ctyun.devops.constants.ESConstants.*;


/**
 * @author bgy
 * @date 2022/8/13 16:37
 */
@Service
@RequiredArgsConstructor
public class EsOperatorService3 {
    private final RestHighLevelClient highLevelClient;
    private final EmployeeOperatorStrategy employeeOperatorStrategy;
    private final DepartmentOperatorStrategy departmentOperatorStrategy;
    private final ProductOperatorStrategy productOperatorStrategy;
    private Map<TargetTypeEnum, OperatorStrategy> operatorStrategyMap;

    private static final String HIGHLIGHT_PRE_TAGS = "<span style=\"color:red\">";
    private static final String HIGHLIGHT_POST_TAGS = "</span>";
    private static final String ES_CLASS_FIELD = "_class";
    private static final String TYPE_FIELD = "type";

    @PostConstruct
    public void init() {
        operatorStrategyMap = new HashMap<>();
        operatorStrategyMap.put(TargetTypeEnum.EMPLOYEE, employeeOperatorStrategy);
        operatorStrategyMap.put(TargetTypeEnum.DEPARTMENT, departmentOperatorStrategy);
        operatorStrategyMap.put(TargetTypeEnum.PRODUCT, productOperatorStrategy);
    }

    public void processKafkaMessage(String message) {
        TargetObject targetObject = JSONObject.parseObject(message).toJavaObject(TargetObject.class);
        operatorStrategyMap.get(targetObject.getTarget()).processOperator(targetObject);
    }

    public void queryByPrefix(String prefix) throws IOException {
        HighlightBuilder highlightBuilder = new HighlightBuilder()
                .field(NAME_FIELD)
                .preTags(HIGHLIGHT_PRE_TAGS)
                .postTags(HIGHLIGHT_POST_TAGS)
                .numOfFragments(1)
                .fragmentSize(50);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .fetchSource(NAME_FIELD, null)
                .query(QueryBuilders.matchPhrasePrefixQuery(NAME_FIELD, prefix))
                .highlighter(highlightBuilder);

        SearchHits searchHits = highLevelClient.search(new SearchRequest()
                .indices(EMPLOYEE_FIELD, DEPARTMENT_FIELD, PRODUCT_FIELD)
                .source(searchSourceBuilder), RequestOptions.DEFAULT).getHits();

        List<JSONObject> result = new ArrayList<>();
        if (searchHits.getHits().length > 0) {
            for (SearchHit searchHit : searchHits.getHits()) {
                JSONObject jsonObject = JSONObject.parseObject(searchHit.getSourceAsString());
                // 使用 highlight 替换原本的 value
                jsonObject.put(NAME_FIELD, searchHit.getHighlightFields().get(NAME_FIELD).getFragments()[0].toString());
                jsonObject.remove(ES_CLASS_FIELD);
                result.add(jsonObject);
            }
        }

        result.forEach(System.out::println);
    }

    public void queryByFullText(String text) throws IOException {
        HighlightBuilder highlightBuilder = new HighlightBuilder()
                .field(NAME_FIELD)
                .field(DESCRIBE_FIELD)
                .field(EMPLOYEES_NAME_FIELD)
                .field(DEPARTMENTS_NAME_FIELD)
                .field(PRODUCTS_NAME_FIELD)
                .preTags(HIGHLIGHT_PRE_TAGS)
                .postTags(HIGHLIGHT_POST_TAGS)
                .fragmentSize(50);

        MultiMatchQueryBuilder multiMatchQueryBuilder = new MultiMatchQueryBuilder(text, EMPLOYEES_NAME_FIELD, DEPARTMENTS_NAME_FIELD, PRODUCTS_NAME_FIELD)
                .field(NAME_FIELD, 2)
                .field(DESCRIBE_FIELD, 2);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(multiMatchQueryBuilder)
                .highlighter(highlightBuilder);

        SearchHits searchHits = highLevelClient.search(new SearchRequest()
                        .indices(EMPLOYEE_FIELD, DEPARTMENT_FIELD, PRODUCT_FIELD)
                        .source(searchSourceBuilder), RequestOptions.DEFAULT)
                .getHits();

        List<JSONObject> result = new ArrayList<>();
        if (searchHits.getHits().length > 0) {
            for (SearchHit searchHit : searchHits.getHits()) {
                JSONObject jsonObject = JSONObject.parseObject(searchHit.getSourceAsString());
                jsonObject.put(TYPE_FIELD, searchHit.getIndex());
                jsonObject.remove(ES_CLASS_FIELD);

                Map<String, HighlightField> highlightFieldMap = searchHit.getHighlightFields();
                highlightFieldMap.forEach((field, highlightField) -> {
                    String highlightValue = highlightField.getFragments()[0].toString();
                    if (!field.contains(".")) {
                        // 非嵌套类型（name, describe），直接替换
                        jsonObject.put(field, highlightValue);
                    } else {
                        // 如果是嵌套类型，需要跟 source 中的多个元素进行匹配
                        String[] strings = field.split("\\.");
                        JSONArray jsonArray = jsonObject.getJSONArray(strings[0]);
                        for (Object object : jsonArray) {
                            // 实现：从highlight中根据标签，截取出匹配的关键字，再判断 sourceValue 中是否包含；
                            // 例如：
                            // 从 "<font color='red'>北极星</font>-的描述" 中截取出 “北极星”
                            // sourceValue为："北极星的描述"，即 sourceValue.contains("北极星")
                            String highlightKeyValue = StringUtils.substringBetween(highlightValue, ">", "<");
                            String sourceValue = ((JSONObject) object).getString(NAME_FIELD);
                            if (sourceValue.contains(highlightKeyValue)) {
                                ((JSONObject) object).put(NAME_FIELD, highlightValue);
                            }
                        }
                    }
                });
                result.add(jsonObject);
            }
        }
        result.forEach(System.out::println);
    }

}
