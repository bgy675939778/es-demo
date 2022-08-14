package com.ctyun.devops.config;

import java.time.Duration;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.config.AbstractElasticsearchConfiguration;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.convert.ElasticsearchConverter;
import org.springframework.data.elasticsearch.repository.config.EnableElasticsearchRepositories;

/**
 * @author bgy
 * @date 2022/8/14 17:35
 */
@Configuration
@EnableElasticsearchRepositories(basePackages = "com.ctyun.devops")
public class ElasticRestClientConfig extends AbstractElasticsearchConfiguration {

	@Value("${spring.elasticsearch.rest.uris}")
	private String url;

	@Override
	@Bean
	public RestHighLevelClient elasticsearchClient() {
		url = url.replace("http://", "");
		String[] urlArr = url.split(",");
		HttpHost[] httpPostArr = new HttpHost[urlArr.length];
		for (int i = 0; i < urlArr.length; i++) {
			HttpHost httpHost = new HttpHost(urlArr[i].split(":")[0].trim(),
				Integer.parseInt(urlArr[i].split(":")[1].trim()), "http");
			httpPostArr[i] = httpHost;
		}
		RestClientBuilder builder = RestClient.builder(httpPostArr)
			// 异步httpclient配置
			.setHttpClientConfigCallback(httpClientBuilder -> {
				// httpclient连接数配置
				httpClientBuilder.setMaxConnTotal(30);
				httpClientBuilder.setMaxConnPerRoute(10);
				// httpclient保活策略
				httpClientBuilder.setKeepAliveStrategy(((response, context) -> Duration.ofMinutes(5).toMillis()));
				return httpClientBuilder;
			});
		return new RestHighLevelClient(builder);
	}

	@Bean
	public ElasticsearchRestTemplate elasticsearchRestTemplate(RestHighLevelClient elasticsearchClient, ElasticsearchConverter elasticsearchConverter) {
		return new ElasticsearchRestTemplate(elasticsearchClient, elasticsearchConverter);
	}
}
