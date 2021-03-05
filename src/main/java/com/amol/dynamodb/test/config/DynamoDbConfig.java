package com.amol.dynamodb.test.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;

@Configuration
public class DynamoDbConfig {
	
	@Bean
	@Order(2)
	public DynamoDBMapper dynamoDBMapper(@Autowired AmazonDynamoDB client)
	{
		return new DynamoDBMapper(client);

	}


	@Bean 
	@Order(1)
	public AmazonDynamoDB amazonDynamoDB()
	{
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("key1", "key2")))
				.withEndpointConfiguration(new EndpointConfiguration("http://localhost:8000/", "us-west-2")).build();
		return client;
		
	}

}
