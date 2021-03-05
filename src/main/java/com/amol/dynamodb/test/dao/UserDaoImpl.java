package com.amol.dynamodb.test.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBDeleteExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBSaveExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amol.dynamodb.test.domain.User;

@Component
public class UserDaoImpl implements UserDao {

	@Autowired
	DynamoDBMapper dynamoDBMapper;
	
	@Autowired
	AmazonDynamoDB amazonDynamoDB;
	
	@Override
	public void setup()
	{
		CreateTableRequest tableRequest = dynamoDBMapper.generateCreateTableRequest(User.class);
		tableRequest.setProvisionedThroughput(new ProvisionedThroughput(1L,1L));
		amazonDynamoDB.createTable(tableRequest);
	}
	
	@Override
	public User createUser(User user) {
			dynamoDBMapper.save(user);
			return user;
	}

	@Override
	public User readUser(String userId) {
		return dynamoDBMapper.load(User.class, userId);
	}

	@Override
	public User updateUser(User user) {
		Map<String, ExpectedAttributeValue> expectedAttributeValueMap = new HashMap<>();
		expectedAttributeValueMap.put("userId", new ExpectedAttributeValue(new AttributeValue().withS(user.getUserId())));
		DynamoDBSaveExpression saveExpression = new DynamoDBSaveExpression().withExpected(expectedAttributeValueMap);
		dynamoDBMapper.save(user, saveExpression);
		return user;
	}

	@Override
	public User deleteUser(String userId) {
		Map<String, ExpectedAttributeValue> expectedAttributeValueMap = new HashMap<>();
		expectedAttributeValueMap.put("userId", new ExpectedAttributeValue(new AttributeValue().withS(userId)));
		DynamoDBDeleteExpression deleteExpression = new DynamoDBDeleteExpression().withExpected(expectedAttributeValueMap);
		User user = readUser(userId);
		dynamoDBMapper.delete(user, deleteExpression);
		return null;
	}

	@Override
	public List<User> readAllUsers() {
		DynamoDBScanExpression scanExpression = new DynamoDBScanExpression();
		List<User> users = new ArrayList<>();
		dynamoDBMapper.scan(User.class, scanExpression).forEach(u -> users.add(u));
		return users;
	}

}
