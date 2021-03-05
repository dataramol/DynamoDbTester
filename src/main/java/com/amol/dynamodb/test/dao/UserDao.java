package com.amol.dynamodb.test.dao;

import java.util.List;

import com.amol.dynamodb.test.domain.User;

public interface UserDao {

	User createUser(User user);
	User readUser(String userId);
	User updateUser(User user);
	User deleteUser(String userId);
	void setup();
	List<User> readAllUsers();
}
