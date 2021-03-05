package com.amol.dynamodb.test.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.amol.dynamodb.test.dao.UserDao;
import com.amol.dynamodb.test.domain.User;

@Service
public class UserService {

	
	private UserDao userDao;
	
	
	@Autowired
	public UserService(UserDao userDao)
	{
		this.userDao = userDao;
		//this.userDao.setup();
	}
	
	
	
	public User createUser(User user) {
        return userDao.createUser(user);
    }

    
    public User readUser(String userId) {
        return userDao.readUser(userId);
    }

    public User updateUser(User user) {
        return userDao.updateUser(user);
    }

    public void deleteUser(String userId) {
        userDao.deleteUser(userId);
    }
    
    public List<User> readAllUsers()
    {
    	return userDao.readAllUsers();
    }
}
