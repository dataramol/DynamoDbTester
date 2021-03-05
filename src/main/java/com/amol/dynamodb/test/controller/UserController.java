package com.amol.dynamodb.test.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import com.amazonaws.AmazonClientException;
import com.amol.dynamodb.test.domain.User;
import com.amol.dynamodb.test.service.UserService;

@RestController
public class UserController {

	@Autowired
	UserService userServ;
	
	@RequestMapping(value = "/users", method = RequestMethod.GET)
	public ResponseEntity<Object> getAllUsers()
	{
		try
		{
			return new ResponseEntity<Object>(userServ.readAllUsers(), HttpStatus.OK);
		}
		catch(AmazonClientException e )
		{
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,e.getMessage(), e);
		}
	}
	
	@RequestMapping(value = "/user",method=RequestMethod.POST)
	public ResponseEntity<Object> createUser(@RequestBody User user)
	{
		try{
			User responseUser = userServ.createUser(user);
			return new ResponseEntity<Object>(responseUser, HttpStatus.CREATED);
		}
		catch(AmazonClientException e )
		{
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,e.getMessage(), e);
		}
	}
	
	@RequestMapping(value = "/user/{userId}",method=RequestMethod.GET)
	public ResponseEntity<Object> getUser(@PathVariable String userId)
	{
		try{
			User responseUser = userServ.readUser(userId);
			return new ResponseEntity<Object>(responseUser, HttpStatus.OK);
		}
		catch(AmazonClientException e )
		{
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,e.getMessage(), e);
		}
	}
	
	@RequestMapping(value = "/user",method=RequestMethod.PUT)
	public ResponseEntity<Object> updateUser(@RequestBody User user)
	{
		try{
			User responseUser = userServ.updateUser(user);
			return new ResponseEntity<Object>(responseUser, HttpStatus.OK);
		}
		catch(AmazonClientException e )
		{
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,e.getMessage(), e);
		}
	}
	
	@RequestMapping(value = "/user/{userId}",method=RequestMethod.DELETE)
	public ResponseEntity<Object> deleteUser(@PathVariable String userId)
	{
		try{
			userServ.deleteUser(userId);
			return new ResponseEntity<Object>(null, HttpStatus.CREATED);
		}
		catch(AmazonClientException e )
		{
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,e.getMessage(), e);
		}
	}
}
