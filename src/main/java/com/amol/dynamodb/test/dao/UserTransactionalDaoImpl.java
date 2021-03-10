package com.amol.dynamodb.test.dao;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMappingException;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTransactionLoadExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTransactionWriteExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.TransactionLoadRequest;
import com.amazonaws.services.dynamodbv2.datamodeling.TransactionWriteRequest;
import com.amazonaws.services.dynamodbv2.model.InternalServerErrorException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TransactionCanceledException;
import com.amol.dynamodb.test.domain.Role;
import com.amol.dynamodb.test.domain.User;

public class UserTransactionalDaoImpl implements UserTransactionalDao {

	@Autowired
	DynamoDBMapper dynamoDBMapper;

	private void executeTransactionWrite(TransactionWriteRequest txWriteRequest)
	{
		try
		{
			dynamoDBMapper.transactionWrite(txWriteRequest);
		}catch(DynamoDBMappingException dbe){
			System.err.println("Client side error in Mapper, fix before retrying. Error: " + dbe.getMessage());
		}catch (ResourceNotFoundException rnfe) {
            System.err.println("One of the tables was not found, verify table exists before retrying. Error: " + rnfe.getMessage());
        } catch (InternalServerErrorException ise) {
            System.err.println("Internal Server Error, generally safe to retry with back-off. Error: " + ise.getMessage());
        } catch (TransactionCanceledException tce) {
            System.err.println("Transaction Canceled, implies a client issue, fix before retrying. Error: " + tce.getMessage());
        } catch (Exception ex) {
            System.err.println("An exception occurred, investigate and configure retry strategy. Error: " + ex.getMessage());
        }
		
	}
	
	private List<Object> executeTransactionLoad(TransactionLoadRequest txLoadRequest)
	{
		List<Object> loadedObjects = new ArrayList<>();
		try
		{
			loadedObjects = dynamoDBMapper.transactionLoad(txLoadRequest);
		}catch(DynamoDBMappingException dbe){
			System.err.println("Client side error in Mapper, fix before retrying. Error: " + dbe.getMessage());
		}catch (ResourceNotFoundException rnfe) {
            System.err.println("One of the tables was not found, verify table exists before retrying. Error: " + rnfe.getMessage());
        } catch (InternalServerErrorException ise) {
            System.err.println("Internal Server Error, generally safe to retry with back-off. Error: " + ise.getMessage());
        } catch (TransactionCanceledException tce) {
            System.err.println("Transaction Canceled, implies a client issue, fix before retrying. Error: " + tce.getMessage());
        } catch (Exception ex) {
            System.err.println("An exception occurred, investigate and configure retry strategy. Error: " + ex.getMessage());
        }
		
		return loadedObjects;
	}
	
	@Override
	public void testPutAndUpdateInTransactionWrite() {
		User user = new User();
		user.setUserName("amold");
		user.setEmail("amold@test.com");
		user.setContact("1234567890");
		dynamoDBMapper.save(user);
		
		user.setContact("9898777722");
		User user2 = new User();
		user2.setUserName("abc");
		user2.setEmail("abc@test.com");
		user2.setContact("1234567890");
		
		TransactionWriteRequest txWriteReq = new TransactionWriteRequest();
		txWriteReq.addUpdate(user);
		txWriteReq.addPut(user2);
		executeTransactionWrite(txWriteReq);
	}

	@Override
	public void testPutWithConditionalUpdateInTransactionWrite() {
		User user = new User();
		user.setUserName("pqr");
		user.setEmail("pqr@test.com");
		
		Role role = new Role();
		role.setRoleId("1");
		role.setRoleName("ADMIN");
		DynamoDBTransactionWriteExpression txWriteExp = new DynamoDBTransactionWriteExpression().withConditionExpression("attribute_exists(role)");
		
		TransactionWriteRequest txWriteReq = new TransactionWriteRequest();
		txWriteReq.addPut(user);
		txWriteReq.addPut(role);
		user.setRole(role);
		txWriteReq.addUpdate(user, txWriteExp);
		executeTransactionWrite(txWriteReq);

	}

	@Override
	public void testPutWithConditionCheckInTransactionWrite() {
		User user1 = new User();
		user1.setUserName("tester");
		user1.setEmail("tester@test.com");
		user1.setContact("1122334455");
		
		User user2 = new User();
		user2.setUserName("test1");
		user2.setContact("7777788888");
		
		DynamoDBTransactionWriteExpression conditionalExp = new DynamoDBTransactionWriteExpression().withConditionExpression("attribute_exists(contact)");
		
		TransactionWriteRequest txWriteReq = new TransactionWriteRequest();
		txWriteReq.addPut(user1);
		txWriteReq.addConditionCheck(user2, conditionalExp);
		
		executeTransactionWrite(txWriteReq);
	}

	@Override
	public void testMixedOperationsInTransactionWrite() {
		// TODO Auto-generated method stub

	}

	@Override
	public void testTransactionLoadWithSave() {
	
		User user = new User();
		user.setUserName("xyz");
		user.setEmail("xyz@test.com");
		user.setContact("1111122222");
		dynamoDBMapper.save(user);
		
		Role role = new Role();
		role.setRoleId("2");
		role.setRoleName("SUPERADMIN");
		dynamoDBMapper.save(role);
		
		TransactionLoadRequest txLoadReq = new TransactionLoadRequest();
		txLoadReq.addLoad(role);
		
		DynamoDBTransactionLoadExpression txLoadExp = new DynamoDBTransactionLoadExpression().withProjectionExpression("userName, contact");
		txLoadReq.addLoad(user, txLoadExp);
		
		List<Object> loadedObjects = executeTransactionLoad(txLoadReq);
		Role loadedRole = (Role)loadedObjects.get(0);
		System.out.println("ROLE ID : " + loadedRole.getRoleId());
		System.out.println("ROLE NAME : " + loadedRole.getRoleName());
		
		
		User loadedUser = (User)loadedObjects.get(1);
		System.out.println("USER ID :" + loadedUser.getUserId());
		System.out.println("USER NAME :" + loadedUser.getUserName());
		System.out.println("USER EMAIL :" + loadedUser.getEmail());
		System.out.println("USER CONTACT :" + loadedUser.getContact());
	}

	@Override
	public void testTransactionLoadWithTransactionWrite() {
		// TODO Auto-generated method stub

	}

}
