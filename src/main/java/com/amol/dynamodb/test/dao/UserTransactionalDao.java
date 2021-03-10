package com.amol.dynamodb.test.dao;

public interface UserTransactionalDao {

	void testPutAndUpdateInTransactionWrite();
	void testPutWithConditionalUpdateInTransactionWrite();
	void testPutWithConditionCheckInTransactionWrite();
	void testMixedOperationsInTransactionWrite();
	void testTransactionLoadWithSave();
	void testTransactionLoadWithTransactionWrite();
}
