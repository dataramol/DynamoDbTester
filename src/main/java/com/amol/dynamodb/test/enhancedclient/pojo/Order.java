package com.amol.dynamodb.test.enhancedclient.pojo;

import java.time.Instant;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondaryPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondarySortKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

@DynamoDbBean
public class Order {

	private String orderId;
	private String orderPaymentTotal;
	private Instant orderDate;
	private String orderNumber;
	
	@DynamoDbPartitionKey
	public String getOrderId() {
		return orderId;
	}
	public void setOrderId(String orderId) {
		this.orderId = orderId;
	}
	@DynamoDbSecondaryPartitionKey(indexNames=""
			+ "")
	public String getOrderPaymentTotal() {
		return orderPaymentTotal;
	}
	public void setOrderPaymentTotal(String orderPaymentTotal) {
		this.orderPaymentTotal = orderPaymentTotal;
	}
	
	@DynamoDbSortKey
	public Instant getOrderDate() {
		return orderDate;
	}
	public void setOrderDate(Instant orderDate) {
		this.orderDate = orderDate;
	}
	
	@DynamoDbSecondarySortKey(indexNames="order_payment")
	public String getOrderNumber() {
		return orderNumber;
	}
	public void setOrderNumber(String orderNumber) {
		this.orderNumber = orderNumber;
	}
	@Override
	public String toString() {
		return "Order [orderId=" + orderId + ", orderPaymentTotal=" + orderPaymentTotal + ", orderDate=" + orderDate
				+ ", orderNumber=" + orderNumber + "]";
	}
	
	
}
