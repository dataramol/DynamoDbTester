package com.amol.dynamodb.test.enhancedclient;

import java.net.URI;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;
import static java.util.Collections.singletonMap;
import static software.amazon.awssdk.enhanced.dynamodb.internal.AttributeValues.stringValue;
import com.amol.dynamodb.test.enhancedclient.pojo.Customer;
import com.amol.dynamodb.test.enhancedclient.pojo.Order;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbIndex;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Expression;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class DBQueryManager {
	
		private static DynamoDbEnhancedClient enhancedClient;
		private static DynamoDbTable<Customer> custTable;
		private static DynamoDbTable<Order> orderTable;
		
		public static void main(String[] args) {
			Region region = Region.US_WEST_2;
			
			DynamoDbClient ddb = DynamoDbClient.builder().region(region).endpointDiscoveryEnabled(true).endpointOverride(URI.create("http://localhost:8000/")).credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("key", "key1"))).build();
			enhancedClient = DynamoDbEnhancedClient.builder().dynamoDbClient(ddb).build();
			custTable =  enhancedClient.table("Customer", TableSchema.fromBean(Customer.class));
			orderTable = enhancedClient.table("Order", TableSchema.fromBean(Order.class));
			//putBatchRecords();
			//getItem();
			//putItem();
			//queryTable();
			//scanTable();
			//queryIndex();
			//txSinglePut();
			//txMultiplePut();
			//txSingleUpdate();
			txSingleConditionCheck();
			ddb.close();
		}
		
		public static void txSinglePut()
		{
			Customer cust = new Customer();
			cust.setId("3");
			cust.setName("tester");
			cust.setEmail("tester@test.com");
			cust.setRegDate(LocalDate.parse("2021-03-09").atStartOfDay().toInstant(ZoneOffset.UTC));
			enhancedClient.transactWriteItems(TransactWriteItemsEnhancedRequest.builder().addPutItem(custTable, cust).build());
			
			System.out.println("RESULT OF CUSTOMER EQUALS : " + cust.equals(custTable.getItem(Key.builder().partitionValue("3").sortValue("tester").build())));
		}
		
		public static void txMultiplePut()
		{
			Customer cust1 = new Customer();
			cust1.setId("4");
			cust1.setName("tester123");
			cust1.setEmail("tester123@test.com");
			cust1.setRegDate(LocalDate.parse("2021-03-09").atStartOfDay().toInstant(ZoneOffset.UTC));
			
			Customer cust2 = new Customer();
			cust2.setId("5");
			cust2.setName("tester1234");
			cust2.setEmail("tester1234@test.com");
			cust2.setRegDate(LocalDate.parse("2021-03-09").atStartOfDay().toInstant(ZoneOffset.UTC));
			
			enhancedClient.transactWriteItems(TransactWriteItemsEnhancedRequest.builder().addPutItem(custTable, cust1).addPutItem(custTable, cust2).build());
			
			System.out.println("RESULT OF CUSTOMER 1 EQUALS :" + cust1.equals(custTable.getItem(Key.builder().partitionValue("4").sortValue("tester123").build())));
			System.out.println("RESULT OF CUSTOMER 2 EQUALS :" + cust2.equals(custTable.getItem(Key.builder().partitionValue("5").sortValue("tester1234").build())));
		}
		
		public static void txSingleUpdate()
		{
			Customer cust  = custTable.getItem(Key.builder().partitionValue("4").sortValue("tester123").build());
			cust.setEmail("tester12345@test.com");
			
			enhancedClient.transactWriteItems(TransactWriteItemsEnhancedRequest.builder().addUpdateItem(custTable, cust).build());
			System.out.println(custTable.getItem(Key.builder().partitionValue("4").sortValue("tester123").build()));
		}
		
		public static void txSingleConditionCheck()
		{
			Expression conditionExpression = Expression.builder().expression("#attribute = :attribute").expressionValues(singletonMap(":attribute", stringValue("tester12345@test.com"))).expressionNames(singletonMap("#attribute", "email")).build();
			
			Key key = Key.builder().partitionValue("4").sortValue("tester123").build();
			enhancedClient.transactWriteItems(
		            TransactWriteItemsEnhancedRequest.builder()
		                                             .addConditionCheck(custTable, ConditionCheck.builder()
		                                                                                            .key(key)
		                                                                                            .conditionExpression(conditionExpression)
		                                                                                            .build()).build());
		}
		
		
		public static void txMixOperations()
		{

			LocalDate localDate = LocalDate.parse("2021-03-10");
			LocalDateTime localDateTime = localDate.atStartOfDay();
			Instant instant  = localDateTime.toInstant(ZoneOffset.UTC);

			Order order4 = new Order();
			order4.setOrderDate(instant);
			order4.setOrderId("3");
			order4.setOrderNumber("100");
			order4.setOrderPaymentTotal("1000");

			Order order5 = new Order();
			order5.setOrderDate(instant);
			order5.setOrderId("4");
			order5.setOrderNumber("101");
			order5.setOrderPaymentTotal("1000");

			Order order3 = orderTable.getItem(Key.builder().partitionValue("1").sortValue(LocalDate.parse("2021-03-08").atStartOfDay().toInstant(ZoneOffset.UTC).toString());
			order3.setOrderNumber("200");

			Expression conditionExpression = Expression.builder().expression("#attribute = :attribute").expressionValues(singletonMap(":attribute", stringValue("100"))).expressionNames(singletonMap("#attribute", "orderNumber")).build();

			Key key = Key.builder().partitionValue("3").build();

			TransactWriteItemsEnhancedRequest transactWriteItemsEnhancedRequest =  TransactWriteItemsEnhancedRequest.builder().
					addConditionCheck(orderTable,ConditionCheck.builder().conditionExpression(conditionExpression).build())
					.addPutItem(orderTable,order4).addPutItem(orderTable,order5).addUpdateItem(orderTable,order3).build();
			enhancedClient.transactWriteItems(transactWriteItemsEnhancedRequest);
		}
		
		public static void queryIndex()
		{
			DynamoDbIndex<Order> orderIndex = enhancedClient.table("Order", TableSchema.fromBean(Order.class)).index("order_payment");
			SdkIterable<Page<Order>> result = orderIndex.scan();
			AtomicInteger atomicInteger = new AtomicInteger();
			atomicInteger.set(0);
			result.forEach(page -> {
				System.out.println(page.items().get(atomicInteger.get()));
				atomicInteger.incrementAndGet();
				
			});
		}
		
		public static void scanTable()
		{
			custTable.scan().items().iterator().forEachRemaining(System.out::println);
		}
		
		public static void queryTable()
		{
			QueryConditional queryConditional = QueryConditional.keyEqualTo(Key.builder().partitionValue("1").build());
			custTable.query(queryConditional).items().iterator().forEachRemaining(System.out::println);
		}
		
		public static void putItem()
		{
			try{
				Customer cust3 = new Customer();
				cust3.setId("1");
				cust3.setName("Atul Datar");
				cust3.setRegDate(LocalDate.parse("2021-03-08").atStartOfDay().toInstant(ZoneOffset.UTC));
				cust3.setEmail("atuld@test.com");
				PutItemEnhancedRequest<Customer> putItemEnhancedReq = PutItemEnhancedRequest.builder(Customer.class).item(cust3).build();
				custTable.putItem(putItemEnhancedReq);
			}
			catch(Exception e)
			{
				System.out.println("EXCEPTION ------> "+ e.getMessage());
				e.printStackTrace();
			}
			
			System.out.println("Record Added Successfully.....");
		}
		
		public static void getItem()
		{
			
						
			//gives error without sort key
			GetItemEnhancedRequest getItemEnhancedReq = GetItemEnhancedRequest.builder().key(Key.builder().partitionValue("1").sortValue("Amol Datar").build()).build();
			Customer customer = custTable.getItem(getItemEnhancedReq);
			System.out.println(customer);
		}
		
		public static void putBatchRecords()
		{
			//custTable.createTable();
			//orderTable.createTable();
			LocalDate localDate = LocalDate.parse("2021-03-08");
			LocalDateTime localDateTime = localDate.atStartOfDay();
			Instant instant  = localDateTime.toInstant(ZoneOffset.UTC);
			
			Order order1 = new Order();
			order1.setOrderDate(instant);
			order1.setOrderId("1");
			order1.setOrderNumber("100");
			order1.setOrderPaymentTotal("1000");
			
			Order order2 = new Order();
			order2.setOrderDate(instant);
			order2.setOrderId("2");
			order2.setOrderNumber("101");
			order2.setOrderPaymentTotal("1000");
			/*
			Customer cust1 = new Customer();
			cust1.setId("1");
			cust1.setName("ABC");
			cust1.setEmail("abc@test.com");
			cust1.setRegDate(instant);
			
			Customer cust2 = new Customer();
			cust2.setId("2");
			cust2.setName("xyz");
			cust2.setEmail("xyz@test.com");
			cust2.setRegDate(instant);
			
			
			BatchWriteItemEnhancedRequest batchWriteEnhancedReq = BatchWriteItemEnhancedRequest.builder().writeBatches(WriteBatch.builder(Customer.class)
					.mappedTableResource(custTable).addPutItem(cust1).
					addPutItem(cust2).build()).build();
			
			*/
			BatchWriteItemEnhancedRequest batchWriteEnhancedReq = BatchWriteItemEnhancedRequest.builder().writeBatches(WriteBatch.builder(Order.class).mappedTableResource(orderTable).addPutItem(order1).addPutItem(order2).build()).build();
			enhancedClient.batchWriteItem(batchWriteEnhancedReq);
			System.out.println("Batch Write Completed....");
			
		}
	
	
	
}
