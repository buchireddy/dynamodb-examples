package org.buchi.dynamodb.examples;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.*;

import java.util.Collections;

/**
 * Created by buchi.busireddy on 1/3/15.
 */
public class DynamoMashup {
    static AmazonDynamoDBClient client = new AmazonDynamoDBClient(new ProfileCredentialsProvider());
    static DynamoDB dynamoDB = new DynamoDB(client);

    public static void main(String []args) {
        client.setEndpoint("http://localhost:8000");

        createTable("Test", 10L, 5L, "Id", "N");
        deleteTable("Test");
    }

    private static void createTable(String tableName,
                                    long readCapacityUnits, long writeCapacityUnits,
                                    String hashKeyName, String hashKeyType) {
        KeySchemaElement schemaElement = new KeySchemaElement()
                .withKeyType(KeyType.HASH).withAttributeName(hashKeyName);

        CreateTableRequest request = new CreateTableRequest()
                .withTableName(tableName)
                .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(readCapacityUnits).withWriteCapacityUnits(writeCapacityUnits))
                .withKeySchema(schemaElement);

        AttributeDefinition attributeDefinition = new AttributeDefinition()
                .withAttributeName(hashKeyName).withAttributeType(hashKeyType);
        request.setAttributeDefinitions(Collections.singleton(attributeDefinition));

        try {
            Table table = dynamoDB.createTable(request);
            System.out.println("Waiting for " + tableName
                    + " to be created...this may take a while...");
            table.waitForActive();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Done creating the table.");
    }

    private static void deleteTable(String tableName) {
        Table table = dynamoDB.getTable(tableName);
        if (table != null) {
            table.delete();
            try {
                table.waitForDelete();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Deleted the table.");
        }
    }
}
