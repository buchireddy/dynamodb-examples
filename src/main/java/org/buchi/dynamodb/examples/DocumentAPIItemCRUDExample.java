package org.buchi.dynamodb.examples;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;

/**
 * Copied from http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/JavaDocumentAPICRUDExample.html
 * for illustration purposes.
 */
public class DocumentAPIItemCRUDExample {

    static AmazonDynamoDBClient client = new AmazonDynamoDBClient(
            new ProfileCredentialsProvider());
    static DynamoDB dynamoDB = new DynamoDB(client);

    static String tableName = "ProductCatalog";

    public static void main(String[] args) throws IOException {

        client.setEndpoint("http://localhost:8000");
        createItems();
        retrieveItem();

        // Perform various updates.
        updateMultipleAttributes();
        updateAddNewAttribute();
        updateExistingAttributeConditionally();

        // Delete the item.
        deleteItem();
    }

    private static void createItems() {

        Table table = dynamoDB.getTable(tableName);
        try {

            Item item = new Item()
                    .withPrimaryKey("Id", 120)
                    .withString("Title", "Book 120 Title")
                    .withString("ISBN", "120-1111111111")
                    .withStringSet(
                            "Authors",
                            new HashSet<String>(Arrays.asList("Author12",
                                    "Author22"))).withNumber("Price", 20)
                    .withString("Dimensions", "8.5x11.0x.75")
                    .withNumber("PageCount", 500)
                    .withBoolean("InPublication", false)
                    .withString("ProductCategory", "Book");
            table.putItem(item);

            item = new Item()
                    .withPrimaryKey("Id", 121)
                    .withString("Title", "Book 121 Title")
                    .withString("ISBN", "121-1111111111")
                    .withStringSet(
                            "Authors",
                            new HashSet<String>(Arrays.asList("Author21",
                                    "Author22"))).withNumber("Price", 20)
                    .withString("Dimensions", "8.5x11.0x.75")
                    .withNumber("PageCount", 500)
                    .withBoolean("InPublication", true)
                    .withString("ProductCategory", "Book");
            table.putItem(item);

        } catch (Exception e) {
            System.err.println("Create items failed.");
            System.err.println(e.getMessage());

        }
    }

    private static void retrieveItem() {
        Table table = dynamoDB.getTable(tableName);

        try {

            Item item = table.getItem("Id", 120, "Id, ISBN, Title, Authors", null);

            System.out.println("Printing item after retrieving it....");
            System.out.println(item.toJSONPretty());

        } catch (Exception e) {
            System.err.println("GetItem failed.");
            System.err.println(e.getMessage());
        }

    }

    private static void updateAddNewAttribute() {
        Table table = dynamoDB.getTable(tableName);

        try {

            Map<String, Object> expressionAttributeValues = new HashMap<String, Object>();
            expressionAttributeValues.put(":val1", "Some value");

            UpdateItemSpec updateItemSpec = new UpdateItemSpec()
                    .withPrimaryKey("Id", 121)
                    .withUpdateExpression("set NewAttribute = :val1")
                    .withValueMap(expressionAttributeValues)
                    .withReturnValues(ReturnValue.ALL_NEW);

            UpdateItemOutcome outcome = table.updateItem(updateItemSpec);

            // Check the response.
            System.out.println("Printing item after adding new attribute...");
            System.out.println(outcome.getItem().toJSONPretty());

        } catch (Exception e) {
            System.err.println("Failed to add new attribute in " + tableName);
            System.err.println(e.getMessage());
        }
    }

    private static void updateMultipleAttributes() {

        Table table = dynamoDB.getTable(tableName);

        try {

            Map<String, Object> expressionAttributeValues = new HashMap<String, Object>();
            expressionAttributeValues
                    .put(":val1",
                            new HashSet<String>(Arrays.asList("Author YY",
                                    "Author ZZ")));
            expressionAttributeValues.put(":val2", "someValue");

            UpdateItemSpec updateItemSpec = new UpdateItemSpec()
                    .withPrimaryKey("Id", 120)
                    .withUpdateExpression(
                            "add Authors :val1 set NewAttribute=:val2")
                    .withValueMap(expressionAttributeValues)
                    .withReturnValues(ReturnValue.ALL_NEW);

            UpdateItemOutcome outcome = table.updateItem(updateItemSpec);

            // Check the response.
            System.out
                    .println("Printing item after multiple attribute update...");
            System.out.println(outcome.getItem().toJSONPretty());

        } catch (Exception e) {
            System.err.println("Failed to update multiple attributes in "
                    + tableName);
            System.err.println(e.getMessage());

        }
    }

    private static void updateExistingAttributeConditionally() {

        Table table = dynamoDB.getTable(tableName);

        try {

            // Specify the desired price (25.00) and also the condition (price =
            // 20.00)

            Map<String, Object> expressionAttributeValues = new HashMap<String, Object>();
            expressionAttributeValues.put(":val1", 25);
            expressionAttributeValues.put(":val2", 20);

            UpdateItemSpec updateItemSpec = new UpdateItemSpec()
                    .withPrimaryKey("Id", 120)
                    .withReturnValues(ReturnValue.ALL_NEW)
                    .withUpdateExpression("set Price = :val1")
                    .withConditionExpression("Price = :val2")
                    .withValueMap(expressionAttributeValues);

            UpdateItemOutcome outcome = table.updateItem(updateItemSpec);

            // Check the response.
            System.out
                    .println("Printing item after conditional update to new attribute...");
            System.out.println(outcome.getItem().toJSONPretty());

        } catch (Exception e) {
            System.err.println("Error updating item in " + tableName);
            System.err.println(e.getMessage());
        }
    }

    private static void deleteItem() {

        Table table = dynamoDB.getTable(tableName);

        try {

            Map<String, Object> expressionAttributeValues = new HashMap<String, Object>();
            expressionAttributeValues.put(":val", false);

            DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
                    .withPrimaryKey("Id", 120)
                    .withConditionExpression("InPublication = :val")
                    .withValueMap(expressionAttributeValues)
                    .withReturnValues(ReturnValue.ALL_OLD);

            DeleteItemOutcome outcome = table.deleteItem(deleteItemSpec);

            // Check the response.
            System.out.println("Printing item that was deleted...");
            System.out.println(outcome.getItem().toJSONPretty());

        } catch (Exception e) {
            System.err.println("Error deleting item in " + tableName);
            System.err.println(e.getMessage());
        }
    }
}