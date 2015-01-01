package org.buchi.dynamodb.examples;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.Page;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;

public class DocumentAPIQuery {

    static DynamoDB dynamoDB = new DynamoDB(new AmazonDynamoDBClient(new ProfileCredentialsProvider()));

    static String tableName = "Reply";

    public static void main(String[] args) throws Exception {

        String forumName = "Amazon DynamoDB";
        String threadSubject = "DynamoDB Thread 1";

        findRepliesForAThread(forumName, threadSubject);
        findRepliesForAThreadSpecifyOptionalLimit(forumName, threadSubject);
        findRepliesInLast15DaysWithConfig(forumName, threadSubject);
        findRepliesPostedWithinTimePeriod(forumName, threadSubject);
        findRepliesUsingAFilterExpression(forumName, threadSubject);
    }

    private static void findRepliesForAThread(String forumName, String threadSubject) {

        Table table = dynamoDB.getTable(tableName);

        String replyId = forumName + "#" + threadSubject;

        ItemCollection<QueryOutcome> items = table.query("Id", replyId);

        System.out.println("\nfindRepliesForAThread results:");

        Iterator<Item> iterator = items.iterator();
        while (iterator.hasNext()) {
            System.out.println(iterator.next().toJSONPretty());
        }

    }

    private static void findRepliesForAThreadSpecifyOptionalLimit(String forumName, String threadSubject) {

        Table table = dynamoDB.getTable(tableName);

        String replyId = forumName + "#" + threadSubject;
        QuerySpec spec = new QuerySpec().withHashKey("Id", replyId).withMaxPageSize(1);

        ItemCollection<QueryOutcome> items = table.query(spec);

        System.out.println("\nfindRepliesForAThreadSpecifyOptionalLimit results:");

        // Process each page of results
        int pageNum = 0;
        for (Page<Item, QueryOutcome> page : items.pages()) {

            System.out.println("\nPage: " + ++pageNum);

            // Process each item on the current page
            Iterator<Item> item = page.iterator();
            while (item.hasNext()) {
                System.out.println(item.next().toJSONPretty());
            }
        }
    }

    private static void findRepliesInLast15DaysWithConfig(String forumName, String threadSubject) {

        Table table = dynamoDB.getTable(tableName);

        long twoWeeksAgoMilli = (new Date()).getTime() - (15L*24L*60L*60L*1000L);
        Date twoWeeksAgo = new Date();
        twoWeeksAgo.setTime(twoWeeksAgoMilli);
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        String twoWeeksAgoStr = df.format(twoWeeksAgo);

        String replyId = forumName + "#" + threadSubject;

        RangeKeyCondition rangeKeyCondition = new RangeKeyCondition("ReplyDateTime").le(twoWeeksAgoStr);

        ItemCollection<QueryOutcome> items = table.query("Id", replyId,
                rangeKeyCondition,
                null, //FilterExpression - not used in this example
                "Message, ReplyDateTime, PostedBy", //ProjectionExpression
                null, //ExpressionAttributeNames - not used in this example
                null); //ExpressionAttributeValues - not used in this example

        System.out.println("\nfindRepliesInLast15DaysWithConfig results:");
        Iterator<Item> iterator = items.iterator();
        while (iterator.hasNext()) {
            System.out.println(iterator.next().toJSONPretty());
        }

    }

    private static void findRepliesPostedWithinTimePeriod(String forumName, String threadSubject) {

        Table table = dynamoDB.getTable(tableName);

        long startDateMilli = (new Date()).getTime() - (15L*24L*60L*60L*1000L);
        long endDateMilli = (new Date()).getTime() - (5L*24L*60L*60L*1000L);
        java.text.SimpleDateFormat df = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        String startDate = df.format(startDateMilli);
        String endDate = df.format(endDateMilli);

        String replyId = forumName + "#" + threadSubject;

        RangeKeyCondition rangeKeyCondition = new RangeKeyCondition("ReplyDateTime").between(startDate, endDate);

        ItemCollection<QueryOutcome> items = table.query("Id", replyId,
                rangeKeyCondition,
                null, //FilterExpression - not used in this example
                "Message, ReplyDateTime, PostedBy",  //ProjectionExpression
                null, //ExpressionAttributeNames - not used in this example
                null); //ExpressionAttributeValues - not used in this example

        System.out.println("\nfindRepliesPostedWithinTimePeriod results:");
        Iterator<Item> iterator = items.iterator();
        while (iterator.hasNext()) {
            System.out.println(iterator.next().toJSONPretty());
        }
    }


    private static void findRepliesUsingAFilterExpression(String forumName, String threadSubject) {

        Table table = dynamoDB.getTable(tableName);

        String replyId = forumName + "#" + threadSubject;

        Map<String, Object> expressionAttributeValues = new HashMap<String, Object>();
        expressionAttributeValues.put(":val", "User B");

        ItemCollection<QueryOutcome> items = table.query("Id", replyId,
                null, ///RangeKeyCondition - not used in this example
                "PostedBy = :val", //FilterExpression
                "Message, ReplyDateTime, PostedBy", //ProjectionExpression
                null, //ExpressionAttributeNames - not used in this example
                expressionAttributeValues);

        System.out.println("\nfindRepliesUsingAFilterExpression results:");
        Iterator<Item> iterator = items.iterator();
        while (iterator.hasNext()) {
            System.out.println(iterator.next().toJSONPretty());
        }
    }

}