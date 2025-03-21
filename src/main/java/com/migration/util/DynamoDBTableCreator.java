package com.migration.util;

import com.migration.config.ConfigLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class to create DynamoDB table if it doesn't exist
 */
public class DynamoDBTableCreator {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoDBTableCreator.class);

    public static void main(String[] args) {
        String region = ConfigLoader.getProperty("aws.region", "us-east-1");
        String endpoint = ConfigLoader.getProperty("aws.dynamodb.endpoint");
        String tableName = ConfigLoader.getProperty("aws.dynamodb.table", "migration_target");

        LOG.info("Creating DynamoDB table: {} in region: {}, endpoint: {}", 
                tableName, region, endpoint);

        // Build DynamoDB client
        DynamoDbClient dynamoDbClient;
        if (endpoint != null && !endpoint.isEmpty()) {
            // Use local DynamoDB endpoint if specified
            dynamoDbClient = DynamoDbClient.builder()
                    .endpointOverride(URI.create(endpoint))
                    .credentialsProvider(DefaultCredentialsProvider.create())
                    .region(Region.of(region))
                    .httpClientBuilder(ApacheHttpClient.builder())
                    .build();
        } else {
            // Use AWS DynamoDB
            dynamoDbClient = DynamoDbClient.builder()
                    .credentialsProvider(DefaultCredentialsProvider.create())
                    .region(Region.of(region))
                    .httpClientBuilder(ApacheHttpClient.builder())
                    .build();
        }

        try {
            // Check if table already exists
            boolean tableExists = tableExists(dynamoDbClient, tableName);
            
            if (tableExists) {
                LOG.info("Table {} already exists", tableName);
            } else {
                // Create table with id as the primary key
                createTable(dynamoDbClient, tableName);
                LOG.info("Table {} created successfully", tableName);
            }
        } catch (Exception e) {
            LOG.error("Error creating DynamoDB table", e);
        } finally {
            dynamoDbClient.close();
        }
    }

    private static boolean tableExists(DynamoDbClient dynamoDbClient, String tableName) {
        try {
            dynamoDbClient.describeTable(DescribeTableRequest.builder()
                    .tableName(tableName)
                    .build());
            return true;
        } catch (ResourceNotFoundException e) {
            return false;
        }
    }

    private static void createTable(DynamoDbClient dynamoDbClient, String tableName) {
        List<KeySchemaElement> keySchema = new ArrayList<>();
        keySchema.add(KeySchemaElement.builder()
                .attributeName("id")
                .keyType(KeyType.HASH)
                .build());

        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(AttributeDefinition.builder()
                .attributeName("id")
                .attributeType(ScalarAttributeType.S)
                .build());

        CreateTableRequest request = CreateTableRequest.builder()
                .tableName(tableName)
                .keySchema(keySchema)
                .attributeDefinitions(attributeDefinitions)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build();

        dynamoDbClient.createTable(request);

        // Wait for table to become active
        LOG.info("Waiting for table to become active...");
        dynamoDbClient.waiter().waitUntilTableExists(DescribeTableRequest.builder()
                .tableName(tableName)
                .build());
    }
}
