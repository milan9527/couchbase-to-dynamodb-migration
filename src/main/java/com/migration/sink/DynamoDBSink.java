package com.migration.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.migration.config.ConfigLoader;
import com.migration.model.Document;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.net.URI;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Flink sink function that writes data to DynamoDB
 */
public class DynamoDBSink extends RichSinkFunction<Document> {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoDBSink.class);
    private static final long serialVersionUID = 1L;
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    private transient DynamoDbClient dynamoDbClient;
    private transient String tableName;
    private final AtomicInteger counter = new AtomicInteger(0);
    private final AtomicInteger batchCounter = new AtomicInteger(0);
    
    // Buffer for batch processing
    private final List<Document> buffer = new ArrayList<>();
    private int batchSize;
    private int maxRetries;
    private long retryBackoffMs;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        String region = ConfigLoader.getProperty("aws.region", "us-east-1");
        String endpoint = ConfigLoader.getProperty("aws.dynamodb.endpoint");
        tableName = ConfigLoader.getProperty("aws.dynamodb.table", "migration_target");
        batchSize = ConfigLoader.getIntProperty("dynamodb.batch.size", 25);
        maxRetries = ConfigLoader.getIntProperty("dynamodb.max.retries", 3);
        retryBackoffMs = ConfigLoader.getIntProperty("dynamodb.retry.backoff.ms", 1000);
        
        LOG.info("Initializing DynamoDB sink for region: {}, endpoint: {}, table: {}, batchSize: {}", 
                region, endpoint, tableName, batchSize);
        
        // Build DynamoDB client
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
        
        LOG.info("DynamoDB sink initialized, counter starting at: {}", counter.get());
    }
    
    @Override
    public void invoke(Document document, Context context) throws Exception {
        // Add document to buffer
        synchronized (buffer) {
            buffer.add(document);
            
            // Process batch if buffer reaches batch size
            if (buffer.size() >= batchSize) {
                processBatch();
            }
        }
    }
    
    private void processBatch() {
        if (buffer.isEmpty()) {
            return;
        }
        
        List<Document> batchToProcess;
        synchronized (buffer) {
            batchToProcess = new ArrayList<>(buffer);
            buffer.clear();
        }
        
        int batchId = batchCounter.incrementAndGet();
        int batchSize = batchToProcess.size();
        LOG.info("Processing batch #{} with {} documents", batchId, batchSize);
        
        int successCount = 0;
        for (Document document : batchToProcess) {
            boolean success = false;
            int attempts = 0;
            
            while (!success && attempts < maxRetries) {
                attempts++;
                try {
                    // Convert document to DynamoDB item
                    Map<String, AttributeValue> item = convertToItem(document);
                    
                    // Put item into DynamoDB
                    PutItemRequest putItemRequest = PutItemRequest.builder()
                            .tableName(tableName)
                            .item(item)
                            .build();
                    
                    dynamoDbClient.putItem(putItemRequest);
                    success = true;
                    successCount++;
                } catch (Exception e) {
                    if (attempts >= maxRetries) {
                        LOG.error("Failed to write document to DynamoDB after {} attempts: {}", 
                                attempts, document.getId(), e);
                    } else {
                        LOG.warn("Error writing document to DynamoDB (attempt {}/{}): {}", 
                                attempts, maxRetries, document.getId());
                        try {
                            // Exponential backoff
                            Thread.sleep(retryBackoffMs * attempts);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            LOG.warn("Retry sleep interrupted", ie);
                            break; // Exit retry loop if interrupted
                        }
                    }
                }
            }
        }
        
        // Update counter with successful writes
        int totalCount = counter.addAndGet(successCount);
        LOG.info("Batch #{} completed: {}/{} documents written successfully, total: {}", 
                batchId, successCount, batchSize, totalCount);
    }
    
    private Map<String, AttributeValue> convertToItem(Document document) {
        Map<String, AttributeValue> item = new HashMap<>();
        
        // Add document ID as the primary key
        item.put("id", AttributeValue.builder().s(document.getId()).build());
        
        // Convert JSON content to DynamoDB attributes
        JsonNode content = document.getContent();
        if (content.isObject()) {
            ObjectNode objectNode = (ObjectNode) content;
            Iterator<Map.Entry<String, JsonNode>> fields = objectNode.fields();
            
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String key = field.getKey();
                JsonNode value = field.getValue();
                
                // Skip if the field is the ID field to avoid duplication
                if ("id".equals(key) || "ID".equals(key)) {
                    continue;
                }
                
                // Convert JsonNode to AttributeValue based on its type
                AttributeValue attributeValue = convertJsonNodeToAttributeValue(value);
                if (attributeValue != null) {
                    item.put(key, attributeValue);
                }
            }
        }
        
        return item;
    }
    
    private AttributeValue convertJsonNodeToAttributeValue(JsonNode node) {
        if (node == null || node.isNull()) {
            return AttributeValue.builder().nul(true).build();
        } else if (node.isTextual()) {
            return AttributeValue.builder().s(node.asText()).build();
        } else if (node.isNumber()) {
            return AttributeValue.builder().n(node.asText()).build();
        } else if (node.isBoolean()) {
            return AttributeValue.builder().bool(node.asBoolean()).build();
        } else if (node.isObject()) {
            Map<String, AttributeValue> map = new HashMap<>();
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                AttributeValue value = convertJsonNodeToAttributeValue(field.getValue());
                if (value != null) {
                    map.put(field.getKey(), value);
                }
            }
            return AttributeValue.builder().m(map).build();
        } else if (node.isArray()) {
            return AttributeValue.builder().s(node.toString()).build();
        }
        
        // Default to string representation for unsupported types
        return AttributeValue.builder().s(node.toString()).build();
    }
    
    @Override
    public void close() throws Exception {
        // Process any remaining documents in the buffer
        processBatch();
        
        LOG.info("Closing DynamoDB sink, total documents written: {}", counter.get());
        if (dynamoDbClient != null) {
            dynamoDbClient.close();
        }
    }
}
