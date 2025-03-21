package com.migration.source;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.migration.config.ConfigLoader;
import com.migration.model.Document;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Flink source function that reads data from Couchbase
 */
public class CouchbaseSource extends RichParallelSourceFunction<Document> implements ResultTypeQueryable<Document> {
    
    private static final Logger LOG = LoggerFactory.getLogger(CouchbaseSource.class);
    private static final long serialVersionUID = 1L;
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final AtomicLong currentOffset = new AtomicLong(0);
    private final AtomicLong totalProcessed = new AtomicLong(0);
    
    private transient Cluster cluster;
    private transient Bucket bucket;
    private transient Collection collection;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        String host = ConfigLoader.getProperty("couchbase.host", "localhost");
        String username = ConfigLoader.getProperty("couchbase.username", "Administrator");
        String password = ConfigLoader.getProperty("couchbase.password", "password");
        String bucketName = ConfigLoader.getProperty("couchbase.bucket", "default");
        String scopeName = ConfigLoader.getProperty("couchbase.scope", "_default");
        String collectionName = ConfigLoader.getProperty("couchbase.collection", "_default");
        
        LOG.info("Connecting to Couchbase at {}", host);
        
        cluster = Cluster.connect(
                host,
                username,
                password
        );
        
        // Set timeout for query operations
        cluster.environment().timeoutConfig().kvTimeout(Duration.ofSeconds(60));
        cluster.environment().timeoutConfig().queryTimeout(Duration.ofSeconds(300));
        
        bucket = cluster.bucket(bucketName);
        bucket.waitUntilReady(Duration.ofSeconds(30));
        collection = bucket.scope(scopeName).collection(collectionName);
        
        LOG.info("Connected to Couchbase bucket: {}, scope: {}, collection: {}", 
                bucketName, scopeName, collectionName);
        
        // Log the current offset
        LOG.info("Starting from offset: {}", currentOffset.get());
    }
    
    @Override
    public void run(SourceContext<Document> ctx) throws Exception {
        // Get configuration values
        String bucketName = ConfigLoader.getProperty("couchbase.bucket", "default");
        int batchSize = ConfigLoader.getIntProperty("migration.batch.size", 1000);
        
        // Continue processing as long as the job is running
        while (isRunning.get()) {
            // Build query with offset and limit for pagination
            String baseQuery = ConfigLoader.getProperty("couchbase.query", "SELECT * FROM `" + bucketName + "`");
            
            // Remove any existing LIMIT or OFFSET clauses
            String queryWithoutLimitOffset = baseQuery.replaceAll("(?i)\\s+LIMIT\\s+\\d+", "")
                                                     .replaceAll("(?i)\\s+OFFSET\\s+\\d+", "");
            
            // Add pagination
            String query = queryWithoutLimitOffset + 
                          " LIMIT " + batchSize + 
                          " OFFSET " + currentOffset.get();
            
            LOG.info("Executing Couchbase query: {}", query);
            
            try {
                // Create query options with the bucket context
                QueryOptions options = QueryOptions.queryOptions()
                    .timeout(Duration.ofSeconds(300));
                
                // Execute the query
                QueryResult result = cluster.query(query, options);
                
                int count = 0;
                for (JsonObject row : result.rowsAsObject()) {
                    if (!isRunning.get()) {
                        break;
                    }
                    
                    try {
                        // Extract document ID and content
                        String id = null;
                        
                        // Try different ways to get the ID
                        if (row.containsKey("id")) {
                            id = row.getString("id");
                        } else if (row.containsKey("ID")) {
                            id = row.getString("ID");
                        } else if (row.containsKey("META")) {
                            JsonObject meta = row.getObject("META");
                            if (meta != null && meta.containsKey("id")) {
                                id = meta.getString("id");
                            }
                        } else if (row.containsKey("meta")) {
                            JsonObject meta = row.getObject("meta");
                            if (meta != null && meta.containsKey("id")) {
                                id = meta.getString("id");
                            }
                        }
                        
                        // If we still don't have an ID, generate one
                        if (id == null) {
                            id = "doc-" + totalProcessed.get();
                            LOG.warn("Document without ID found, generating ID: {}", id);
                        }
                        
                        // Convert JsonObject to JsonNode
                        JsonNode content = objectMapper.readTree(row.toString());
                        
                        // Create document and emit to downstream operators
                        Document document = new Document(id, content);
                        ctx.collect(document);
                        
                        count++;
                        totalProcessed.incrementAndGet();
                    } catch (Exception e) {
                        LOG.error("Error processing document: {}", row, e);
                    }
                }
                
                // Update the offset for the next batch
                currentOffset.addAndGet(count);
                
                LOG.info("Processed batch of {} documents, total: {}, next offset: {}", 
                        count, totalProcessed.get(), currentOffset.get());
                
                // If we got fewer results than the batch size, we've reached the end
                // Wait before trying again to avoid hammering the database
                if (count < batchSize) {
                    LOG.info("Reached end of data (got {} records, batch size is {}). Waiting before next attempt...", 
                            count, batchSize);
                    Thread.sleep(60000); // Wait for 1 minute before checking for new data
                }
            } catch (Exception e) {
                LOG.error("Error executing Couchbase query", e);
                // Wait before retrying to avoid hammering the database in case of errors
                Thread.sleep(10000);
            }
        }
    }
    
    @Override
    public void cancel() {
        LOG.info("Cancelling Couchbase source");
        isRunning.set(false);
    }
    
    @Override
    public void close() throws Exception {
        LOG.info("Closing Couchbase connections");
        if (cluster != null) {
            cluster.disconnect();
        }
    }
    
    @Override
    public TypeInformation<Document> getProducedType() {
        return TypeInformation.of(Document.class);
    }
}
