package com.migration;

import com.migration.config.ConfigLoader;
import com.migration.model.Document;
import com.migration.sink.DynamoDBSink;
import com.migration.source.CouchbaseSource;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Main class for the Couchbase to DynamoDB migration pipeline
 */
public class CouchbaseToDynamoDBMigration {
    private static final Logger LOG = LoggerFactory.getLogger(CouchbaseToDynamoDBMigration.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Couchbase to DynamoDB migration");
        
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Configure parallelism
        int parallelism = ConfigLoader.getIntProperty("migration.parallelism", 4);
        env.setParallelism(parallelism);
        
        // Set execution mode to STREAMING for long-running job
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        
        // Configure restart strategy - exponential backoff to handle transient failures
        env.setRestartStrategy(RestartStrategies.exponentialDelayRestart(
            Time.of(1, TimeUnit.SECONDS),  // initial backoff
            Time.of(60, TimeUnit.SECONDS), // max backoff
            3.0,                           // multiplier
            Time.of(5, TimeUnit.MINUTES),  // reset backoff threshold
            0.1                            // jitter factor (must be between 0 and 1)
        ));
        
        // Create Couchbase source
        CouchbaseSource couchbaseSource = new CouchbaseSource();
        
        // Create data stream from Couchbase
        DataStream<Document> documentStream = env
                .addSource(couchbaseSource)
                .name("couchbase-source")
                .uid("couchbase-source");
        
        // Add processing operators if needed
        // For example, you could add filtering, transformation, etc.
        
        // Write to DynamoDB sink
        documentStream
                .addSink(new DynamoDBSink())
                .name("dynamodb-sink")
                .uid("dynamodb-sink");
        
        LOG.info("Executing Flink job with parallelism: {}", parallelism);
        
        // Execute the Flink job
        env.execute("Couchbase to DynamoDB Migration");
    }
}
