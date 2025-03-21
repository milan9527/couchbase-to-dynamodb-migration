# Couchbase to DynamoDB Migration Tool

This project implements a data migration pipeline from Couchbase to Amazon DynamoDB using Apache Flink.

## Prerequisites

- Java 11 or higher
- Maven
- Couchbase Server 7.6.6 running locally
- Apache Flink 1.16.2
- AWS account with DynamoDB access or DynamoDB Local

## Project Structure

```
couchbase-to-dynamodb-migration/
├── src/
│   └── main/
│       ├── java/
│       │   └── com/
│       │       └── migration/
│       │           ├── config/
│       │           │   └── ConfigLoader.java
│       │           ├── model/
│       │           │   └── Document.java
│       │           ├── sink/
│       │           │   └── DynamoDBSink.java
│       │           ├── source/
│       │           │   └── CouchbaseSource.java
│       │           ├── util/
│       │           │   └── DynamoDBTableCreator.java
│       │           └── CouchbaseToDynamoDBMigration.java
│       └── resources/
│           ├── application.properties
│           └── log4j2.xml
└── pom.xml
```

## Configuration

Edit the `src/main/resources/application.properties` file to configure:

- Couchbase connection details
- DynamoDB connection details
- Migration parameters

```properties
# Couchbase Configuration
couchbase.host=localhost
couchbase.username=Administrator
couchbase.password=password
couchbase.bucket=default
couchbase.scope=_default
couchbase.collection=_default
couchbase.query=SELECT * FROM `default`

# AWS DynamoDB Configuration
aws.region=us-east-1
aws.dynamodb.table=migration_target
aws.dynamodb.endpoint=http://localhost:8000

# Migration Configuration
migration.batch.size=100
migration.parallelism=4
```

## Running the Migration

### 1. Create DynamoDB Table

Before running the migration, create the target DynamoDB table:

```bash
mvn compile exec:java -Dexec.mainClass="com.migration.util.DynamoDBTableCreator"
```

### 2. Run the Migration

```bash
mvn compile exec:java -Dexec.mainClass="com.migration.CouchbaseToDynamoDBMigration"
```

## Running in IntelliJ IDEA

1. Import the project as a Maven project
2. Configure your AWS credentials in `~/.aws/credentials` or set environment variables
3. Run the `DynamoDBTableCreator` class to create the DynamoDB table
4. Run the `CouchbaseToDynamoDBMigration` class to start the migration

## Customization

- Modify the Couchbase query in `application.properties` to select specific documents
- Adjust the parallelism settings for better performance
- Implement additional transformations in the Flink pipeline

## Notes

- The migration reads all documents from Couchbase using the configured query
- Documents are converted to DynamoDB format and written to the target table
- The document ID from Couchbase is used as the primary key in DynamoDB
- Complex data types are handled appropriately during conversion
