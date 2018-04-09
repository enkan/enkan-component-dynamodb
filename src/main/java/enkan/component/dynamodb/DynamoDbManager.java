package enkan.component.dynamodb;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.waiters.Waiter;
import com.amazonaws.waiters.WaiterParameters;
import enkan.component.ComponentLifecycle;
import enkan.component.SystemComponent;
import enkan.exception.MisconfigurationException;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;

/**
 * System component for DynamoDB Client.
 *
 * @author kawasima
 */
public class DynamoDbManager extends SystemComponent {
    private String regionId = "us-east-1";

    private String endpoint;

    private File credentialsFile;

    private String accessKey;

    private String secretKey;

    private String tableName = "EnkanKvs";

    private Duration ttl = Duration.ofSeconds(3600);

    private long readCapacityUnits = 10;

    private long writeCapacityUnits = 5;

    private boolean createIfNotExist = true;

    private AmazonDynamoDB client;

    private AmazonDynamoDB createDynamoClient() {
        AmazonDynamoDBClientBuilder builder = AmazonDynamoDBClientBuilder.standard();
        if (regionId != null) {
            builder.withRegion(regionId);
        }

        if (endpoint != null) {
            builder.withEndpointConfiguration(
                    new AwsClientBuilder.EndpointConfiguration(endpoint, regionId)
            );
        }

        if (accessKey != null && secretKey != null) {
            builder.withCredentials(
                    new AWSStaticCredentialsProvider(
                            new BasicAWSCredentials(accessKey, secretKey)));
        }

        return builder.build();
    }

    @Override
    protected ComponentLifecycle<DynamoDbManager> lifecycle() {
        return new ComponentLifecycle<DynamoDbManager>() {
            @Override
            public void start(DynamoDbManager component) {
                client = createDynamoClient();
                Optional<String> table = client.listTables().getTableNames()
                        .stream()
                        .filter(s -> Objects.equals(s, tableName))
                        .findAny();
                if (!table.isPresent()) {
                    if (createIfNotExist) {
                        CreateTableResult res = client.createTable(
                                Arrays.asList(
                                        new AttributeDefinition("id", ScalarAttributeType.S),
                                        new AttributeDefinition("value", ScalarAttributeType.B)
                                ),
                                tableName,
                                Collections.singletonList(
                                        new KeySchemaElement("id", KeyType.HASH)
                                ),
                                new ProvisionedThroughput(readCapacityUnits, writeCapacityUnits));
                        Waiter<DescribeTableRequest> waiter = client.waiters().tableExists();
                        waiter.run(new WaiterParameters<>(new DescribeTableRequest(tableName)));
                        client.updateTimeToLive(new UpdateTimeToLiveRequest()
                                .withTableName(tableName)
                                .withTimeToLiveSpecification(new TimeToLiveSpecification()
                                        .withAttributeName("ttl")
                                        .withEnabled(true)));
                    } else {
                        throw new MisconfigurationException("dynamodb.TABLE_NOT_FOUND.problem", tableName);
                    }
                }
            }

            @Override
            public void stop(DynamoDbManager component) {
                client.shutdown();
            }
        };
    }

    public AmazonDynamoDB getClient() {
        return client;
    }

    public DynamoDbStore createDynamoDbStore() {
        return new DynamoDbStore(client, tableName, ttl);
    }

    /**
     * Create a DynamoDbStore with the name of table.
     *
     * @param tableName the name of DynamoDB table
     * @return DynamoDbStore
     */
    public DynamoDbStore createDynamoDbStore(String tableName) {
        return new DynamoDbStore(client, tableName, ttl);
    }

    /**
     * Create a DynamoDbStore with the name of table and TTL.
     *
     * @param tableName the name of DynamoDB table
     * @param ttl the duration of time-to-live
     * @return DynamoDbStore
     */
    public DynamoDbStore createDynamoDbStore(String tableName, Duration ttl) {
        return new DynamoDbStore(client, tableName, ttl);
    }

    public String getRegionId() {
        return this.regionId;
    }

    public String getEndpoint() {
        return this.endpoint;
    }

    public File getCredentialsFile() {
        return this.credentialsFile;
    }

    public String getAccessKey() {
        return this.accessKey;
    }

    public String getSecretKey() {
        return this.secretKey;
    }

    public String getTableName() {
        return this.tableName;
    }

    public Duration getTtl() {
        return this.ttl;
    }

    public long getReadCapacityUnits() {
        return this.readCapacityUnits;
    }

    public long getWriteCapacityUnits() {
        return this.writeCapacityUnits;
    }

    public boolean isCreateIfNotExist() {
        return this.createIfNotExist;
    }

    public void setRegionId(String regionId) {
        this.regionId = regionId;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public void setCredentialsFile(File credentialsFile) {
        this.credentialsFile = credentialsFile;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setTtl(Duration ttl) {
        this.ttl = ttl;
    }

    public void setReadCapacityUnits(long readCapacityUnits) {
        this.readCapacityUnits = readCapacityUnits;
    }

    public void setWriteCapacityUnits(long writeCapacityUnits) {
        this.writeCapacityUnits = writeCapacityUnits;
    }

    public void setCreateIfNotExist(boolean createIfNotExist) {
        this.createIfNotExist = createIfNotExist;
    }
}
