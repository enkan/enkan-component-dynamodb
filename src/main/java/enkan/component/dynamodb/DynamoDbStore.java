package enkan.component.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;
import enkan.middleware.session.KeyValueStore;

import java.io.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * DynamoDB Store
 *
 * @author kawasima
 */
public class DynamoDbStore implements KeyValueStore {
    private final AmazonDynamoDB client;
    private final String tableName;
    private final Duration ttl;

    protected DynamoDbStore(AmazonDynamoDB client, String tableName, Duration ttl) {
        this.client = client;
        this.tableName = tableName;
        this.ttl = ttl;
    }

    @Override
    public Serializable read(String key) {
        GetItemResult res = client.getItem(tableName, Collections.singletonMap("id", new AttributeValue(key)));
        if (res.getItem() == null) {
            return null;
        }
        ByteBuffer buf = res.getItem().get("value").getB().asReadOnlyBuffer();
        byte[] value = new byte[buf.remaining()];
        buf.get(value, 0, buf.remaining());
        try(ByteArrayInputStream bais = new ByteArrayInputStream(value);
            ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (Serializable) ois.readObject();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (ClassNotFoundException e) {

        }
        return null;
    }

    @Override
    public String write(String key, Serializable value) {
        Map<String, AttributeValue> item = new HashMap<>(2);
        item.put("id", new AttributeValue(key));
        long expiry = LocalDateTime.now().plus(ttl).toInstant(ZoneOffset.UTC).toEpochMilli();
        item.put("ttl", new AttributeValue().withN(Long.toString(expiry)));
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)
        ) {
            oos.writeObject(value);
            ByteBuffer buf = ByteBuffer.wrap(baos.toByteArray());
            item.put("value", new AttributeValue().withB(buf));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        PutItemResult res = client.putItem(tableName, item);
        return key;
    }

    @Override
    public String delete(String key) {
        DeleteItemResult res = client.deleteItem(tableName, Collections.singletonMap("id", new AttributeValue(key)));
        return key;
    }
}
