package enkan.component.dynamodb;

import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import enkan.Env;
import enkan.system.EnkanSystem;
import lombok.Data;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;

import static enkan.util.BeanBuilder.*;
import static org.junit.Assert.*;

public class DynamoDbManagerTest {
    private String accessKey;
    private String secretKey;

    @Before
    public void setupKeys() {
        accessKey = Env.get("AWS_ACCESS_KEY");
        secretKey = Env.get("AWS_SECRET_KEY");
    }

    @Test
    public void createTable() {
        Assume.assumeNotNull(accessKey, secretKey);
        Assume.assumeTrue(!accessKey.isEmpty() && !secretKey.isEmpty());
        EnkanSystem system = EnkanSystem.of("dynamoManager", builder(new DynamoDbManager())
                .set(DynamoDbManager::setAccessKey, accessKey)
                .set(DynamoDbManager::setSecretKey, secretKey)
                .build());
        try {
            system.start();
            DynamoDbManager dynamo = (DynamoDbManager) system.getComponent("dynamoManager");
            ListTablesResult res = dynamo.getClient().listTables();
            assertTrue(res.getTableNames().contains("EnkanKvs"));
        } finally {
            system.stop();
        }
    }

    @Test
    public void crud() {
        Assume.assumeNotNull(accessKey, secretKey);
        Assume.assumeTrue(!accessKey.isEmpty() && !secretKey.isEmpty());
        EnkanSystem system = EnkanSystem.of("dynamoManager", builder(new DynamoDbManager())
                .set(DynamoDbManager::setAccessKey, accessKey)
                .set(DynamoDbManager::setSecretKey, secretKey)
                .build());
        try {
            system.start();
            DynamoDbManager dynamo = (DynamoDbManager) system.getComponent("dynamoManager");
            DynamoDbStore store = dynamo.createDynamoDbStore();
            TestBean bean = new TestBean();
            bean.setS("ABC");
            bean.setB(true);
            bean.setI(123);
            store.write("key", bean);
            TestBean deserialized = (TestBean) store.read("key");
            assertNotNull(deserialized);
            assertEquals("ABC", deserialized.getS());
            assertEquals(true, deserialized.isB());
            assertEquals(123, deserialized.getI());

            store.delete("key");
            assertNull(store.read("key"));
        } finally {
            system.stop();
        }
    }

    @Data
    public static class TestBean implements Serializable {
        private String s;
        private int i;
        private boolean b;
    }

}
