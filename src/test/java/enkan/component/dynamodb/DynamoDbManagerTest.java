package enkan.component.dynamodb;

import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import enkan.Env;
import enkan.system.EnkanSystem;
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
            DynamoDbManager dynamo = system.getComponent("dynamoManager");
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
            DynamoDbManager dynamo = system.getComponent("dynamoManager");
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

    public static class TestBean implements Serializable {
        private String s;
        private int i;
        private boolean b;

        public TestBean() {
        }

        public String getS() {
            return this.s;
        }

        public int getI() {
            return this.i;
        }

        public boolean isB() {
            return this.b;
        }

        public void setS(String s) {
            this.s = s;
        }

        public void setI(int i) {
            this.i = i;
        }

        public void setB(boolean b) {
            this.b = b;
        }

        public boolean equals(Object o) {
            if (o == this) return true;
            if (!(o instanceof TestBean)) return false;
            final TestBean other = (TestBean) o;
            if (!other.canEqual((Object) this)) return false;
            final Object this$s = this.getS();
            final Object other$s = other.getS();
            if (this$s == null ? other$s != null : !this$s.equals(other$s)) return false;
            if (this.getI() != other.getI()) return false;
            if (this.isB() != other.isB()) return false;
            return true;
        }

        public int hashCode() {
            final int PRIME = 59;
            int result = 1;
            final Object $s = this.getS();
            result = result * PRIME + ($s == null ? 43 : $s.hashCode());
            result = result * PRIME + this.getI();
            result = result * PRIME + (this.isB() ? 79 : 97);
            return result;
        }

        protected boolean canEqual(Object other) {
            return other instanceof TestBean;
        }

        public String toString() {
            return "DynamoDbManagerTest.TestBean(s=" + this.getS() + ", i=" + this.getI() + ", b=" + this.isB() + ")";
        }
    }

}
