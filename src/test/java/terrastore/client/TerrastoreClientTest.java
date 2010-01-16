/**
 * Copyright 2009 Sergio Bossa (sergio.bossa@gmail.com)
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package terrastore.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class TerrastoreClientTest {

    private static final TestValue TEST_VALUE_1 = new TestValue("value_1");
    private static final TestValue TEST_VALUE_2 = new TestValue("value_2");
    private static final TestValue TEST_VALUE_3 = new TestValue("value_3");

    @Test
    public void testAddThenRemoveBucket() throws Exception {
        TerrastoreClient client = new TerrastoreClient("http://localhost:8080");
        client.addBucket("bucket");
        client.removeBucket("bucket");
    }

    @Test(expected = TerrastoreRequestException.class)
    public void testRemoveBucketNotFoundThrowsException() throws Exception {
        TerrastoreClient client = new TerrastoreClient("http://localhost:8080");
        client.removeBucket("not_found");
    }

    @Test
    public void testAddThenGetBuckets() throws Exception {
        TerrastoreClient client = new TerrastoreClient("http://localhost:8080");
        client.addBucket("bucket1");
        client.addBucket("bucket2");
        //
        Set<String> buckets = client.getBuckets();
        assertEquals(2, buckets.size());
        assertTrue(buckets.contains("bucket1"));
        assertTrue(buckets.contains("bucket2"));
        //
        client.removeBucket("bucket1");
        client.removeBucket("bucket2");
    }

    @Test
    public void testPutAndRemoveValue() throws Exception {
        TerrastoreClient client = new TerrastoreClient("http://localhost:8080");
        client.addBucket("bucket");
        //
        client.<TestValue>putValue("bucket", "key1", TEST_VALUE_1);
        client.removeValue("bucket", "key1");
        //
        client.removeBucket("bucket");
    }

    @Test(expected = TerrastoreRequestException.class)
    public void testRemoveValueNotFoundThrowsException() throws Exception {
        TerrastoreClient client = new TerrastoreClient("http://localhost:8080");
        try {
            client.addBucket("bucket");
            //
            client.removeValue("bucket", "error");
        } finally {
            client.removeBucket("bucket");
        }
    }

    @Test
    public void testPutAndGetValue() throws Exception {
        TerrastoreClient client = new TerrastoreClient("http://localhost:8080");
        client.addBucket("bucket");
        //
        client.<TestValue>putValue("bucket", "key1", TEST_VALUE_1);
        TestValue value = client.<TestValue>getValue("bucket", "key1", TestValue.class);
        assertNotNull(value);
        assertEquals(TEST_VALUE_1, value);
        //
        client.removeBucket("bucket");
    }

    @Test(expected = TerrastoreRequestException.class)
    public void testGetValueNotFoundThrowsException() throws Exception {
        TerrastoreClient client = new TerrastoreClient("http://localhost:8080");
        try {
            client.addBucket("bucket");
            //
            client.<TestValue>getValue("bucket", "not_found", TestValue.class);
        } finally {
            client.removeBucket("bucket");
        }
    }

    @Test
    public void testGetAllValues() throws Exception {
        TerrastoreClient client = new TerrastoreClient("http://localhost:8080");
        client.addBucket("bucket");
        //
        client.<TestValue>putValue("bucket", "key1", TEST_VALUE_1);
        client.<TestValue>putValue("bucket", "key2", TEST_VALUE_2);
        client.<TestValue>putValue("bucket", "key3", TEST_VALUE_3);
        //
        Map<String, TestValue> map = client.<TestValue>getAllValues("bucket", TestValue.class);
        assertNotNull(map);
        assertEquals(3, map.size());
        assertTrue(map.containsKey("key1"));
        assertTrue(map.containsKey("key2"));
        assertTrue(map.containsKey("key3"));
        assertTrue(map.containsValue(TEST_VALUE_1));
        assertTrue(map.containsValue(TEST_VALUE_2));
        assertTrue(map.containsValue(TEST_VALUE_3));
        //
        client.removeBucket("bucket");
    }

    @Test
    public void testDoRangeQueryWithNoPredicate() throws Exception {
        TerrastoreClient client = new TerrastoreClient("http://localhost:8080");
        client.addBucket("bucket");
        //
        client.<TestValue>putValue("bucket", "key1", TEST_VALUE_1);
        client.<TestValue>putValue("bucket", "key2", TEST_VALUE_2);
        client.<TestValue>putValue("bucket", "key3", TEST_VALUE_3);
        //
        Map<String, TestValue> map = client.<TestValue>doRangeQuery("bucket", "key2", "key3", 0, "lexical-asc", null, 0, TestValue.class);
        assertNotNull(map);
        assertEquals(2, map.size());
        List<TestValue> values = new ArrayList<TestValue>(map.values());
        assertEquals(TEST_VALUE_2, values.get(0));
        assertEquals(TEST_VALUE_3, values.get(1));
        //
        client.removeBucket("bucket");
    }

    @Test
    public void testDoRangeQueryWithNoEndKey() throws Exception {
        TerrastoreClient client = new TerrastoreClient("http://localhost:8080");
        client.addBucket("bucket");
        //
        client.<TestValue>putValue("bucket", "key1", TEST_VALUE_1);
        client.<TestValue>putValue("bucket", "key2", TEST_VALUE_2);
        client.<TestValue>putValue("bucket", "key3", TEST_VALUE_3);
        //
        Map<String, TestValue> map = client.<TestValue>doRangeQuery("bucket", "key2", null, 0, "lexical-asc", null, 0, TestValue.class);
        assertNotNull(map);
        assertEquals(2, map.size());
        List<TestValue> values = new ArrayList<TestValue>(map.values());
        assertEquals(TEST_VALUE_2, values.get(0));
        assertEquals(TEST_VALUE_3, values.get(1));
        //
        client.removeBucket("bucket");
    }

    @Test
    public void testDoRangeQueryWithPredicate() throws Exception {
        TerrastoreClient client = new TerrastoreClient("http://localhost:8080");
        client.addBucket("bucket");
        //
        client.<TestValue>putValue("bucket", "key1", TEST_VALUE_1);
        client.<TestValue>putValue("bucket", "key2", TEST_VALUE_2);
        client.<TestValue>putValue("bucket", "key3", TEST_VALUE_3);
        //
        Map<String, TestValue> map = client.<TestValue>doRangeQuery("bucket", "key2", "key3", 0, "lexical-asc", "jxpath:/value", 0, TestValue.class);
        assertNotNull(map);
        assertEquals(2, map.size());
        List<TestValue> values = new ArrayList<TestValue>(map.values());
        assertEquals(TEST_VALUE_2, values.get(0));
        assertEquals(TEST_VALUE_3, values.get(1));
        //
        client.removeBucket("bucket");
    }

    @Test
    public void testDoPredicateQuery() throws Exception {
        TerrastoreClient client = new TerrastoreClient("http://localhost:8080");
        client.addBucket("bucket");
        //
        client.<TestValue>putValue("bucket", "key1", TEST_VALUE_1);
        client.<TestValue>putValue("bucket", "key2", TEST_VALUE_2);
        client.<TestValue>putValue("bucket", "key3", TEST_VALUE_3);
        //
        Map<String, TestValue> map = client.<TestValue>doPredicateQuery("bucket", "jxpath:/value", TestValue.class);
        assertNotNull(map);
        assertEquals(3, map.size());
        assertTrue(map.containsKey("key1"));
        assertTrue(map.containsKey("key2"));
        assertTrue(map.containsKey("key3"));
        assertTrue(map.containsValue(TEST_VALUE_1));
        assertTrue(map.containsValue(TEST_VALUE_2));
        assertTrue(map.containsValue(TEST_VALUE_3));
        //
        client.removeBucket("bucket");
    }

    @Test
    public void testExecuteUpdate() throws Exception {
        Map<String, Object> parameters = new HashMap<String, Object>();
        String param1 = "param1";
        String value1 = "value1";
        parameters.put(param1, value1);
        //
        TerrastoreClient client = new TerrastoreClient("http://localhost:8080");
        client.addBucket("bucket");
        //
        client.<TestValue>putValue("bucket", "key1", TEST_VALUE_1);
        //
        client.executeUpdate("bucket", "key1", "replace", 1000, new Parameters(parameters));
        //
        client.removeBucket("bucket");
    }

    public static class TestValue {

        private String value;

        public TestValue(String value) {
            this.value = value;
        }

        protected TestValue() {
        }

        public String getValue() {
            return value;
        }

        private void setValue(String value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object obj) {
            TestValue other = (TestValue) obj;
            return this.value.equals(other.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }
}
