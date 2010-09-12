/**
 * Copyright 2009 - 2010 Sergio Bossa (sergio.bossa@gmail.com)
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
package terrastore.client.test.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;

import org.junit.Before;
import org.junit.Test;

import terrastore.client.BucketOperation;
import terrastore.client.ClusterStats;
import terrastore.client.KeyOperation;
import terrastore.client.TerrastoreClient;
import terrastore.client.TerrastoreClientException;
import terrastore.client.connection.KeyNotFoundException;
import terrastore.client.connection.TerrastoreConnectionException;
import terrastore.client.connection.UnsatisfiedConditionException;
import terrastore.client.connection.resteasy.HTTPConnectionFactory;

/**
 * @author Sergio Bossa
 * @author Sven Johansson
 */
public class TerrastoreClientIntegrationTest {

    private static final TestValue TEST_VALUE_1 = new TestValue("value_1");
    private static final TestValue TEST_VALUE_2 = new TestValue("value_2");
    private static final TestValue TEST_VALUE_3 = new TestValue("value_3");
    private static final TestValue TEST_VALUE_NULL = new TestValue(null);
    
    private static final TupleTestValue TUPLE_TEST_VALUE_1 = new TupleTestValue("value_1-1", "value_1-2");
    private static final TupleTestValue TUPLE_TEST_VALUE_2 = new TupleTestValue("value_2-1", "value_1-2");
    
    private TerrastoreClient client;
    private BucketOperation bucket;
    private BucketOperation bucket1;
    private BucketOperation bucket2;

    @Before
    public void setUp() throws Exception {
        client = new TerrastoreClient("http://localhost:8080", new HTTPConnectionFactory());
        bucket = client.bucket("bucket");
        bucket1 = client.bucket("bucket1");
        bucket2 = client.bucket("bucket2");
    }

    @After
    public void tearDown() throws Exception {
        bucket.remove();
        bucket1.remove();
        bucket2.remove();
        Thread.sleep(1000);
    }

    @Test
    public void testClusterStats() throws Exception {
        ClusterStats stats = client.stats().cluster();
        assertNotNull(stats);
    }

    @Test
    public void testBuckets() throws TerrastoreClientException {
        bucket1.key("value").put(TEST_VALUE_1);
        bucket2.key("value").put(TEST_VALUE_1);

        Set<String> buckets = client.buckets().list();
        assertEquals(2, buckets.size());
        assertTrue(buckets.contains("bucket1"));
        assertTrue(buckets.contains("bucket2"));
    }

    @Test
    public void testRemoveNonExistantBucket() throws Exception {
        client.bucket("not_found").remove();
    }

    @Test
    public void testPutAndGetValue() throws TerrastoreClientException {
        bucket.key("key1").put(TEST_VALUE_1);
        TestValue value = bucket.key("key1").get(TestValue.class);
        assertNotNull(value);
        assertEquals(TEST_VALUE_1, value);
    }

    @Test
    public void testPutAndRemoveValue() throws TerrastoreClientException {
        bucket.key("key1").put(TEST_VALUE_1);
        bucket.key("key1").remove();
    }

    @Test
    public void testRemoveValueNotFound() throws Exception {
        bucket.key("not_found").remove();
    }

    @Test
    public void testConditionallyPutValue() throws Exception {
        bucket.key("key1").put(TEST_VALUE_1);
        bucket.key("key1").conditional("jxpath:/value").put(TEST_VALUE_2);
        assertEquals(TEST_VALUE_2, bucket.key("key1").get(TestValue.class));
    }

    @Test(expected = UnsatisfiedConditionException.class)
    public void testConditionallyPutValueThrowsExceptionDueToUnsatisfiedCondition() throws Exception {
        bucket.key("key1").put(TEST_VALUE_1);
            bucket.key("key1").conditional("jxpath:/notFound").put(TEST_VALUE_2);
    }

    @Test
    public void testConditionallyGetValue() throws Exception {
        bucket.key("key1").put(TEST_VALUE_1);
        assertEquals(TEST_VALUE_1, bucket.key("key1").conditional("jxpath:/value").get(TestValue.class));
    }

    @Test(expected = UnsatisfiedConditionException.class)
    public void testConditionallyGetValueThrowsExceptionDueToUnsatisfiedCondition() throws Exception {
        bucket.key("key1").put(TEST_VALUE_1);
            bucket.key("key1").conditional("jxpath:/notFound").get(TestValue.class);
    }

    @Test(expected = KeyNotFoundException.class)
    public void testGetValueNotFoundFromExistingBucketThrowsException() throws Exception {
        bucket.key("value").put(TEST_VALUE_1);
            bucket.key("not_found").get(TestValue.class);
    }

    @Test(expected = KeyNotFoundException.class)
    public void testGetValueNotFoundFromNonExistingBucketThrowsException() throws Exception {
            bucket.key("not_found").get(TestValue.class);
    }

    @Test
    public void testGetAllValuesWithNoLimit() throws Exception {
        bucket.key("key1").put(TEST_VALUE_1);
        bucket.key("key2").put(TEST_VALUE_2);
        bucket.key("key3").put(TEST_VALUE_3);

        /**
         * Sleep needed for operations comprising multiple keys, to allow the cluster propagate keys information.
         */
        Thread.sleep(1000);
        //

        Map<String, TestValue> values = client.bucket("bucket").values().get(TestValue.class);
        assertNotNull(values);
        assertEquals(3, values.size());
        assertTrue(values.containsKey("key1"));
        assertTrue(values.containsKey("key2"));
        assertTrue(values.containsKey("key3"));
        assertTrue(values.containsValue(TEST_VALUE_1));
        assertTrue(values.containsValue(TEST_VALUE_2));
        assertTrue(values.containsValue(TEST_VALUE_3));
    }

    @Test
    public void testGetAllValuesWithLimit() throws Exception {
        bucket.key("key1").put(TEST_VALUE_1);
        bucket.key("key2").put(TEST_VALUE_2);

        /**
         * Sleep needed for operations comprising multiple keys, to allow the cluster propagate keys information.
         */
        Thread.sleep(1000);
        //

        Map<String, TestValue> values = client.bucket("bucket").values().limit(1).get(TestValue.class);

        assertNotNull(values);
        assertEquals(1, values.size());
        assertTrue(values.containsKey("key1") || values.containsKey("key2"));
        assertTrue(values.containsValue(TEST_VALUE_1) || values.containsValue(TEST_VALUE_2));
    }

    @Test
    public void testDoRangeQueryWithNoPredicate() throws Exception {
        bucket.key("key1").put(TEST_VALUE_1);
        bucket.key("key2").put(TEST_VALUE_2);
        bucket.key("key3").put(TEST_VALUE_3);

        /**
         * Sleep needed for operations comprising multiple keys, to allow the cluster propagate keys information.
         */
        Thread.sleep(1000);
        //

        Map<String, TestValue> result = bucket.range().from("key2").to("key3").get(TestValue.class);

        assertNotNull(result);
        assertEquals(2, result.size());
        List<TestValue> values = new ArrayList<TestValue>(result.values());
        assertEquals(TEST_VALUE_2, values.get(0));
        assertEquals(TEST_VALUE_3, values.get(1));
    }

    @Test
    public void testDoRangeQueryWithNoEndKey() throws Exception {
        bucket.key("key1").put(TEST_VALUE_1);
        bucket.key("key2").put(TEST_VALUE_2);
        bucket.key("key3").put(TEST_VALUE_3);

        /**
         * Sleep needed for operations comprising multiple keys, to allow the cluster propagate keys information.
         */
        Thread.sleep(1000);
        //

        Map<String, TestValue> map = bucket.range().from("key2").get(TestValue.class);

        assertNotNull(map);
        assertEquals(2, map.size());
        List<TestValue> values = new ArrayList<TestValue>(map.values());
        assertEquals(TEST_VALUE_2, values.get(0));
        assertEquals(TEST_VALUE_3, values.get(1));
    }

    @Test
    public void testDoRangeQueryWithPredicate() throws Exception {
        bucket.key("key1").put(TEST_VALUE_1);
        bucket.key("key2").put(TEST_VALUE_2);
        bucket.key("key3").put(TEST_VALUE_3);

        /**
         * Sleep needed for operations comprising multiple keys, to allow the cluster propagate keys information.
         */
        Thread.sleep(1000);
        //

        Map<String, TestValue> result = bucket.range("lexical-asc").from("key2").to("key3").predicate("jxpath:/value").get(TestValue.class);

        assertNotNull(result);
        assertEquals(2, result.size());
        List<TestValue> values = new ArrayList<TestValue>(result.values());
        assertEquals(TEST_VALUE_2, values.get(0));
        assertEquals(TEST_VALUE_3, values.get(1));
    }

    @Test
    public void testDoRangeQueryWithPredicateWhenNoKeysExists() throws Exception {
        Map<String, TestValue> result = bucket.range("lexical-asc").from("key2").to("key3").predicate("jxpath:/value").get(TestValue.class);
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    @Test
    public void testDoPredicateQuery() throws Exception {
        bucket.key("key1").put(TEST_VALUE_1);
        bucket.key("key2").put(TEST_VALUE_2);
        bucket.key("key3").put(TEST_VALUE_3);

        /**
         * Sleep needed for operations comprising multiple keys, to allow the cluster propagate keys information.
         */
        Thread.sleep(1000);
        //

        Map<String, TestValue> result = bucket.predicate("jxpath:/value").get(TestValue.class);

        assertNotNull(result);
        assertEquals(3, result.size());
        assertTrue(result.containsKey("key1"));
        assertTrue(result.containsKey("key2"));
        assertTrue(result.containsKey("key3"));
        assertTrue(result.containsValue(TEST_VALUE_1));
        assertTrue(result.containsValue(TEST_VALUE_2));
        assertTrue(result.containsValue(TEST_VALUE_3));
    }

    @Test
    public void testDoPredicateQueryWhenNoKeysExists() throws Exception {
        Map<String, TestValue> result = bucket.predicate("jxpath:/value").get(TestValue.class);
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    @Test
    public void testExecuteReplaceUpdate() throws Exception {
        Map<String, Object> parameters = new HashMap<String, Object>();
        String param1 = "param1";
        String value1 = "value1";
        parameters.put(param1, value1);

        KeyOperation key = bucket.key("key1");

        key.put(TEST_VALUE_1);

        assertEquals(TEST_VALUE_2, key.update("replace").timeOut(1000L).parameters(getAsMap(TEST_VALUE_2)).executeAndGet(TestValue.class));
    }

    @Test
    public void testExecuteReplaceUpdateWithNoParameters() throws Exception {
        KeyOperation key = bucket.key("key1");

        key.put(TEST_VALUE_1);

        assertEquals(TEST_VALUE_NULL, key.update("replace").timeOut(1000L).executeAndGet(TestValue.class));
    }
    
    @Test
    public void testExecuteMergeUpdate() throws Exception {
        Map<String, Object> parameters = new HashMap<String, Object>();
        parameters.put("value1", "value_2-1");

        KeyOperation key = bucket.key("key1");
        key.put(TUPLE_TEST_VALUE_1);
        
        assertEquals(TUPLE_TEST_VALUE_2, key.update("merge").parameters(parameters).timeOut(1000L).executeAndGet(TupleTestValue.class));
    }
    
    @Test
    public void testExecuteMergeUpdateWithNoParameters() throws Exception {
        KeyOperation key = bucket.key("key1");
        key.put(TUPLE_TEST_VALUE_1);
        
        assertEquals(TUPLE_TEST_VALUE_1, key.update("merge").timeOut(1000L).executeAndGet(TupleTestValue.class));
    }
    
    @Test
    public void testExecuteAtomicCounterUpdate() throws Exception {
        Map<String, Object> parameters = new HashMap<String, Object>();
        parameters.put("value", "15");

        KeyOperation key = bucket.key("key1");
        key.put(new TestValue("10"));
        
        assertEquals(new TestValue("25"), key.update("counter").timeOut(1000L).parameters(parameters).executeAndGet(TestValue.class));
    }

    @Test
    public void testExecuteJsFunction() throws Exception {
        Map<String, Object> params = new HashMap<String, Object>();
        String f = "function update(key, value, params) {"
                + "   if(value['value'] == 'value_1') value['value'] = 'value_2';"
                + "   return value;"
                + "}";
        params.put("update", f);

        KeyOperation key = bucket.key("key1");
        key.put(TEST_VALUE_1);

        assertEquals(TEST_VALUE_2, key.update("js").timeOut(1000L).parameters(params).executeAndGet(TestValue.class));
    }

    @Test
    public void testExportImportBackup() throws Exception {
        bucket.key("key1").put(TEST_VALUE_1);
        bucket.key("key2").put(TEST_VALUE_2);
        bucket.key("key3").put(TEST_VALUE_3);

        assertEquals(3, bucket.values().get(TestValue.class).size());

        bucket.backup().file("test.bak").secretKey("SECRET-KEY").executeExport();

        bucket.remove();
        assertEquals(0, bucket.values().get(TestValue.class).size());

        bucket.backup().file("test.bak").secretKey("SECRET-KEY").executeImport();
        assertEquals(3, bucket.values().get(TestValue.class).size());
    }
    
    @Test(expected=TerrastoreConnectionException.class)
    public void testUnableToReachServer() throws Exception {
        client = new TerrastoreClient("localhost:9999", new HTTPConnectionFactory());
        client.bucket("bucket").key("value").put(TEST_VALUE_1);
    }

    private Map getAsMap(TestValue value) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.<Map>readValue(mapper.writeValueAsString(value), Map.class);
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

        @SuppressWarnings("unused")
        private void setValue(String value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object obj) {
            TestValue other = (TestValue) obj;
            return value != null ? this.value.equals(other.value) : true;
        }

        @Override
        public int hashCode() {
            return value != null ? value.hashCode() : 0;
        }

        @Override
        public String toString() {
            return "TestValue [value=" + value + "]";
        }
    }
    
    public static class TupleTestValue {
        
        private String value1;
        private String value2;
        
        public TupleTestValue(String value1, String value2) {
            this.value1 = value1;
            this.value2 = value2;
        }
        
        protected TupleTestValue() {
        }

        public String getValue1() {
            return value1;
        }

        @SuppressWarnings("unused")
        private void setValue1(String value1) {
            this.value1 = value1;
        }

        public String getValue2() {
            return value2;
        }

        @SuppressWarnings("unused")
        private void setValue2(String value2) {
            this.value2 = value2;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result
                    + ((value1 == null) ? 0 : value1.hashCode());
            result = prime * result
                    + ((value2 == null) ? 0 : value2.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            TupleTestValue other = (TupleTestValue) obj;
            if (value1 == null) {
                if (other.value1 != null)
                    return false;
            } else if (!value1.equals(other.value1))
                return false;
            if (value2 == null) {
                if (other.value2 != null)
                    return false;
            } else if (!value2.equals(other.value2))
                return false;
            return true;
        }
    }
}
