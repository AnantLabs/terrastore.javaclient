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

package terrastore.client.integrationtest;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import terrastore.client.BucketOperation;
import terrastore.client.KeyOperation;
import terrastore.client.TerrastoreClient;
import terrastore.client.TerrastoreClientException;
import terrastore.client.TerrastoreRequestException;


/**
 * @author Sergio Bossa
 * @author Sven Johansson
 */
public class TerrastoreClientIntegrationTest {

    private static final TestValue TEST_VALUE_1 = new TestValue("value_1");
    private static final TestValue TEST_VALUE_2 = new TestValue("value_2");
    private static final TestValue TEST_VALUE_3 = new TestValue("value_3");
	
	private TerrastoreClient client;

	@Before
	public void setUp() throws Exception {
		client = new TerrastoreClient("http://localhost:8080");
	}
	
	@Test
	public void testAddThenRemoveBucket() throws Exception {
		client.bucket("bucket").key("value").put(TEST_VALUE_1);
		client.bucket("bucket").remove();
	}
	
	@Test
	public void testRemoveNonExistantBucket() throws Exception {
		client.bucket("not_found").remove();
	}
	
	@Test
	public void testAddThenGetBuckets() throws TerrastoreClientException {
		BucketOperation bucket1 = client.bucket("bucket1");
		BucketOperation bucket2 = client.bucket("bucket2");
		
		bucket1.key("value").put(TEST_VALUE_1);
		bucket2.key("value").put(TEST_VALUE_1);
		
		Set<String> buckets = client.buckets().list();
		assertEquals(2, buckets.size());
		assertTrue(buckets.contains("bucket1"));
		assertTrue(buckets.contains("bucket2"));
		
		bucket1.remove();
		bucket2.remove();
	}
	
	@Test
	public void testPutAndRemoveValue() throws TerrastoreClientException {
		BucketOperation bucket = client.bucket("bucket");
		
		bucket.key("key1").put(TEST_VALUE_1);
		bucket.key("key1").remove();
		
		bucket.remove();
	}
	
	@Test
    public void testRemoveValueNotFound() throws Exception {
		client.bucket("bucket").key("not_found").remove();
	}
	
	@Test
	public void testAndGetValue() throws TerrastoreClientException {
		BucketOperation bucket = client.bucket("bucket");
		
		bucket.key("key1").put(TEST_VALUE_1);
		TestValue value = bucket.key("key1").get(TestValue.class);
		assertNotNull(value);
		assertEquals(TEST_VALUE_1, value);
		
		bucket.remove();
	}
	
    @Test(expected = TerrastoreRequestException.class)
    public void testGetValueNotFoundFromExistingBucketThrowsException() throws Exception {
    	BucketOperation bucket = client.bucket("bucket");
    	bucket.key("value").put(TEST_VALUE_1);
    	
    	try {
    		bucket.key("not_found").get(TestValue.class);
    	} finally {
    		bucket.remove();
    	}
    }
    
    @Test(expected = TerrastoreRequestException.class)
    public void testGetValueNotFoundFromNonExistingBucketThrowsException() throws Exception {
        BucketOperation bucket = client.bucket("bucket");
        
        try {
            bucket.key("not_found").get(TestValue.class);
        } finally {
            bucket.remove();
        }
    }
    
    @Test
    public void testGetAllValuesWithNoLimit() throws Exception {
    	
    	client.bucket("bucket").key("key1").put(TEST_VALUE_1);
    	client.bucket("bucket").key("key2").put(TEST_VALUE_2);
    	client.bucket("bucket").key("key3").put(TEST_VALUE_3);
    	
    	Map<String, TestValue> values = client.bucket("bucket").get(TestValue.class);  
    	assertNotNull(values);
    	assertEquals(3, values.size());
    	assertTrue(values.containsKey("key1"));
    	assertTrue(values.containsKey("key2"));
    	assertTrue(values.containsKey("key3"));
    	assertTrue(values.containsValue(TEST_VALUE_1));
    	assertTrue(values.containsValue(TEST_VALUE_2));
    	assertTrue(values.containsValue(TEST_VALUE_3));
    	
    	client.bucket("bucket").remove();
    }
    
    @Test
    public void testGetAllValuesWithLimit() throws Exception {
    	
    	client.bucket("bucket").key("key1").put(TEST_VALUE_1);
    	client.bucket("bucket").key("key2").put(TEST_VALUE_2);
    	
    	Map<String, TestValue> values = client.bucket("bucket").limit(1).get(TestValue.class);
    	
        assertNotNull(values);
        assertEquals(1, values.size());
        assertTrue(values.containsKey("key1") || values.containsKey("key2"));
        assertTrue(values.containsValue(TEST_VALUE_1) || values.containsValue(TEST_VALUE_2));

        client.bucket("bucket").remove();
    }
    
    @Test
    public void testDoRangeQueryWithNoPredicate() throws Exception {
    	BucketOperation bucket = client.bucket("bucket");
    	
    	bucket.key("key1").put(TEST_VALUE_1);
    	bucket.key("key2").put(TEST_VALUE_2);
    	bucket.key("key3").put(TEST_VALUE_3);
    	
    	Map<String, TestValue> result = bucket.range().from("key2").to("key3").get(TestValue.class);
    	
        assertNotNull(result);
        assertEquals(2, result.size());
        List<TestValue> values = new ArrayList<TestValue>(result.values());
        assertEquals(TEST_VALUE_2, values.get(0));
        assertEquals(TEST_VALUE_3, values.get(1));

    	bucket.remove();
    }
    
    @Test
    public void testDoRangeQueryWithNoEndKey() throws Exception {
    	BucketOperation bucket = client.bucket("bucket");
    	
    	bucket.key("key1").put(TEST_VALUE_1);
    	bucket.key("key2").put(TEST_VALUE_2);
    	bucket.key("key3").put(TEST_VALUE_3);
    	
    	Map<String, TestValue> map = bucket.range().from("key2").get(TestValue.class);
    	
        assertNotNull(map);
        assertEquals(2, map.size());
        List<TestValue> values = new ArrayList<TestValue>(map.values());
        assertEquals(TEST_VALUE_2, values.get(0));
        assertEquals(TEST_VALUE_3, values.get(1));

        bucket.remove();
    }
    
    @Test
    public void testDoRangeQueryWithPredicate() throws Exception {
    	BucketOperation bucket = client.bucket("bucket");
    	
    	bucket.key("key1").put(TEST_VALUE_1);
    	bucket.key("key2").put(TEST_VALUE_2);
    	bucket.key("key3").put(TEST_VALUE_3);
    	
    	Map<String, TestValue> result = bucket.range("lexical-asc")
    		.from("key2")
    		.to("key3")
    		.conditionally("jxpath:/value")
    		.get(TestValue.class);
    	
        assertNotNull(result);
        assertEquals(2, result.size());
        List<TestValue> values = new ArrayList<TestValue>(result.values());
        assertEquals(TEST_VALUE_2, values.get(0));
        assertEquals(TEST_VALUE_3, values.get(1));
        
        bucket.remove();
    }
    
    @Test
    public void testDoRangeQueryWithPredicateWhenNoKeysExists() throws Exception {
        BucketOperation bucket = client.bucket("bucket");
  
        Map<String, TestValue> result = bucket.range("lexical-asc")
            .from("key2")
            .to("key3")
            .conditionally("jxpath:/value")
            .get(TestValue.class);
        
        assertNotNull(result);
        assertEquals(0, result.size());
        
        bucket.remove();
    }
    
    @Test
    public void testDoPredicateQuery() throws Exception {
    	BucketOperation bucket = client.bucket("bucket");
    	
    	bucket.key("key1").put(TEST_VALUE_1);
    	bucket.key("key2").put(TEST_VALUE_2);
    	bucket.key("key3").put(TEST_VALUE_3);
    	
    	Map<String, TestValue> result = bucket.conditional("jxpath:/value").get(TestValue.class);
    	
		assertNotNull(result);
		assertEquals(3, result.size());
		assertTrue(result.containsKey("key1"));
		assertTrue(result.containsKey("key2"));
		assertTrue(result.containsKey("key3"));
		assertTrue(result.containsValue(TEST_VALUE_1));
		assertTrue(result.containsValue(TEST_VALUE_2));
		assertTrue(result.containsValue(TEST_VALUE_3));
		
		bucket.remove();
    }
    
    @Test
    public void testDoPredicateQueryWhenNoKeysExists() throws Exception {
        BucketOperation bucket = client.bucket("bucket");
        
        Map<String, TestValue> result = bucket.conditional("jxpath:/value").get(TestValue.class);
        
        assertNotNull(result);
        assertEquals(0, result.size());
        
        bucket.remove();
    }

    @Test
    public void testExecuteUpdate() throws Exception {
        Map<String, Object> parameters = new HashMap<String, Object>();
        String param1 = "param1";
        String value1 = "value1";
        parameters.put(param1, value1);
    	
        KeyOperation key = client.bucket("bucket").key("key1");
        key.put(TEST_VALUE_1);
        
        key.update().timeOut(1000L).parameters(parameters).execute();
        
        client.bucket("bucket").remove();
    }
    
    @Test
    public void testExportImportBackup() throws Exception {
        BucketOperation bucket = client.bucket("bucket");
        
        bucket.key("key1").put(TEST_VALUE_1);
        bucket.key("key2").put(TEST_VALUE_2);
        bucket.key("key3").put(TEST_VALUE_3);

        assertEquals(3, bucket.get(TestValue.class).size());
        
        bucket.backup().file("test.bak").secretKey("SECRET-KEY").executeExport();

        bucket.remove();
        assertEquals(0, bucket.get(TestValue.class).size());        
        
        bucket.backup().file("test.bak").secretKey("SECRET-KEY").executeImport();
        assertEquals(3, bucket.get(TestValue.class).size());
        
        bucket.remove();
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
            return this.value.equals(other.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }

}
