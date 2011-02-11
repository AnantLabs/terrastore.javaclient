/**
 * Copyright 2009 - 2011 Sergio Bossa (sergio.bossa@gmail.com)
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
package terrastore.client.connection.resteasy;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import terrastore.client.TerrastoreClient;
import terrastore.client.TerrastoreClientException;
import terrastore.client.TerrastoreRequestException;
import terrastore.client.connection.NoSuchKeyException;
import terrastore.client.connection.MapReduceQueryException;
import terrastore.client.connection.TerrastoreConnectionException;
import terrastore.client.mapreduce.MapReduceQuery;
import terrastore.client.test.pojostest.Address;
import terrastore.client.test.pojostest.PhoneNumber;
import terrastore.server.EmbeddedServerWrapper;
import terrastore.client.mapreduce.MapReduceQuery.Range;
import terrastore.client.mapreduce.MapReduceQuery.Task;

/**
 * @author Sven Johansson
 */
public class HTTPConnectionExceptionTest {

    private static final PhoneNumber TEST_PHONENUMBER = new PhoneNumber("123", "home");

    private TerrastoreClient client;

    private static EmbeddedServerWrapper server;
    
    @BeforeClass
    public static void startEmbeddedServer() throws Exception {
        server = new EmbeddedServerWrapper();
    }
    
    @AfterClass
    public static void stopEmbeddedServer() throws Exception {
        server.stop();
    }
    
    @Before
    public void setUp() {
        client = new TerrastoreClient("http://localhost:8080", new HTTPConnectionFactory());
        client.bucket("bucket").clear();
    }
    
    @Test
    public void testClearBucket_no_server() {
        try {
            client = new TerrastoreClient("http://localhost:7646", new HTTPConnectionFactory());
            client.bucket("bucket").clear();
            fail("An exception was expected");
        } catch (Exception e) {
            verifyTerrastoreConnectionException(e);
        }
    }

    @Test
    public void testPutValue_no_value() {
        try {
            client.bucket("bucket").key("illegal/value").put(null);
            fail("An exception was expected");
        } catch (Exception e) {
            verifyTerrastoreClientException(e);
        }
    }
    
    @Test
    public void testGetValue_incompatible_java_type() {
        try {
            client.bucket("bucket").key("value").put(TEST_PHONENUMBER);
            client.bucket("bucket").key("value").get(Address.class);
            fail("An exception was expected");
        } catch (Exception e) {
            verifyTerrastoreClientException(e);
        } 
    }
    
    @Test
    public void testMapReduce_no_task() {
        try {
            MapReduceQuery query = new MapReduceQuery()
                .range(new Range().from("aaaa").to("cccc"));
            
           client.bucket("bucket").mapReduce(query).execute(String.class);
           fail("An exception was expected");
        } catch (Exception e) {
            verifyException(MapReduceQueryException.class, e, 400);
        }
    }
    
    @Test
    public void testMapReduce_no_timeout() {
        try {
            MapReduceQuery query = new MapReduceQuery()
                .task(new Task().mapper("size").reducer("size"));
            
            client.bucket("bucket").mapReduce(query).execute(String.class);
            fail("An exception was expected");
        } catch (Exception e) {
            verifyException(MapReduceQueryException.class, e, 400);
        }
    }

    @Test
    public void testMapReduce_no_mapper() {
        try {
            MapReduceQuery query = new MapReduceQuery()
                .task(new Task().reducer("size").timeout(10000));
            
            client.bucket("bucket").mapReduce(query).execute(String.class);
            fail("An exception was expected");
        } catch (Exception e) {
            verifyException(MapReduceQueryException.class, e, 400);
        }
    }
    
    @Test
    public void testMapReduce_no_reducer() {
        try {
            MapReduceQuery query = new MapReduceQuery()
                .task(new Task().mapper("size").timeout(10000));
            
            client.bucket("bucket").mapReduce(query).execute(String.class);
            fail("An exception was expected");
        } catch (Exception e) {
            verifyException(MapReduceQueryException.class, e, 400);
        }
    }
    
    @Test
    public void testConditionalPut_invalid_condition() {
        try {
            client.bucket("bucket").key("value").conditional("Three blind mice").put(TEST_PHONENUMBER);
            fail("An exception was expected");
        } catch (Exception e) {
            verifyTerrastoreRequestException(e, 400);
        }
    }
    
    @Test
    public void testConditionalGet_invalid_condition() {
        try {
            client.bucket("bucket").key("value").conditional("Quick brown fox").get(PhoneNumber.class);
            fail("An exception was expected");
        } catch (Exception e) {
            verifyTerrastoreRequestException(e, 400);
        }
    }

    @Test
    public void testGetAllValues_incompatible_java_type() {
        try {
            client.bucket("bucket").key("value").put(TEST_PHONENUMBER);
            client.bucket("bucket").values().get(Address.class);
            fail("An exception was expected");
        } catch (Exception e) {
            verifyTerrastoreClientException(e);
        }
    }
    
    @Test
    public void testQueryByRange_no_range() {
        try {
            client.bucket("bucket").range().get(PhoneNumber.class);
            fail("An exception was expected");
        } catch (Exception e) {
            verifyTerrastoreClientException(e);
        }
    }
    
    @Test
    public void testQueryByRange_invalid_comparator() {
        try {
            client.bucket("bucket").range("flippety-flop").from("aaaa").get(PhoneNumber.class);
            fail("An exception was expected");
        } catch (Exception e) {
            verifyTerrastoreRequestException(e);
        }
    }
    
    @Test
    public void testQueryByRange_invalid_predicate_condition() {
        try {
            client.bucket("bucket").range().from("aaaa").predicate("A day in the life...").get(PhoneNumber.class);
            fail("An exception was expected");
        } catch (Exception e) {
            verifyTerrastoreRequestException(e, 400);
        }
    }
    
    @Test
    public void testQueryByPredicate_invalid_predicate() {
        try {
            client.bucket("bucket").predicate("We, the people...").get(PhoneNumber.class);
            fail("An exception was expected");
        } catch (Exception e) {
            verifyTerrastoreRequestException(e, 400);
        }
    }
    
    @Test
    public void testExportBackup_incorrect_secret_key() {
        try {
            client.bucket("bucket").backup().secretKey("abcdefghij").file("bucket.bak").executeExport();
            fail("An exception was expected");
        } catch (Exception e) {
            verifyTerrastoreRequestException(e, 400);
        }
    }
    
    @Test
    public void testExportBackup_invalid_file_name() {
        try {
            client.bucket("bucket").backup().secretKey("SECRET-KEY").file("???/???.??").executeExport();
            fail("An exception was expected");
        } catch (Exception e) {
            verifyTerrastoreRequestException(e, 500);
        }
    }
    
    @Test
    public void testImportBackup_incorrect_secret_key() {
        try {
            client.bucket("bucket").backup().secretKey("gfergoij").file("bucket.bak").executeImport();
            fail("An exception was expected");
        } catch (Exception e) {
            verifyTerrastoreRequestException(e, 400);
        }
    }
    
    @Test
    public void testImportBackup_no_such_file() {
        try {
            client.bucket("bucket").backup().secretKey("SECRET-KEY").file("nosuchfile.bak").executeImport();
            fail("An exception was expected");
        } catch (Exception e) {
            verifyTerrastoreRequestException(e, 500);
        }
    }

    @Test
    public void testExecuteUpdate_no_such_key() {
        try {
            client.bucket("bucket").key("value").update("make-funky").executeAndGet(PhoneNumber.class);
            fail("An exception was expected");
        } catch (Exception e) {
            verifyException(NoSuchKeyException.class, e, 404);
        }
    }

    @Test
    public void testExecuteUpdate_invalid_function() {
        try {
            client.bucket("bucket").key("value").put(TEST_PHONENUMBER);
            client.bucket("bucket").key("value").update("make-funky").executeAndGet(PhoneNumber.class);
            fail("An exception was expected");
        } catch (Exception e) {
            verifyTerrastoreRequestException(e, 400);
        }
    }
    
    @Test
    public void testExecuteUpdate_incorrect_parameters() {
        try {
            client.bucket("bucket").key("value").put(TEST_PHONENUMBER);
            
            Map<String, Object> parameters = new HashMap<String, Object>();
            parameters.put("type", "15");
            
            client.bucket("bucket").key("value").update("counter").parameters(parameters).timeOut(10000).executeAndGet(PhoneNumber.class);
            fail("Expected an exception");
            fail("An exception was expected");
        } catch (Exception e) {
            verifyTerrastoreRequestException(e, 500);
        }
    }
    
    private void verifyTerrastoreRequestException(Exception e) {
        verifyTerrastoreRequestException(e, null);
    }

    private void verifyException(Class<? extends TerrastoreRequestException> expectedClass, Exception e, Integer statusCode) {
        assertEquals(expectedClass, e.getClass());
        TerrastoreRequestException requestException = (TerrastoreRequestException) e;
        System.out.println("Exception message: (" + requestException.getStatus() + "): " + requestException.getMessage());
    }
    
    private void verifyTerrastoreRequestException(Exception e, Integer expectedStatusCode) {
        assertEquals("Expected TerrastoreRequestException. Got: " + e.getClass(), TerrastoreRequestException.class, e.getClass());
        TerrastoreRequestException requestException = (TerrastoreRequestException) e;
        System.out.println("Exception message (" + requestException.getStatus() + "): " + requestException.getMessage());
        if (null != expectedStatusCode) {
            assertEquals(expectedStatusCode, new Integer(requestException.getStatus()));
        }
        assertFalse(StringUtils.isBlank(requestException.getMessage()));
    }
    
    private void verifyTerrastoreConnectionException(Exception e) {
        assertEquals("Expected TerrastoreConnectionException. Got: " + e.getClass(), TerrastoreConnectionException.class, e.getClass());
        TerrastoreConnectionException requestException = (TerrastoreConnectionException) e;
        System.out.println("Exception message: " + requestException.getMessage());
        assertFalse(StringUtils.isBlank(requestException.getMessage()));
    }
    
    private void verifyTerrastoreClientException(Exception e) {
        assertEquals("Expected TerrastoreClientException. Got: " + e.getClass(), TerrastoreClientException.class, e.getClass());
        System.out.println("Exception message: " + e.getMessage());
        assertFalse(StringUtils.isBlank(e.getMessage()));
    }

    
}
