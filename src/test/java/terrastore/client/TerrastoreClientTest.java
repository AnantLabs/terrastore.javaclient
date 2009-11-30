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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.jboss.resteasy.plugins.server.tjws.TJWSEmbeddedJaxrsServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import terrastore.common.ErrorMessage;
import terrastore.server.Server;
import terrastore.server.impl.JsonHttpServer;
import terrastore.server.impl.support.JsonErrorMessageProvider;
import terrastore.server.impl.support.JsonParametersMapProvider;
import terrastore.server.impl.support.JsonServerOperationExceptionMapper;
import terrastore.server.impl.support.JsonValueProvider;
import terrastore.server.impl.support.JsonValuesMapProvider;
import terrastore.service.QueryOperationException;
import terrastore.service.QueryService;
import terrastore.service.UpdateOperationException;
import terrastore.service.UpdateService;
import terrastore.service.comparators.LexicographicalComparator;
import terrastore.store.Value;
import terrastore.store.features.Update;
import terrastore.store.features.Range;
import static org.junit.Assert.*;
import static org.easymock.classextension.EasyMock.*;

/**
 * @author Sergio Bossa
 */
public class TerrastoreClientTest {

    private static final int STOP_WAIT_TIME = 5000;
    private static final String JSON_VALUE_1 = "{\"value\":\"value_1\"}";
    private static final String JSON_VALUE_2 = "{\"value\":\"value_2\"}";
    private static final String JSON_VALUE_3 = "{\"value\":\"value_3\"}";
    private static final TestValue TEST_VALUE_1 = new TestValue("value_1");
    private static final TestValue TEST_VALUE_2 = new TestValue("value_2");
    private static final TestValue TEST_VALUE_3 = new TestValue("value_3");
    private static TJWSEmbeddedJaxrsServer server;

    @BeforeClass
    public static void onSetUp() throws Exception {
        startWebServerWith(setupTerrastoreServerMock());
    }

    @AfterClass
    public static void onTearDown() throws Exception {
        server.stop();
        Thread.sleep(STOP_WAIT_TIME);
    }

    @Test
    public void testAddBucket() throws Exception {
        TerrastoreClient client = new TerrastoreClient("http://localhost:8080");
        client.addBucket("bucket");
    }

    @Test
    public void testRemoveBucket() throws Exception {
        TerrastoreClient client = new TerrastoreClient("http://localhost:8080");
        client.removeBucket("bucket");
    }

    @Test(expected = TerrastoreRequestException.class)
    public void testRemoveBucketNotFoundThrowsException() throws Exception {
        TerrastoreClient client = new TerrastoreClient("http://localhost:8080");
        client.removeBucket("error");
    }

    @Test
    public void testAddThenRemoveBucket() throws Exception {
        TerrastoreClient client = new TerrastoreClient("http://localhost:8080");
        client.addBucket("bucket");
        client.removeBucket("bucket");
    }

    @Test
    public void testPutValue() throws Exception {
        TerrastoreClient client = new TerrastoreClient("http://localhost:8080");
        client.<TestValue>putValue("bucket", "key1", TEST_VALUE_1);
    }

    @Test
    public void testRemoveValue() throws Exception {
        TerrastoreClient client = new TerrastoreClient("http://localhost:8080");
        client.removeValue("bucket", "key1");
    }

    @Test(expected = TerrastoreRequestException.class)
    public void testRemoveValueNotFoundThrowsException() throws Exception {
        TerrastoreClient client = new TerrastoreClient("http://localhost:8080");
        client.removeValue("bucket", "error");
    }

    @Test
    public void testGetValue() throws Exception {
        TerrastoreClient client = new TerrastoreClient("http://localhost:8080");
        TestValue value = client.<TestValue>getValue("bucket", "key1", TestValue.class);
        assertNotNull(value);
        assertEquals(TEST_VALUE_1, value);
    }

    @Test(expected = TerrastoreRequestException.class)
    public void testGetValueNotFoundThrowsException() throws Exception {
        TerrastoreClient client = new TerrastoreClient("http://localhost:8080");
        client.<TestValue>getValue("bucket", "error", TestValue.class);
    }

    @Test
    public void testPutThenGetThenRemoveValue() throws Exception {
        TerrastoreClient client = new TerrastoreClient("http://localhost:8080");
        client.<TestValue>putValue("bucket", "key1", TEST_VALUE_1);
        TestValue value = client.<TestValue>getValue("bucket", "key1", TestValue.class);
        assertNotNull(value);
        assertEquals(TEST_VALUE_1, value);
        client.removeValue("bucket", "key1");
    }

    @Test
    public void testGetAllValues() throws Exception {
        TerrastoreClient client = new TerrastoreClient("http://localhost:8080");
        Map<String, TestValue> map = client.<TestValue>getAllValues("bucket", TestValue.class);
        assertNotNull(map);
        assertEquals(3, map.size());
        assertTrue(map.containsKey("key1"));
        assertTrue(map.containsKey("key2"));
        assertTrue(map.containsKey("key3"));
        assertTrue(map.containsValue(TEST_VALUE_1));
        assertTrue(map.containsValue(TEST_VALUE_2));
        assertTrue(map.containsValue(TEST_VALUE_3));
    }

    @Test
    public void testDoRangeQuery() throws Exception {
        TerrastoreClient client = new TerrastoreClient("http://localhost:8080");
        Map<String, TestValue> map = client.<TestValue>doRangeQuery("bucket", "key2", "key3", "lexical-asc", 0, TestValue.class);
        assertNotNull(map);
        assertEquals(2, map.size());
        List<TestValue> values = new ArrayList<TestValue>(map.values());
        assertEquals(TEST_VALUE_2, values.get(0));
        assertEquals(TEST_VALUE_3, values.get(1));
    }

    @Test
    public void testExecuteUpdate() throws Exception {
        Map<String, Object> parameters = new HashMap<String, Object>();
        String param1 = "param1";
        String value1 = "value1";
        parameters.put(param1, value1);

        TerrastoreClient client = new TerrastoreClient("http://localhost:8080");
        client.executeUpdate("bucket", "key1", "update", 1000, new Parameters(parameters));
    }

    private static Server setupTerrastoreServerMock() throws Exception {
        String bucket = "bucket";
        String key1 = "key1";
        String key2 = "key2";
        String key3 = "key3";
        String notFound = "error";
        Map<String, Value> values = new HashMap<String, Value>();
        values.put(key1, new Value(JSON_VALUE_1.getBytes()));
        values.put(key2, new Value(JSON_VALUE_2.getBytes()));
        values.put(key3, new Value(JSON_VALUE_3.getBytes()));
        SortedMap<String, Value> range = new TreeMap<String, Value>(new LexicographicalComparator(true));
        range.put(key2, new Value(JSON_VALUE_2.getBytes()));
        range.put(key3, new Value(JSON_VALUE_3.getBytes()));
        Map<String, Object> parameters = new HashMap<String, Object>();
        String param1 = "param1";
        String value1 = "value1";
        parameters.put(param1, value1);
        UpdateService updateService = createNiceMock(UpdateService.class);
        makeThreadSafe(updateService, true);
        QueryService queryService = createNiceMock(QueryService.class);
        makeThreadSafe(queryService, true);

        updateService.addBucket(eq(bucket));
        expectLastCall().asStub();
        updateService.removeBucket(eq(bucket));
        expectLastCall().asStub();
        updateService.removeBucket(eq(notFound));
        expectLastCall().andStubThrow(new UpdateOperationException(new ErrorMessage(404, "error")));
        updateService.putValue(eq(bucket), eq(key1), eq(new Value(JSON_VALUE_1.getBytes())));
        expectLastCall().asStub();
        updateService.removeValue(eq(bucket), eq(key1));
        expectLastCall().asStub();
        updateService.removeValue(eq(bucket), eq(notFound));
        expectLastCall().andStubThrow(new UpdateOperationException(new ErrorMessage(404, "error")));
        updateService.executeUpdate(eq(bucket), eq(key1), eq(new Update("update", 1000, parameters)));
        expectLastCall().asStub();

        queryService.getValue(bucket, key1);
        expectLastCall().andStubReturn(new Value(JSON_VALUE_1.getBytes()));
        queryService.getValue(bucket, key2);
        expectLastCall().andStubReturn(new Value(JSON_VALUE_2.getBytes()));
        queryService.getValue(bucket, key3);
        expectLastCall().andStubReturn(new Value(JSON_VALUE_3.getBytes()));
        queryService.getValue(bucket, notFound);
        expectLastCall().andStubThrow(new QueryOperationException(new ErrorMessage(404, "error")));
        queryService.getAllValues(bucket);
        expectLastCall().andStubReturn(values);
        queryService.doRangeQuery(bucket, new Range(key2, key3, "lexical-asc"));
        expectLastCall().andStubReturn(range);

        replay(updateService, queryService);

        return new JsonHttpServer(updateService, queryService);
    }

    private static void startWebServerWith(Server terrastoreServer) {
        server = new TJWSEmbeddedJaxrsServer();
        server.getDeployment().setRegisterBuiltin(true);
        server.getDeployment().setProviderClasses(Arrays.asList(
                JsonErrorMessageProvider.class.getName(),
                JsonValuesMapProvider.class.getName(),
                JsonParametersMapProvider.class.getName(),
                JsonValueProvider.class.getName(),
                JsonServerOperationExceptionMapper.class.getName()));
        server.getDeployment().setResources(Arrays.<Object>asList(terrastoreServer));
        server.setPort(8080);
        server.start();
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
