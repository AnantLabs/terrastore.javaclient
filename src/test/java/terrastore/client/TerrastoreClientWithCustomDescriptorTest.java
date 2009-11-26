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

import java.io.IOException;
import java.util.Arrays;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;
import org.jboss.resteasy.plugins.server.tjws.TJWSEmbeddedJaxrsServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import terrastore.server.Server;
import terrastore.server.impl.JsonHttpServer;
import terrastore.server.impl.support.JsonErrorMessageProvider;
import terrastore.server.impl.support.JsonParametersMapProvider;
import terrastore.server.impl.support.JsonServerOperationExceptionMapper;
import terrastore.server.impl.support.JsonValueProvider;
import terrastore.server.impl.support.JsonValuesMapProvider;
import terrastore.service.QueryService;
import terrastore.service.UpdateService;
import terrastore.store.Value;
import static org.junit.Assert.*;
import static org.easymock.classextension.EasyMock.*;

/**
 * @author Sergio Bossa
 */
public class TerrastoreClientWithCustomDescriptorTest {

    private static final int STOP_WAIT_TIME = 5000;
    private static final String CUSTOM_JSON_VALUE = "{\"key\":\"value\"}";
    private static final TestValue CUSTOM_TEST_VALUE = new TestValue("value");
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
    public void testPutThenGetWithCustomDescriptor() throws Exception {
        TerrastoreClient client = new TerrastoreClient("http://localhost:8080", Arrays.asList(new TestValueDescriptor()));
        client.<TestValue>putValue("bucket", "custom", CUSTOM_TEST_VALUE);
        TestValue value = client.<TestValue>getValue("bucket", "custom", TestValue.class);
        assertNotNull(value);
        assertEquals(CUSTOM_TEST_VALUE, value);
    }

    private static Server setupTerrastoreServerMock() throws Exception {
        String bucket = "bucket";
        String custom = "custom";
        UpdateService updateService = createNiceMock(UpdateService.class);
        makeThreadSafe(updateService, true);
        QueryService queryService = createNiceMock(QueryService.class);
        makeThreadSafe(queryService, true);

        updateService.putValue(eq(bucket), eq(custom), eq(new Value(CUSTOM_JSON_VALUE.getBytes())));
        expectLastCall().asStub();

        queryService.getValue(bucket, custom);
        expectLastCall().andStubReturn(new Value(CUSTOM_JSON_VALUE.getBytes()));

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

    public static class TestValueDescriptor implements JsonObjectDescriptor<TestValue> {

        public Class<TestValue> getObjectClass() {
            return TestValue.class;
        }

        public JsonSerializer<TestValue> getJsonSerializer() {
            return new JsonSerializer<TestValue>() {

                @Override
                public void serialize(TestValue value, JsonGenerator generator, SerializerProvider provider) throws IOException, JsonProcessingException {
                    generator.writeStartObject();
                    generator.writeStringField("key", value.getValue());
                    generator.writeEndObject();
                    generator.flush();
                }
            };
        }

        public JsonDeserializer<TestValue> getJsonDeserializer() {
            return new JsonDeserializer<TestValue>() {

                @Override
                public TestValue deserialize(JsonParser parser, DeserializationContext ctxt) throws IOException, JsonProcessingException {
                    parser.nextToken();
                    parser.nextToken();
                    if (parser.getCurrentName().equals("key")) {
                        return new TestValue(parser.getText());
                    } else {
                        throw new IllegalStateException();
                    }
                }
            };
        }
    }
}
