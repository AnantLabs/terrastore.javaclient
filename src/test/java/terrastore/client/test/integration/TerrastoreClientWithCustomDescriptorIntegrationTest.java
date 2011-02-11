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
package terrastore.client.test.integration;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import terrastore.client.TerrastoreClient;
import terrastore.client.connection.resteasy.HTTPConnectionFactory;
import terrastore.client.mapping.JsonObjectDescriptor;
import terrastore.server.EmbeddedServerWrapper;

/**
 * @author Sven Johansson
 * @author Sergio Bossa  
 */
public class TerrastoreClientWithCustomDescriptorIntegrationTest {

    private static final TestValue CUSTOM_TEST_VALUE = new TestValue("value");

    private static EmbeddedServerWrapper server;
    
    @BeforeClass
    public static void startEmbeddedServer() throws Exception {
        server = new EmbeddedServerWrapper();
    }
    
    @AfterClass
    public static void stopEmbeddedServer() throws Exception {
        server.stop();
    }

    @Test
    public void testPutThenGetWithCustomDescriptor() throws Exception {
        TerrastoreClient client = new TerrastoreClient("http://localhost:8080", new HTTPConnectionFactory(), new ArrayList<JsonObjectDescriptor<?>>(Arrays.asList(new TestValueDescriptor())));

        client.bucket("bucket").key("custom").put(CUSTOM_TEST_VALUE);
        TestValue value = client.bucket("bucket").key("custom").get(TestValue.class);
        assertNotNull(value);
        assertEquals(CUSTOM_TEST_VALUE, value);

        client.bucket("bucket").clear();
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
