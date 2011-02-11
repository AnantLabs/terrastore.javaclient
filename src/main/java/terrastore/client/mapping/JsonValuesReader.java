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
package terrastore.client.mapping;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.Consumes;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.Provider;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.deser.CustomDeserializerFactory;
import org.codehaus.jackson.map.deser.StdDeserializerProvider;
import org.codehaus.jackson.map.ser.CustomSerializerFactory;

import terrastore.client.Values;

/**
 * @author Sergio Bossa
 */
@Provider
@Consumes("application/json")
public class JsonValuesReader implements MessageBodyReader<Values> {

    private final ObjectMapper jsonMapper;

    public JsonValuesReader(List<? extends JsonObjectDescriptor> descriptors) {
        CustomSerializerFactory serializerFactory = new CustomSerializerFactory();
        CustomDeserializerFactory deserializerFactory = new CustomDeserializerFactory();
        for (JsonObjectDescriptor descriptor : descriptors) {
            serializerFactory.addSpecificMapping(descriptor.getObjectClass(), descriptor.getJsonSerializer());
            deserializerFactory.addSpecificMapping(descriptor.getObjectClass(), descriptor.getJsonDeserializer());
        }
        this.jsonMapper = new ObjectMapper();
        this.jsonMapper.setSerializerFactory(serializerFactory);
        this.jsonMapper.setDeserializerProvider(new StdDeserializerProvider(deserializerFactory));
    }

    public boolean isReadable(Class type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return Values.class.isAssignableFrom(type);
    }

    public Values readFrom(Class type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap httpHeaders, InputStream entityStream) throws IOException, WebApplicationException {
        Map<String, Object> result = new LinkedHashMap<String, Object>();
        JsonParser jsonParser = jsonMapper.getJsonFactory().createJsonParser(entityStream);
        jsonParser.nextToken();
        while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
            String name = jsonParser.getCurrentName();
            jsonParser.nextToken();
            Object value = jsonParser.readValueAs((Class) genericType);
            result.put(name, value);
        }
        return new Values(result);
    }
}
