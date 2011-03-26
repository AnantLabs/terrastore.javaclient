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
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.List;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.deser.CustomDeserializerFactory;
import org.codehaus.jackson.map.deser.StdDeserializerProvider;
import org.codehaus.jackson.map.ser.CustomSerializerFactory;

import terrastore.client.Values;

/**
 * @author Sergio Bossa
 */
@Provider
@Produces("application/json")
public class JsonValuesWriter implements MessageBodyWriter<Values> {

    private final ObjectMapper jsonMapper;

    public JsonValuesWriter(List<? extends JsonObjectDescriptor> descriptors) {
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

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return Values.class.isAssignableFrom(type);
    }

    @Override
    public long getSize(Values values, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return -1;
    }

    @Override
    public void writeTo(Values values, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream) throws IOException, WebApplicationException {
        jsonMapper.writeValue(entityStream, values);
    }

}
