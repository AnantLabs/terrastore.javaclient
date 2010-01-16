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
package terrastore.client.mapping;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Collection;
import javax.ws.rs.Consumes;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.Provider;
import org.codehaus.jackson.map.ObjectMapper;
import terrastore.client.Buckets;

/**
 * @author Sergio Bossa
 */
@Provider
@Consumes("application/json")
public class JsonBucketsReader implements MessageBodyReader<Buckets> {

    private final ObjectMapper jsonMapper;

    public JsonBucketsReader() {
        this.jsonMapper = new ObjectMapper();
    }

    public boolean isReadable(Class type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return Buckets.class.isAssignableFrom(type);
    }

    public Buckets readFrom(Class type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap httpHeaders, InputStream entityStream) throws IOException, WebApplicationException {
        Collection<String> result = jsonMapper.readValue(entityStream, Collection.class);
        return new Buckets(result);
    }
}
