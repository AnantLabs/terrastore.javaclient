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

import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.JsonSerializer;

/**
 * Interface to implement for providing custom serializer and deserializer to use for converting
 * Java objects to/from Json objects.<br>
 * Custom serializer and deserializer are based on Jackson Json library (see http://jackson.codehaus.org/).
 *
 * @author Sergio Bossa
 */
public interface JsonObjectDescriptor<T> {

    /**
     * Get the class of the Java object whose custom serializer and deserializer
     * are provided here. This must be the exact class.
     */
    public Class<T> getObjectClass();

    /**
     * Get the custom serializer.
     */
    public JsonSerializer<T> getJsonSerializer();

    /**
     * Get the custom deserializer.
     */
    public JsonDeserializer<T> getJsonDeserializer();
}
