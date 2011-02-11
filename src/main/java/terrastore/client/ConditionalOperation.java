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
package terrastore.client;

import terrastore.client.connection.Connection;

/**
 * @author Sven Johansson
 * @author Sergio Bossa
 *  
 */
public class ConditionalOperation extends AbstractOperation {

    private final String bucket;
    private final String key;
    private final String predicate;

    public ConditionalOperation(Connection connection, String bucket, String key, String predicate) {
        super(connection);
        this.bucket = bucket;
        this.key = key;
        this.predicate = predicate;
    }

    /**
     * Puts a value under the current key if, and only if the predicate
     * condition is satisfied.
     * 
     * @param <T> The Java type of the object to put.
     * @param value The value to put
     * @throws TerrastoreClientException if the operation fails, ie due to an invalid predicate.
     */
    public <T> void put(T value) throws TerrastoreClientException {
        connection.putValue(new Context(), value);
    }

    /**
     * Retrieves the value of the current key if, and only if the predicate
     * condition is satisfied.
     * 
     * @param <T> The Java type of the object to retrieve.
     * @param type The Java class of the object to retrieve.
     * @return The value stored under the current key, if the condition is satisfied.
     * @throws TerrastoreClientException if the operation fails, ie due to an invalid predicate.
     */
    public <T> T get(Class<T> type) throws TerrastoreClientException {
        return connection.getValue(new Context(), type);
    }

    public class Context {

        public String getKey() {
            return key;
        }

        public String getBucket() {
            return bucket;
        }

        public String getPredicate() {
            return predicate;
        }
    }
}
