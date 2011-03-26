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
public class ValuesOperation extends AbstractOperation {

    private final String bucket;

    private volatile int limit;

    ValuesOperation(Connection connection, String bucket) {
        super(connection);
        if (null == bucket) {
            throw new IllegalArgumentException("Bucket name cannot be null.");
        }
        this.bucket = bucket;
    }
    
    ValuesOperation(ValuesOperation other) {
        super(other.connection);
        this.bucket = other.bucket;
        this.limit = other.limit;
    }

    /**
     * Specifies a maximum limit of values to retrieve using this
     * ValuesOperation. 
     * 
     * @param limit The maximum number of values to retrieve.
     */
    public ValuesOperation limit(int limit) {
        ValuesOperation newInstance = new ValuesOperation(this);
        newInstance.limit = limit;
        return newInstance;
    }

    /**
     * Retrieves all values contained in the current bucket, or as many
     * as permitted by the limit-method.
     * 
     * @param <T> The Java type to deserialize the values to.
     * @param type The Java class to deserialize the values to.
     * @return A Map of keys and values.
     * @throws TerrastoreClientException if the operation fails for any reason.
     */
    public <T> Values<T> get(Class<T> type) throws TerrastoreClientException {
        return connection.getAllValues(new Context(), type);
    }

    public class Context {

        public String getBucket() {
            return bucket;
        }

        public int getLimit() {
            return limit;
        }
    }
}
