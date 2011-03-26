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
public class PredicateOperation extends AbstractOperation {

    private final String bucket;
    private final String predicate;

    PredicateOperation(Connection connection, String bucket, String predicate) {
        super(connection);
        if (null == bucket) {
            throw new IllegalArgumentException("Bucket name cannot be null.");
        }
        if (null == predicate) {
            throw new IllegalArgumentException("Predicate cannot be null.");
        }
        this.bucket = bucket;
        this.predicate = predicate;
    }

    /**
     * Retrieves a Map of all keys/values matching the specified predicate.
     * 
     * @param <T> The Java type to deserialize the values to.
     * @param type The Java class to deserialize the values to.
     * @return A Map of matching keys and values.
     * @throws TerrastoreClientException if the request is invalid, ie due to an incorrect predicate syntax.
     */
    public <T> Values<T> get(Class<T> type) throws TerrastoreClientException {
        return connection.queryByPredicate(new Context(), type);
    }

    public class Context {

        public String getBucket() {
            return bucket;
        }

        public String getPredicate() {
            return predicate;
        }
    }
}
