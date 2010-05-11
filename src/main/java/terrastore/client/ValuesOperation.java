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

import java.util.Map;

import terrastore.client.connection.Connection;

/**
 * @author Sven Johansson
 * @author Sergio Bossa
 * @since 2.0
 */
public class ValuesOperation extends AbstractOperation {

    private final String bucket;
    //
    private int limit;
    private String predicate;

    /**
     * Sets up a BucketOperation for the specified bucket and Terrastore
     * {@link Connection}
     * 
     * @param connection The connection to be used for bucket operations
     * @param bucket The bucket to be operated on.
     */
    ValuesOperation(Connection connection, String bucket) {
        super(connection);
        if (null == bucket) {
            throw new IllegalArgumentException("Bucket name cannot be null.");
        }
        this.bucket = bucket;
    }

    /**
     * Sets a max limit for retrieving values from this bucket.
     * 
     * The limit option is only valid for the {@link BucketOperation#get(Class)}
     * operation, and has no effect on other operations.
     * 
     * @param limit The max number of keys/values to retrieve from this bucket.
     * @return
     * 
     * @see BucketOperation#get(Class)
     */
    public ValuesOperation limit(int limit) {
        this.limit = limit;
        return this;
    }

    /**
     * 
     */
    public ValuesOperation conditionally(String predicate) {
        this.predicate = predicate;
        return this;
    }

    /**
     * Lists all values, or a subset of values defined by the
     * {@link #limit(int)} method, in this bucket. You must provide the expected
     * Java type for these values.
     * 
     * @param <T> The Java type of the values in this bucket
     * @param type The Java type of the values in this bucket
     * @return A {@link Map} of the bucket contents.
     * @throws TerrastoreClientException if server communication fails, or
     *             values cannot be deserialized to the specified type.
     * 
     * @see BucketOperation#limit(int)
     */
    public <T> Map<String, T> get(Class<T> type) throws TerrastoreClientException {
        if (predicate == null || predicate.isEmpty()) {
        return connection.getAllValues(new Context(), type);
        } else {
            return connection.doPredicateQuery(new Context(), type);
        }
    }

    public class Context {

        public String getBucket() {
            return bucket;
        }

        public int getLimit() {
            return limit;
        }

        public String getPredicate() {
            return predicate;
        }
    }
}
