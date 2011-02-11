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
import terrastore.client.merge.MergeDescriptor;
import terrastore.client.merge.MergeOperation;

/**
 * @author Sven Johansson
 * @author Sergio Bossa
 *  
 */
public class KeyOperation extends AbstractOperation {

    private final String bucket;
    private final String key;

    /**
     * Sets up a KeyOperation for the specified bucket, connection and key.
     * 
     * @param bucket The parent {@link BucketOperation}
     * @param connection The Connection to be used for server communication.
     * @param key The key to perform operations on.
     */
    KeyOperation(Connection connection, String bucket, String key) {
        super(connection);
        this.bucket = bucket;
        this.key = key;
    }

    /**
     * Writes a value/document for this key.
     * 
     * If a value is already present for this key, it will be
     * overwritten/replaced by the specified value.
     * 
     * @param <T> The Java type for the value
     * @param value The value to be written.
     * @throws TerrastoreClientException If server communication fails, or the
     *             value is rejected, i.e. because it cannot be serialized.
     */
    public <T> void put(T value) throws TerrastoreClientException {
        connection.putValue(new Context(), value);
    }

    /**
     * Removes/deletes this key and its value from the current bucket.
     * 
     * @throws TerrastoreClientException if server communication fails.
     */
    public void remove() throws TerrastoreClientException {
        connection.removeValue(new Context());
    }

    /**
     * Retrieves the stored value for this key, as an instance of the specified
     * Java type.
     * 
     * @param <T> The Java type for this value.
     * @param type The Java type for this value.
     * @return The value for the current key, as an instance of <T>/type
     * @throws TerrastoreClientException if server communication fails, or the
     *             key does not exist within the current bucket.
     */
    public <T> T get(Class<T> type) throws TerrastoreClientException {
        return connection.getValue(new Context(), type);
    }

    /**
     * Sets up an {@link UpdateOperation} for the value of the current key.
     *
     * @param function The name of the update function to invoke.
     * @return an UpdateOperation for the current key.
     */
    public UpdateOperation update(String function) {
        return new UpdateOperation(connection, bucket, key, function);
    }

    /**
     * Sets up a {@link terrastore.client.merge.MergeOperation} for the document at the current key.
     *
     * @param descriptor The merge descriptor.
     * @return a MergeOperation for the current key.
     */
    public MergeOperation merge(MergeDescriptor descriptor) {
        return new MergeOperation(connection, bucket, key, descriptor);
    }
    
    /**
     * Sets up a {@link ConditionalOperation} for the current key, that may
     * be used to get or put values based on the fulfilment of a predicate.
     * 
     * @param predicate The predicate to serve as a condition.
     */
    public ConditionalOperation conditional(String predicate) {
        return new ConditionalOperation(connection, bucket, key, predicate);
    }

    public class Context {

        public String getKey() {
            return key;
        }

        public String getBucket() {
            return bucket;
        }
    }
}
