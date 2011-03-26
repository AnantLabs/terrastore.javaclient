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
import terrastore.client.mapreduce.MapReduceOperation;
import terrastore.client.mapreduce.MapReduceQuery;

/**
 * BucketOperation serves as intermediaries for server operations pertaining to
 * Terrastore buckets, such as removing buckets, or reading
 * keys/values from buckets.
 * 
 * The {@link BucketOperation} is the root operation for all operations that are
 * performed on a specific bucket.
 * 
 * @author Sven Johansson
 * @author Sergio Bossa
 *  
 */
public class BucketOperation extends AbstractOperation {

    private final String bucket;

    /**
     * Sets up a BucketOperation for the specified bucket and Terrastore
     * {@link Connection}
     * 
     * @param connection The connection to be used for bucket operations
     * @param bucket The bucket to be operated on.
     */
    BucketOperation(Connection connection, String bucket) {
        super(connection);
        if (null == bucket) {
            throw new IllegalArgumentException("Bucket name cannot be null.");
        }
        this.bucket = bucket;
    }

    /**
     * Clears this bucket on the Terrastore server cluster, discarding all stored contents.
     * 
     * @throws TerrastoreClientException if the request fails
     */
    public void clear() throws TerrastoreClientException {
        connection.clearBucket(bucket);
    }

    /**
     * Sets up a {@link KeyOperation} for a named key within this bucket.
     * 
     * @param key The name of the key.
     * @return a {@link KeyOperation} instance for the specified key.
     */
    public KeyOperation key(String key) {
        return new KeyOperation(connection, bucket, key);
    }

    /**
     * Sets up a {@link ValuesOperation} to perform operations on all 
     * existing values in this bucket.
     */
    public ValuesOperation values() {
        return new ValuesOperation(connection, bucket);
    }

    /**
     * Sets up a {@link PredicateOperation} to evaluate a predicate on 
     * all bucket values.
     */
    public PredicateOperation predicate(String predicate) {
        return new PredicateOperation(connection, bucket, predicate);
    }

    /**
     * Sets up a Range Query for this bucket with a specified comparator.
     * Comparators can be configured on the Terrastore server. Valid
     * pre-configured comparator identifiers are <code>lexical-asc</code>,
     * <code>lexical-desc</code>, <code>numeric-asc</code> and
     * <code>numeric-desc</code>.
     * 
     * @param comparator The name/identifier of the comparator to be used.
     * @return A RangeOperation instance with the specified comparator
     */
    public RangeOperation range(String comparator) {
        return new RangeOperation(connection, bucket, comparator);
    }

    /**
     * Sets up a {@link RangeOperation} for this bucket with the, on the Terrastore
     * server, configured default comparator.
     * 
     * The default comparator is <code>lexical-asc</code>, unless default
     * configuration has been overridden in the server configuration.
     * 
     * @return A RangeOperation with the default comparator.
     */
    public RangeOperation range() {
        return new RangeOperation(connection, bucket);
    }

    /**
     * Sets up a {@link BackupOperation} for this bucket, used
     * for importing or exporting snapshots of bucket contents.
     * 
     * @return A {@link BackupOperation} for this bucket.
     */
    public BackupOperation backup() {
        return new BackupOperation(connection, bucket);
    }

    public MapReduceOperation mapReduce(MapReduceQuery query) {
        return new MapReduceOperation(connection, bucket, query);
    }

    public BulkOperation bulk() {
        return new BulkOperation(connection, bucket);
    }
}
