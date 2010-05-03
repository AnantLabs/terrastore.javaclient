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
package terrastore.client.connection;

import java.util.Map;
import java.util.Set;

import terrastore.client.BackupOperation;
import terrastore.client.BucketOperation;
import terrastore.client.KeyOperation;
import terrastore.client.PredicateQuery;
import terrastore.client.RangeQuery;
import terrastore.client.TerrastoreClientException;
import terrastore.client.UpdateOperation;
import terrastore.client.Values;

/**
 * Connection interface for Terrastore server operations.
 * 
 * @author Sven Johansson
 * @date 24 apr 2010
 * @since 2.0
 */
public interface Connection {

    /**
     * Adds a bucket with the name specified by a {@link BucketOperation}
     * 
     * @param bucket The BucketOperation that describes the bucket.
     * @throws TerrastoreClientException If the server returns a failure
     *             response.
     */
    void addBucket(BucketOperation bucket) throws TerrastoreClientException;

    /**
     * Removes a bucket with the name specified by a {@link BucketOperation}
     * 
     * @param bucket The BucketOperation that describes the bucket.
     * @throws TerrastoreClientException If the server returns a failure
     *             response.
     */
    void removeBucket(BucketOperation bucket) throws TerrastoreClientException;

    /**
     * Retrieves a {@link Set} containing the names of all available buckets.
     * 
     * @return The names of all existing buckets.
     * @throws TerrastoreClientException If the server returns a failure
     *             response.
     */
    Set<String> getBuckets() throws TerrastoreClientException;

    /**
     * Put/store a value in a bucket, under a key as specified by a
     * KeyOperation.
     * 
     * @param <T> The Java type of the value to be stored
     * @param key The KeyOperation that describes the operation
     * @param value The value to be stored
     * @throws TerrastoreClientException If the server returns a failure
     *             response.
     */
    <T> void putValue(KeyOperation key, T value)
            throws TerrastoreClientException;

    /**
     * Remove/delete a key and its value, as specified by a KeyOperation.
     * 
     * @param key The KeyOperation that describes the operation
     * @throws TerrastoreClientException
     */
    void removeValue(KeyOperation key) throws TerrastoreClientException;

    /**
     * Gets the stored value for a key, as an instance of the specified Java
     * type.
     * 
     * @param <T> The Java type of the stored value
     * @param key The KeyOperation that describes this operation.
     * @param type The Java type of the stored value
     * @return An instance of <T> representing the specified value.
     * @throws TerrastoreClientException If the server returns a failure
     *             response or if the specified bucket/key does not exist.
     */
    <T> T getValue(KeyOperation key, Class<T> type)
            throws TerrastoreClientException;

    /**
     * Returns all (or up to the specified limit) values within a bucket.
     * 
     * @param <T> The Java type of the bucket values
     * @param bucket The BucketOperation that describes this operation.
     * @param limit The max amount of keys/values to retrieve.
     * @param type The Java type of the bucket values
     * @return A Map of the keys/values in the current bucket.
     * @throws TerrastoreClientException if the server returns a failure
     *             response, or if the specified bucket does not exist.
     */
    <T> Map<String, T> getAllValues(BucketOperation bucket, Class<T> type)
            throws TerrastoreClientException;

    /**
     * Executes a {@link RangeQuery} and returns the results as a Values/Map.
     * 
     * @param <T> The Java type of the bucket values
     * @param query The RangeQuery to execute.
     * @param type The Java type of the bucket values
     * @return A Values/Map of the keys/values selected by the RangeQuery.
     * @throws TerrastoreClientException if the server returns a failure
     *             response, i.e. because the query is incorrect.
     */
    <T> Values<T> doRangeQuery(RangeQuery query, Class<T> type)
            throws TerrastoreClientException;

    /**
     * 
     * 
     * @param <T>
     * @param bucketName
     * @param query
     * @param type
     * @return
     * @throws TerrastoreClientException
     */
    <T> Values<T> doPredicateQuery(PredicateQuery query, Class<T> type)
            throws TerrastoreClientException;

    /**
     * 
     * @param backupOperation
     * @throws TerrastoreClientException
     */
    void exportBackup(BackupOperation backup) throws TerrastoreClientException;

    void importBackup(BackupOperation backup) throws TerrastoreClientException;

    void executeUpdate(UpdateOperation update) throws TerrastoreClientException;

    String getServerHost();

}
