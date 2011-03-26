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
package terrastore.client.connection;

import java.util.Set;

import terrastore.client.BackupOperation;
import terrastore.client.BulkOperation;
import terrastore.client.ClusterStats;
import terrastore.client.ConditionalOperation;
import terrastore.client.KeyOperation;
import terrastore.client.PredicateOperation;
import terrastore.client.RangeOperation;
import terrastore.client.TerrastoreClientException;
import terrastore.client.UpdateOperation;
import terrastore.client.Values;
import terrastore.client.ValuesOperation;
import terrastore.client.RangeOperation.Context;
import terrastore.client.mapreduce.MapReduceOperation;
import terrastore.client.merge.MergeOperation;

/**
 * Connection interface for Terrastore server operations.
 * 
 * @author Sven Johansson
 * @author Sergio Bossa
 *  
 */
public interface Connection {

    /**
     * Gets the cluster statistics.
     */
    ClusterStats getClusterStats() throws TerrastoreClientException;

    /**
     * Removes a bucket.
     */
    void clearBucket(String bucket) throws TerrastoreClientException;

    /**
     * Retrieves a {@link Set} containing the names of all available buckets.
     */
    Set<String> getBuckets() throws TerrastoreClientException;

    /**
     * Put/store a value in a bucket.
     */
    <T> void putValue(KeyOperation.Context context, T value)
            throws TerrastoreClientException;

    /**
     * Conditionally put/store a value in a bucket.
     */
    <T> void putValue(ConditionalOperation.Context context, T value)
            throws TerrastoreClientException;

    /**
     * Remove/delete a key and its value.
     */
    void removeValue(KeyOperation.Context context) throws TerrastoreClientException;

    /**
     * Gets the stored value for a key, as an instance of the specified Java
     * type.
     */
    <T> T getValue(KeyOperation.Context context, Class<T> type)
            throws TerrastoreClientException;

    /**
     * Conditionally gets the stored value for a key, as an instance of the specified Java
     * type.
     */
    <T> T getValue(ConditionalOperation.Context context, Class<T> type)
            throws TerrastoreClientException;

    /**
     * Returns all (or up to the specified limit) values within a bucket.
     */
    <T> Values<T> getAllValues(ValuesOperation.Context context, Class<T> type)
            throws TerrastoreClientException;

    /**
     * Executes a range query and returns the results as a Values/Map.
     */
    <T> Values<T> queryByRange(RangeOperation.Context context, Class<T> type)
            throws TerrastoreClientException;

    /**
     * Executes a range based remove command and returns the removed key set
     */
    Set<String> removeByRange(Context context)
            throws TerrastoreClientException;
    
    /**
     * Executes a predicate query on all values and returns the results as a Values/Map.
     */
    <T> Values<T> queryByPredicate(PredicateOperation.Context context, Class<T> type)
            throws TerrastoreClientException;

    /**
     * Executes a map-reduce query and returns the aggregated value.
     */
    <T> T queryByMapReduce(MapReduceOperation.Context context, Class<T> returnType)
            throws TerrastoreClientException;

    /**
     * Do a backup export.
     */
    void exportBackup(BackupOperation.Context context) throws TerrastoreClientException;

    /**
     * Do a backup import.
     */
    void importBackup(BackupOperation.Context context) throws TerrastoreClientException;

    /**
     * Execute an atomic update and returns the updated value.
     */
    <T> T executeUpdate(UpdateOperation.Context context, Class<T> type) throws TerrastoreClientException;

    /**
     * Execute a merge operation and return the merged value.
     */
    <T> T executeMerge(MergeOperation.Context context, Class<T> type) throws TerrastoreClientException;

    /**
     * Executes a bulk put operation and returns the keys whose values have been actually inserted.
     */
    Set<String> bulkPut(BulkOperation.Context context) throws TerrastoreClientException;

    /**
     * Executes a bulk get operation and returns all values corresponding to the given keys.
     */
    <T> Values<T> bulkGet(BulkOperation.Context context, Class<T> type) throws TerrastoreClientException;
}
