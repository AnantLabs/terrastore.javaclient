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
 * BucketOperations serves as intermediaries for server operations
 * pertaining to Terrastore buckets, such as adding or removing buckets, or
 * reading keys/values for buckets.
 * 
 * The {@link BucketOperation} is the root operation for all operations that
 * are performed on a specific bucket.
 * 
 * @author Sven Johansson
 * @date 24 apr 2010
 * @since 2.0
 */
public class BucketOperation extends AbstractOperation {

	private String bucketName;
	private int limit = 0;

	/**
	 * Sets up a BucketOperation for the specified bucket and Terrastore
	 * {@link Connection}
	 * 
	 * @param connection The connection to be used for bucket operations
	 * @param bucketName The bucket to be operated on.
	 */
	BucketOperation(Connection connection, String bucketName) {
		super(connection);
		if (null == bucketName) {
			throw new IllegalArgumentException("Bucket name cannot be null.");
		}
		this.bucketName = bucketName;
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
	public BucketOperation limit(int limit) {
		this.limit = limit;
		return this;
	}
	
	/**
	 * Adds/creates this bucket on the Terrastore server cluster.
	 * 
	 * @throws TerrastoreClientException if the request fails.
	 */
	public void add() throws TerrastoreClientException {
		connection.addBucket(this);
	}

	/**
	 * Removes/deletes this bucket on the Terrastore server cluster.
	 * 
	 * Removing a bucket also discards any and all content stored within.
	 * 
	 * @throws TerrastoreClientException if the request fails
	 */
	public void remove() throws TerrastoreClientException {
		connection.removeBucket(this);
	}
	
	/**
	 * Sets up a {@link KeyOperation} for a named key within this bucket.
	 * 
	 * @param key The name of the key.
	 * @return a {@link KeyOperation} instance for the specified key. 
	 */
	public KeyOperation key(String key) {
		return new KeyOperation(this, connection, key);
	}

	/**
	 * Lists all values, or a subset of values defined by the
	 * {@link #limit(int)} method, in this bucket. You must provide the expected
	 * Java type for these values.
	 * 
	 * @param <T>
	 *            The Java type of the values in this bucket
	 * @param type
	 *            The Java type of the values in this bucket
	 * @return A {@link Map} of the bucket contents.
	 * @throws TerrastoreClientException
	 *             if server communication fails, or values cannot be
	 *             deserialized to the specified type.
	 *             
 	 * @see BucketOperation#limit(int)
	 */
	public <T> Map<String, T> get(Class<T> type) throws TerrastoreClientException {
		return connection.getAllValues(this, type);
	}

	/**
	 * Sets up a Range Query for this bucket with a specified comparator.
	 * Comparators can be configured on the Terrastore server. Valid pre-configured
	 * comparator identifiers are <code>lexical-asc</code>, <code>lexical-desc</code>,
	 * <code>numeric-asc</code> and <code>numeric-desc</code>.
	 * 
	 * @param comparator The name/identifier of the comparator to be used. 
	 * @return A RangeQuery instance with the specified comparator
	 */
	public RangeQuery rangeQuery(String comparator) {
		return new RangeQuery(this, connection, comparator);
	}
	
	/**
	 * Sets up a Range Query for this bucket with the, on the Terrastore server, configured
	 * default comparator.
	 * 
	 * The default comparator is <code>lexical-asc</code>, unless default configuration has been
	 * overriden.
	 * 
	 * @return A RangeQuery with the default comparator.
	 */
	public RangeQuery rangeQuery() {
		return new RangeQuery(this, connection);
	}
	
	/**
	 * Sets up a {@link PredicateQuery} within this bucket.
	 * 
	 * TODO: Quick instruction about predicates. JxPath only?
	 * 
	 * @param predicate The predicate to be used
	 * @return
	 */
	public PredicateQuery predicateQuery(String predicate) {
		return new PredicateQuery(this, connection, predicate);
	}

	/**
	 * Sets up a {@link BackupOperation} for this bucket.
	 * 
	 * @return A {@link BackupOperation} for this bucket.
	 */
	public BackupOperation backup() {
		return new BackupOperation(this, connection);		
	}
	
	/**
	 * @return the name of the bucket this instance operates on.
	 */
	public String bucketName() {
		return bucketName;
	}

	public int limit() {
		return limit;
	}
	
}
