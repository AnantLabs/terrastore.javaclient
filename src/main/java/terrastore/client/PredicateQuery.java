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

import terrastore.client.connection.Connection;

/**
 * TODO: Basic summary of predicate queries.
 * 
 * @author Sven Johansson
 * @date 24 apr 2010
 * @since 2.0
 */
public class PredicateQuery extends AbstractBucketOperation {

	private String predicate;

	/**
	 * Sets up a PredicateQuery within a given bucket.
	 * 
	 * @param bucket
	 * @param connection
	 * @param predicate
	 */
	public PredicateQuery(BucketOperation bucket, Connection connection, String predicate) {
		super(bucket, connection);
		this.predicate = predicate;
	}

	/**
	 * Executes this query and returns a Map of the matching keys/values.
	 * 
	 * @param <T> The Java type of the bucket values.
	 * @param type The Java type of the bucket values.
	 * @return A Values map of the matching keys/values
	 * 
	 * @throws TerrastoreClientException if server communication fails, or the predicate syntax is incorrect.
	 */
	public <T> Values<T> result(Class<T> type) throws TerrastoreClientException {
		return connection.doPredicateQuery(this, type);
	}

	/**
	 * @return the predicate query string of this PredicateQuery.
	 */
	public String predicate() {
		return predicate;
	}

}
