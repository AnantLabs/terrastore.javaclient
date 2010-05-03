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
 * A RangeQuery is used to retrieve keys/vales within a specified range.
 * 
 * The RangeQuery requires a specified start of the range. The end of the range
 * is optional. If no end key is specified, all elements starting from start key
 * and up to the limit will be selected.
 * 
 * @author Sven Johansson
 * @date 24 apr 2010
 * @since 2.0
 */
public class RangeQuery extends AbstractBucketOperation {

    private String comparator;
    private String fromKey;
    private String toKey;
    private String predicate;
    private int limit;
    private long timeToLive;

    /**
     * Sets up a RangeQuery within the specified bucket, using the configured
     * default comparator.
     * 
     * @param bucket The parent BucketOperation.
     * @param connection The Terrastore Connection.
     */
    public RangeQuery(BucketOperation bucket, Connection connection) {
        super(bucket, connection);
    }

    /**
     * Sets up a RangeQuery within the specified bucket, using a specific
     * comparator.
     * 
     * @param bucket The parent BucketOperaton.
     * @param connection The Terrastore Connection.
     * @param comparator The name of the comparator to be used for this query.
     */
    public RangeQuery(BucketOperation bucket, Connection connection,
            String comparator) {
        super(bucket, connection);
        this.comparator = comparator;
    }

    /**
     * Specifies the start key of the range.
     * 
     * @param fromKey The first key in the range.
     */
    public RangeQuery from(String fromKey) {
        this.fromKey = fromKey;
        return this;
    }

    /**
     * Specifies the end key of the range.
     * 
     * @param toKey The last key in range (inclusive).
     */
    public RangeQuery to(String toKey) {
        this.toKey = toKey;
        return this;
    }

    /**
     * Specifies a predicate/conditional for this range query.
     * 
     * TODO: Document predicates
     * 
     * @param predicate The predicate value
     */
    public RangeQuery conditionally(String predicate) {
        this.predicate = predicate;
        return this;
    }

    /**
     * Specifies a max limit as to the number of keys/values to retrieve.
     * 
     * @param limit The max amount of values to retrieve
     */
    public RangeQuery limit(int limit) {
        this.limit = limit;
        return this;
    }

    /**
     * Specifies the number of milliseconds determining how fresh the retrieved
     * data has to be; if set to 0 (default), the query will be immediately
     * computed on current data.
     * 
     * @param timeToLive Time to live in milliseconds
     */
    public RangeQuery timeToLive(long timeToLive) {
        this.timeToLive = timeToLive;
        return this;
    }

    /**
     * Executes this RangeQuery and returns a Values-map of the specified range
     * selection.
     * 
     * @param <T> The Java type of the values in the current bucket.
     * @param type The Java type of the values in the current bucket.
     * @return A Map of matching keys/values.
     * @throws TerrastoreClientException If the query fails or is incomplete.
     */
    public <T> Values<T> get(Class<T> type) throws TerrastoreClientException {
        return connection.doRangeQuery(this, type);
    }

    /**
     * @return The "from" key of this range.
     */
    public String from() {
        return fromKey;
    }

    /**
     * @return The "to" key of this range is one is specified, otherwise
     *         <code>null</code>
     */
    public String to() {
        return toKey;
    }

    /**
     * @return The max amount of values that this query will return.
     */
    public int limit() {
        return limit;
    }

    public String comparator() {
        return comparator;
    }

    public long timeToLive() {
        return timeToLive;
    }

    public String predicate() {
        return predicate;
    }

}
