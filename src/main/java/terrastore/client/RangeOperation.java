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
 * A RangeOperation is used to retrieve keys/vales within a specified range.
 * 
 * The RangeOperation requires a specified start of the range. The end of the range
 * is optional. If no end key is specified, all elements starting from start key
 * and up to the limit will be selected.
 * 
 * @author Sven Johansson
 * @author Sergio Bossa
 * @since 2.0
 */
public class RangeOperation extends AbstractOperation {

    private final String bucket;
    private final String comparator;
    //
    private String fromKey;
    private String toKey;
    private String predicate;
    private int limit;
    private long timeToLive;

    /**
     * Sets up a RangeOperation within the specified bucket, using the configured
     * default comparator.
     * 
     * @param bucket The parent BucketOperation.
     * @param connection The Terrastore Connection.
     */
    RangeOperation(Connection connection, String bucket) {
        this(connection, bucket, null);
    }

    /**
     * Sets up a RangeOperation within the specified bucket, using a specific
     * comparator.
     * 
     * @param bucket The parent BucketOperaton.
     * @param connection The Terrastore Connection.
     * @param comparator The name of the comparator to be used for this query.
     */
    RangeOperation(Connection connection, String bucket, String comparator) {
        super(connection);
        this.bucket = bucket;
        this.comparator = comparator;
    }

    /**
     * Specifies the start key of the range.
     * 
     * @param fromKey The first key in the range.
     */
    public RangeOperation from(String fromKey) {
        this.fromKey = fromKey;
        return this;
    }

    /**
     * Specifies the end key of the range.
     * 
     * @param toKey The last key in range (inclusive).
     */
    public RangeOperation to(String toKey) {
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
    public RangeOperation conditionally(String predicate) {
        this.predicate = predicate;
        return this;
    }

    /**
     * Specifies a max limit as to the number of keys/values to retrieve.
     * 
     * @param limit The max amount of values to retrieve
     */
    public RangeOperation limit(int limit) {
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
    public RangeOperation timeToLive(long timeToLive) {
        this.timeToLive = timeToLive;
        return this;
    }

    /**
     * Executes this RangeOperation and returns a Values-map of the specified range
     * selection.
     * 
     * @param <T> The Java type of the values in the current bucket.
     * @param type The Java type of the values in the current bucket.
     * @return A Map of matching keys/values.
     * @throws TerrastoreClientException If the query fails or is incomplete.
     */
    public <T> Values<T> get(Class<T> type) throws TerrastoreClientException {
        return connection.queryByRange(new Context(), type);
    }

    public class Context {

        public String getBucket() {
            return bucket;
        }

        public String getStartKey() {
            return fromKey;
        }

        public String getEndKey() {
            return toKey;
        }

        public int getLimit() {
            return limit;
        }

        public String getComparator() {
            return comparator;
        }

        public long getTimeToLive() {
            return timeToLive;
        }

        public String getPredicate() {
            return predicate;
        }
    }
}
