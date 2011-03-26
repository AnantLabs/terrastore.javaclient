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

import java.util.Set;

import terrastore.client.connection.Connection;

/**
 * @author Sven Johansson
 * @author Sergio Bossa
 *  
 */
public class RangeOperation extends AbstractOperation {

    private final String bucket;
    private final String comparator;
    private volatile String fromKey;
    private volatile String toKey;
    private volatile String predicate;
    private volatile int limit;
    private volatile long timeToLive;

    RangeOperation(Connection connection, String bucket) {
        this(connection, bucket, null);
    }

    RangeOperation(Connection connection, String bucket, String comparator) {
        super(connection);
        this.bucket = bucket;
        this.comparator = comparator;
    }

    RangeOperation(RangeOperation other) {
        super(other.connection);
        this.bucket = other.bucket;
        this.comparator = other.comparator;
        this.fromKey = other.fromKey;
        this.toKey = other.toKey;
        this.limit = other.limit;
        this.predicate = other.predicate;
        this.timeToLive = other.timeToLive;
    }

    /**
     * Specifies the start key of the range.
     * 
     * @param fromKey The first key in the range.
     */
    public RangeOperation from(String fromKey) {
        RangeOperation newInstance = new RangeOperation(this);
        newInstance.fromKey = fromKey;
        return newInstance;
    }

    /**
     * Specifies the end key of the range.
     * 
     * @param toKey The last key in range (inclusive).
     */
    public RangeOperation to(String toKey) {
        RangeOperation newInstance = new RangeOperation(this);
        newInstance.toKey = toKey;
        return newInstance;
    }

    /**
     * Specifies a predicate/conditional for this range query.
     * 
     * @param predicate The predicate value
     */
    public RangeOperation predicate(String predicate) {
        RangeOperation newInstance = new RangeOperation(this);
        newInstance.predicate = predicate;
        return newInstance;
    }

    /**
     * Specifies a max limit as to the number of keys/values to retrieve.
     * 
     * @param limit The max amount of values to retrieve
     */
    public RangeOperation limit(int limit) {
        RangeOperation newInstance = new RangeOperation(this);
        newInstance.limit = limit;
        return newInstance;
    }

    /**
     * Specifies the number of milliseconds determining how fresh the retrieved
     * data has to be; if set to 0 (default), the query will be immediately
     * computed on current data.
     * 
     * @param timeToLive Time to live in milliseconds
     */
    public RangeOperation timeToLive(long timeToLive) {
        RangeOperation newInstance = new RangeOperation(this);
        newInstance.timeToLive = timeToLive;
        return newInstance;
    }

    /**
     * Executes this RangeOperation and returns values from the specified range
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
    
    public Set<String> remove() throws TerrastoreClientException {
        return connection.removeByRange(new Context());
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
