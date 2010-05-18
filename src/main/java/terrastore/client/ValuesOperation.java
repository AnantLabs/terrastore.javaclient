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
    private volatile int limit;

    ValuesOperation(Connection connection, String bucket) {
        super(connection);
        if (null == bucket) {
            throw new IllegalArgumentException("Bucket name cannot be null.");
        }
        this.bucket = bucket;
    }

    ValuesOperation(ValuesOperation other) {
        super(other.connection);
        this.bucket = other.bucket;
        this.limit = other.limit;
    }

    public ValuesOperation limit(int limit) {
        ValuesOperation newInstance = new ValuesOperation(this);
        newInstance.limit = limit;
        return newInstance;
    }

    public <T> Map<String, T> get(Class<T> type) throws TerrastoreClientException {
        return connection.getAllValues(new Context(), type);
    }

    public class Context {

        public String getBucket() {
            return bucket;
        }

        public int getLimit() {
            return limit;
        }
    }
}
