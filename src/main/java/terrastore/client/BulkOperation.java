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

import java.util.Map;
import java.util.Set;
import terrastore.client.connection.Connection;

/**
 * @author Sergio Bossa
 *  
 */
public class BulkOperation extends AbstractOperation {

    private final String bucket;

    BulkOperation(Connection connection, String bucket) {
        super(connection);
        this.bucket = bucket;
    }

    public <T> Values<T> get(Set<String> keys, Class<T> type) throws TerrastoreClientException {
        return connection.bulkGet(new Context(keys), type);
    }

    public <T> Set<String> put(Values<T> values) throws TerrastoreClientException {
        return connection.bulkPut(new Context(values));
    }

    public class Context {

        private final Set keys;
        private final Values values;

        public Context(Set keys) {
            this.keys = keys;
            this.values = null;
        }

        public Context(Values values) {
            this.keys = null;
            this.values = values;
        }

        public String getBucket() {
            return bucket;
        }

        public Set getKeys() {
            return keys;
        }

        public Map getValues() {
            return values;
        }

    }
}
