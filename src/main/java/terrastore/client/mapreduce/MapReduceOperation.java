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
package terrastore.client.mapreduce;

import terrastore.client.connection.Connection;

public class MapReduceOperation {

    private final String bucket;
    private final MapReduceQuery query;
    private final Connection connection;

    public MapReduceOperation(Connection connection, String bucket, MapReduceQuery query) {
        this.bucket = bucket;
        this.query = query;
        this.connection = connection;
    }

    public <T> T execute(Class<T> returnType) {
        return connection.queryByMapReduce(new Context(), returnType);
    }

    public class Context {

        public MapReduceQuery getQuery() {
            return query;
        }

        public String getBucket() {
            return bucket;
        }

    }
}
