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
package terrastore.client.merge;

import terrastore.client.TerrastoreClientException;
import terrastore.client.connection.Connection;

/**
 * @author Sergio Bossa
 */
public class MergeOperation {

    private final Connection connection;
    private final String bucket;
    private final String key;
    private final MergeDescriptor descriptor;

    public MergeOperation(Connection connection, String bucket, String key, MergeDescriptor descriptor) {
        this.connection = connection;
        this.bucket = bucket;
        this.key = key;
        this.descriptor = descriptor;
    }

    /**
     * Executes the merge update, and returns the updated document as an instance of the specified type.
     *
     * @param <T> The Java type for the returned document.
     * @param type The Java class for the returned document.
     * @return The updated document, as an instance of <T>/type
     * @throws TerrastoreClientException
     */
    public <T> T executeAndGet(Class<T> returnType) throws TerrastoreClientException {
        return connection.executeMerge(new Context(), returnType);
    }

    public class Context {

        public String getBucket() {
            return bucket;
        }

        public String getKey() {
            return key;
        }

        public MergeDescriptor getDescriptor() {
            return descriptor;
        }

    }
}
