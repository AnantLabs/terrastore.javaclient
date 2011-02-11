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

import java.util.Collections;
import java.util.Map;
import terrastore.client.connection.Connection;

/**
 * @author Sven Johansson
 * @author Sergio Bossa
 *  
 */
public class UpdateOperation extends AbstractOperation {

    private final String bucket;
    private final String key;
    private final String function;
    //
    private volatile Map<String, Object> parameters = Collections.emptyMap();
    private volatile long timeOut;

    public UpdateOperation(Connection connection, String bucket, String key, String function) {
        super(connection);
        this.bucket = bucket;
        this.key = key;
        this.function = function;
    }

    UpdateOperation(UpdateOperation other) {
        super(other.connection);
        this.bucket = other.bucket;
        this.key = other.key;
        this.function = other.function;
        this.parameters = other.parameters;
        this.timeOut = other.timeOut;
    }

    /**
     * Specifies the max number of milliseconds for the update operation to
     * complete successfully.
     *
     * @param timeOut The timeout for this operation, in milliseconds
     */
    public UpdateOperation timeOut(long timeOut) {
        UpdateOperation newInstance = new UpdateOperation(this);
        newInstance.timeOut = timeOut;
        return newInstance;
    }

    /**
     * Specifies update function parameters.
     */
    public UpdateOperation parameters(Map<String, Object> parameters) {
        UpdateOperation newInstance = new UpdateOperation(this);
        newInstance.parameters = parameters;
        return newInstance;
    }

    /**
     * Executes this update operation and returns the updated document, as an instance of the specified
     * Java type.
     * 
     * The server side function must not last more that the given timeout
     * (expressed in milliseconds): otherwise, an exception is thrown and update
     * is aborted.<br>
     * That's because the server side update operation locks the updated value
     * for its duration (in order to provide per-record ACID properties): use of
     * timeouts minimizes livelocks and starvations.
     *
     * @param <T> The Java type for the returned document.
     * @param type The Java class for the returned document.
     * @return The updated document, as an instance of <T>/type
     * @throws TerrastoreClientException
     */
    public <T> T executeAndGet(Class<T> type) throws TerrastoreClientException {
        return connection.executeUpdate(new Context(), type);
    }

    public class Context {

        public String getKey() {
            return key;
        }

        public String getBucket() {
            return bucket;
        }

        public String getFunction() {
            return function;
        }

        public long getTimeOut() {
            return timeOut;
        }

        public Map<String, Object> getParameters() {
            return parameters;
        }
    }
}
