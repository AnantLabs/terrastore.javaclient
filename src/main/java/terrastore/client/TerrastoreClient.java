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

import java.util.ArrayList;
import java.util.List;

import terrastore.client.connection.Connection;
import terrastore.client.connection.ConnectionFactory;
import terrastore.client.connection.resteasy.HTTPConnectionFactory;
import terrastore.client.mapping.JsonObjectDescriptor;

/**
 * Terrastore Client for communicating with a Terrastore server cluster.
 * 
 * You only need to know and connect to a single Terrastore server, as the server
 * itself will manage requests and route them to other servers in the cluster,
 * if needed.
 * 
 * Terrastore Client instances are immutable.
 * 
 * @author Sven Johansson
 * @author Sergio Bossa
 * @since 2.0
 */
public class TerrastoreClient {

    private final Connection connection;

    /**
     * Connects to the Terrastore server identified by the provided
     * serverHost/url using the type of connection provided by the
     * ConnectionFactory.
     * 
     * @param serverHost The host name of the Terrastore server, i.e http://localhost:8080
     * @param connectionFactory A ConnectionFactory instance, typically {@link HTTPConnectionFactory}
     * @throws TerrastoreClientException If the provided arguments are invalid.
     */
    public TerrastoreClient(String serverHost, ConnectionFactory connectionFactory) throws TerrastoreClientException {
        this(serverHost, connectionFactory, new ArrayList<JsonObjectDescriptor<?>>(0));
    }

    /**
     * Connects to the Terrastore server identified by the provided
     * serverHost/url using the type of connection provided by the
     * ConnectionFactory.
     * 
     * A List of {@link JsonObjectDescriptor} is used to override the default
     * behaviour of serialization/deserialization between java objects and JSON.
     * 
     * @param serverHost The host name of the Terrastore server, i.e http://localhost:8080
     * @param connectionFactory A ConnectionFactory instance, typically {@link HTTPConnectionFactory}
     * @param descriptors Serialization/deserialization instructions.
     * @throws TerrastoreClientException If the provided arguments are invalid.
     */
    public TerrastoreClient(String serverHost, ConnectionFactory connectionFactory, List<? extends JsonObjectDescriptor<?>> descriptors) throws TerrastoreClientException {
        if (null == serverHost) {
            throw new IllegalArgumentException(
                    "Cannot establish connection to null server URL");
        }
        this.connection = connectionFactory.makeConnection(serverHost, descriptors);
    }

    /**
     * Sets up a {@link BucketOperation} instance for access to a bucket of the provided name.
     * If a bucket does not exist, it is implicitly created when written to. 
     * 
     * @param bucketName The name of the bucket to access.
     * @return A {@link BucketOperation} instance for the specified bucket name.
     */
    public BucketOperation bucket(String bucketName) {
        return new BucketOperation(connection, bucketName);
    }

    /**
     * Sets up a {@link BucketsOperation} for access to all currently existing
     * buckets on the Terrastore server.
     * 
     * @return A {@link BucketsOperation}.
     */
    public BucketsOperation buckets() {
        return new BucketsOperation(connection);
    }
}
