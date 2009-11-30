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
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientRequestFactory;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.spi.ResteasyProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.client.mapping.JsonValuesReader;
import terrastore.client.mapping.JsonObjectReader;
import terrastore.client.mapping.JsonObjectWriter;
import terrastore.client.mapping.JsonParametersWriter;
import static org.jboss.resteasy.plugins.providers.RegisterBuiltin.*;

/**
 * Terrastore client for sending request to a Terrastore cluster.<br>
 * You only need to know and connect to only a single Terrastore server: the server itslef will then manage requests and
 * route them to other servers if needed.
 *
 * @author Sergio Bossa
 */
public class TerrastoreClient {

    private static final String JSON_CONTENT_TYPE = "application/json";
    private static final Logger LOG = LoggerFactory.getLogger(TerrastoreClient.class);
    private final ClientRequestFactory requestFactory = new ClientRequestFactory();
    private final String baseUrl;

    /**
     * Connect to the Terrastore server identified by the given URL.
     *
     * @param baseUrl The Terrastore server address (i.e. http://192.168.1.1:8080).
     */
    public TerrastoreClient(String baseUrl) throws Exception {
        this(baseUrl, new ArrayList<JsonObjectDescriptor>(0));
    }

    /**
     * Connect to the Terrastore server identified by the given URL, and use the provided
     * {@link JsonObjectDescriptor}s to serialize and deserialize object values.
     *
     * @param baseUrl The Terrastore server address (i.e. http://192.168.1.1:8080).
     * @param descriptors A list of {@link JsonObjectDescriptor}s describing how to serialize
     * and deserialize object values.
     */
    public TerrastoreClient(String baseUrl, List<? extends JsonObjectDescriptor> descriptors) throws Exception {
        this.baseUrl = baseUrl;
        //
        ResteasyProviderFactory providerFactory = ResteasyProviderFactory.getInstance();
        // Registration order matters: JsonObjectWriter must come last because writes all:
        providerFactory.addMessageBodyWriter(new JsonParametersWriter());
        providerFactory.addMessageBodyWriter(new JsonObjectWriter(descriptors));
        // Registration order matters: JsonObjectReader must come last because reads all:
        providerFactory.addMessageBodyReader(new JsonValuesReader(descriptors));
        providerFactory.addMessageBodyReader(new JsonObjectReader(descriptors));
        //
        registerProviders(providerFactory);
    }

    /**
     * Add a bucket with the given name.
     *
     * @param bucket The bucket name.
     * @throws Exception If something wrong happens while connecting/interacting with the Terrastore server.
     * @throws TerrastoreRequestException If Terrastore server returns a failure response.
     */
    public void addBucket(String bucket) throws Exception, TerrastoreRequestException {
        String requestUri = UriBuilder.fromUri(baseUrl).path(bucket).build().toString();
        ClientRequest request = requestFactory.createRequest(requestUri);
        ClientResponse response = request.accept(JSON_CONTENT_TYPE).put();
        if (!response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
            throw new TerrastoreRequestException(response.getResponseStatus().getStatusCode(), response.getEntity(String.class).toString());
        }
    }

    /**
     * Remove the bucket with the given name.
     *
     * @param bucket The bucket name.
     * @throws Exception If something wrong happens while connecting/interacting with the Terrastore server.
     * @throws TerrastoreRequestException If Terrastore server returns a failure response.
     */
    public void removeBucket(String bucket) throws Exception, TerrastoreRequestException {
        String requestUri = UriBuilder.fromUri(baseUrl).path(bucket).build().toString();
        ClientRequest request = requestFactory.createRequest(requestUri);
        ClientResponse response = request.accept(JSON_CONTENT_TYPE).delete();
        if (!response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
            throw new TerrastoreRequestException(response.getResponseStatus().getStatusCode(), response.getEntity(String.class).toString());
        }
    }

    /**
     * Put an object into the given bucket, under the given key.
     *
     * @param <T> Type of the object to put.
     * @param bucket The bucket name.
     * @param key The object key.
     * @param value The object to put.
     * @throws Exception If something wrong happens while connecting/interacting with the Terrastore server.
     * @throws TerrastoreRequestException If Terrastore server returns a failure response.
     */
    public <T> void putValue(String bucket, String key, T value) throws Exception, TerrastoreRequestException {
        String requestUri = UriBuilder.fromUri(baseUrl).path(bucket).path(key).build().toString();
        ClientRequest request = requestFactory.createRequest(requestUri);
        ClientResponse<T> response = request.body(JSON_CONTENT_TYPE, value).put();
        if (!response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
            throw new TerrastoreRequestException(response.getResponseStatus().getStatusCode(), response.getEntity(String.class).toString());
        }
    }

    /**
     * Remove the object under the given key from the given bucket.
     *
     * @param bucket The bucket name.
     * @param key The object key.
     * @throws Exception If something wrong happens while connecting/interacting with the Terrastore server.
     * @throws TerrastoreRequestException If Terrastore server returns a failure response.
     */
    public void removeValue(String bucket, String key) throws Exception, TerrastoreRequestException {
        String requestUri = UriBuilder.fromUri(baseUrl).path(bucket).path(key).build().toString();
        ClientRequest request = requestFactory.createRequest(requestUri);
        ClientResponse response = request.accept(JSON_CONTENT_TYPE).delete();
        if (!response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
            throw new TerrastoreRequestException(response.getResponseStatus().getStatusCode(), response.getEntity(String.class).toString());
        }
    }

    /**
     * Get the object under the given key from the given bucket.
     *
     * @param <T> Type of the object to get.
     * @param bucket The bucket name.
     * @param key The object key.
     * @param type type of the object to get.
     * @return The value.
     * @throws Exception If something wrong happens while connecting/interacting with the Terrastore server.
     * @throws TerrastoreRequestException If Terrastore server returns a failure response.
     */
    public <T> T getValue(String bucket, String key, Class<T> type) throws Exception, TerrastoreRequestException {
        String requestUri = UriBuilder.fromUri(baseUrl).path(bucket).path(key).build().toString();
        ClientRequest request = requestFactory.createRequest(requestUri);
        ClientResponse<T> response = request.accept(JSON_CONTENT_TYPE).get();
        if (response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
            return response.getEntity(type);
        } else {
            throw new TerrastoreRequestException(response.getResponseStatus().getStatusCode(), response.getEntity(String.class).toString());
        }
    }

    /**
     * Get all values from the given bucket.
     *
     * @param <T> Type of the objects to get (as contained in the given bucket).
     * @param bucket The bucket name.
     * @param type Type of the objects to get (as contained in the given bucket).
     * @return A map of key/value pairs.
     * @throws Exception If something wrong happens while connecting/interacting with the Terrastore server.
     * @throws TerrastoreRequestException If Terrastore server returns a failure response.
     */
    public <T> Values<T> getAllValues(String bucket, Class<T> type) throws Exception, TerrastoreRequestException {
        String requestUri = UriBuilder.fromUri(baseUrl).path(bucket).build().toString();
        ClientRequest request = requestFactory.createRequest(requestUri);
        ClientResponse<T> response = request.accept(JSON_CONTENT_TYPE).get();
        if (response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
            return (Values<T>) response.getEntity(Values.class, type);
        } else {
            throw new TerrastoreRequestException(response.getResponseStatus().getStatusCode(), response.getEntity(String.class).toString());
        }
    }

    /**
     * Execute a range query on the given bucket, selecting all values whose key falls into the given range as determined
     * by the given comparator (which must be configured on the server side).
     *
     * @param <T> Type of the objects to get (as contained in the given bucket).
     * @param bucket The bucket name.
     * @param startKey First key in range.
     * @param endKey Last key in range (comprised).
     * @param comparator The name of the comparator to use for determining if a key belongs to the range.
     * @param timeToLive Number of milliseconds determining how fresh the retrieved data has to be; if set to 0, the query will be immediately computed
     * on current data.
     * @param type Type of the objects to get (as contained in the given bucket).
     * @return A map of key/value pairs.
     * @throws Exception If something wrong happens while connecting/interacting with the Terrastore server.
     * @throws TerrastoreRequestException If Terrastore server returns a failure response.
     */
    public <T> Values<T> doRangeQuery(String bucket, String startKey, String endKey, String comparator, long timeToLive, Class<T> type) throws Exception, TerrastoreRequestException {
        String requestUri = UriBuilder.fromUri(baseUrl).path(bucket).path("range").queryParam("startKey", startKey).queryParam("endKey", endKey).queryParam("comparator", comparator).
                queryParam("timeToLive", timeToLive).
                build().toString();
        ClientRequest request = requestFactory.createRequest(requestUri);
        ClientResponse<T> response = request.accept(JSON_CONTENT_TYPE).get();
        if (response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
            return (Values<T>) response.getEntity(Values.class, type);
        } else {
            throw new TerrastoreRequestException(response.getResponseStatus().getStatusCode(), response.getEntity(String.class).toString());
        }
    }

    /**
     * Execute an update on the value under the given key from the given bucket.<br>
     * The update operation is implemented as a server side function
     * with the given parameters map; the server side function must not last more that the given timeout (expressed in milliseconds): otherwise,
     * an exception is thrown and update is aborted.<br>
     * That's because the server side update operation locks the updated value for its duration (in order to provide per-record ACID properties): use of timeouts
     * minimizes livelocks and starvations.
     *
     * @param bucket The bucket name.
     * @param key The object key.
     * @param function The server side function to invoke for actually performing the update.
     * @param timeout The max number of milliseconds for the update operation to complete successfully.
     * @param parameters The function parameters as a json associative array.
     * @throws Exception If something wrong happens while connecting/interacting with the Terrastore server.
     * @throws TerrastoreRequestException If Terrastore server returns a failure response.
     */
    public void executeUpdate(String bucket, String key, String function, long timeout, Parameters parameters) throws Exception, TerrastoreRequestException {
        String requestUri = UriBuilder.fromUri(baseUrl).path(bucket).path(key).path("update").queryParam("function", function).queryParam("timeout", timeout).
                build().toString();
        ClientRequest request = requestFactory.createRequest(requestUri);
        ClientResponse response = request.body(JSON_CONTENT_TYPE, parameters).post();
        if (!response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
            throw new TerrastoreRequestException(response.getResponseStatus().getStatusCode(), response.getEntity(String.class).toString());
        }
    }
}
