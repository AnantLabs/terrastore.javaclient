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
package terrastore.client.legacy;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientRequestFactory;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.spi.ResteasyProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import terrastore.client.Parameters;
import terrastore.client.TerrastoreCommunicationException;
import terrastore.client.TerrastoreRequestException;
import terrastore.client.Values;
import terrastore.client.mapping.JsonBucketsReader;
import terrastore.client.mapping.JsonObjectDescriptor;
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
    public TerrastoreClient(String baseUrl) {
        this(baseUrl, new ArrayList<JsonObjectDescriptor<?>>(0));
    }

    /**
     * Connect to the Terrastore server identified by the given URL, and use the provided
     * {@link JsonObjectDescriptor}s to serialize and deserialize object values.
     *
     * @param baseUrl The Terrastore server address (i.e. http://192.168.1.1:8080).
     * @param descriptors A list of {@link JsonObjectDescriptor}s describing how to serialize
     * and deserialize object values.
     */
    public TerrastoreClient(String baseUrl, List<? extends JsonObjectDescriptor<?>> descriptors) {
        this.baseUrl = baseUrl;
        try {
            ResteasyProviderFactory providerFactory = ResteasyProviderFactory.getInstance();
            // Registration order matters: JsonObjectWriter must come last because writes all:
            providerFactory.addMessageBodyWriter(new JsonParametersWriter());
            providerFactory.addMessageBodyWriter(new JsonObjectWriter(descriptors));
            // Registration order matters: JsonObjectReader must come last because reads all:
            providerFactory.addMessageBodyReader(new JsonValuesReader(descriptors));
            providerFactory.addMessageBodyReader(new JsonBucketsReader());
            providerFactory.addMessageBodyReader(new JsonObjectReader(descriptors));
            //
            registerProviders(providerFactory);
        } catch (Exception ex) {
            throw new IllegalStateException(ex.getMessage(), ex);
        }
    }

    /**
     * Get the name of all buckets.
     *
     * @return A set containing the names of all buckets.
     * @throws TerrastoreRequestException If Terrastore server returns a failure response.
     */
    public Set<String> getBuckets() throws TerrastoreRequestException {
        try {
            String requestUri = UriBuilder.fromUri(baseUrl).build().toString();
            ClientRequest request = requestFactory.createRequest(requestUri);
            ClientResponse response = request.accept(JSON_CONTENT_TYPE).get();
            if (response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                return (Set<String>) response.getEntity(Set.class);
            } else {
                throw new TerrastoreRequestException(response.getResponseStatus().getStatusCode(), response.getEntity(String.class).toString());
            }
        } catch (TerrastoreRequestException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new TerrastoreCommunicationException(ex.getMessage(), ex);
        }
    }

    /**
     * Add a bucket with the given name.
     *
     * @param bucket The bucket name.
     * @throws TerrastoreRequestException If Terrastore server returns a failure response.
     */
    public void addBucket(String bucket) throws TerrastoreRequestException {
        try {
            String requestUri = UriBuilder.fromUri(baseUrl).path(bucket).build().toString();
            ClientRequest request = requestFactory.createRequest(requestUri);
            ClientResponse response = request.accept(JSON_CONTENT_TYPE).put();
            if (!response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                throw new TerrastoreRequestException(response.getResponseStatus().getStatusCode(), response.getEntity(String.class).toString());
            }
        } catch (TerrastoreRequestException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new TerrastoreCommunicationException(ex.getMessage(), ex);
        }
    }

    /**
     * Remove the bucket with the given name.
     *
     * @param bucket The bucket name.
     * @throws TerrastoreRequestException If Terrastore server returns a failure response.
     */
    public void removeBucket(String bucket) throws TerrastoreRequestException {
        try {
            String requestUri = UriBuilder.fromUri(baseUrl).path(bucket).build().toString();
            ClientRequest request = requestFactory.createRequest(requestUri);
            ClientResponse response = request.accept(JSON_CONTENT_TYPE).delete();
            if (!response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                throw new TerrastoreRequestException(response.getResponseStatus().getStatusCode(), response.getEntity(String.class).toString());
            }
        } catch (TerrastoreRequestException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new TerrastoreCommunicationException(ex.getMessage(), ex);
        }
    }

    /**
     * Put an object into the given bucket, under the given key.
     *
     * @param <T> Type of the object to put.
     * @param bucket The bucket name.
     * @param key The object key.
     * @param value The object to put.
     * @throws TerrastoreRequestException If Terrastore server returns a failure response.
     */
    public <T> void putValue(String bucket, String key, T value) throws TerrastoreRequestException {
        try {
            String requestUri = UriBuilder.fromUri(baseUrl).path(bucket).path(key).build().toString();
            ClientRequest request = requestFactory.createRequest(requestUri);
            ClientResponse<T> response = request.body(JSON_CONTENT_TYPE, value).put();
            if (!response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                throw new TerrastoreRequestException(response.getResponseStatus().getStatusCode(), response.getEntity(String.class).toString());
            }
        } catch (TerrastoreRequestException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new TerrastoreCommunicationException(ex.getMessage(), ex);
        }
    }

    /**
     * Remove the object under the given key from the given bucket.
     *
     * @param bucket The bucket name.
     * @param key The object key.
     * @throws TerrastoreRequestException If Terrastore server returns a failure response.
     */
    public void removeValue(String bucket, String key) throws TerrastoreRequestException {
        try {
            String requestUri = UriBuilder.fromUri(baseUrl).path(bucket).path(key).build().toString();
            ClientRequest request = requestFactory.createRequest(requestUri);
            ClientResponse response = request.accept(JSON_CONTENT_TYPE).delete();
            if (!response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                throw new TerrastoreRequestException(response.getResponseStatus().getStatusCode(), response.getEntity(String.class).toString());
            }
        } catch (TerrastoreRequestException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new TerrastoreCommunicationException(ex.getMessage(), ex);
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
     * @throws TerrastoreRequestException If Terrastore server returns a failure response.
     */
    public <T> T getValue(String bucket, String key, Class<T> type) throws TerrastoreRequestException {
        try {
            String requestUri = UriBuilder.fromUri(baseUrl).path(bucket).path(key).build().toString();
            ClientRequest request = requestFactory.createRequest(requestUri);
            ClientResponse<T> response = request.accept(JSON_CONTENT_TYPE).get();
            if (response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                return response.getEntity(type);
            } else {
                throw new TerrastoreRequestException(response.getResponseStatus().getStatusCode(), response.getEntity(String.class).toString());
            }
        } catch (TerrastoreRequestException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new TerrastoreCommunicationException(ex.getMessage(), ex);
        }
    }

    /**
     * Get all values from the given bucket, in random order.<br>
     * It is strongly suggested to set a reasonable limit: returning all values may be a very expensive
     * operation.
     *
     * @param <T> Type of the objects to get (as contained in the given bucket).
     * @param bucket The bucket name.
     * @param limit Max number of elements to retrieve; if zero, all values will be returned.
     * @param type Type of the objects to get (as contained in the given bucket).
     * @return A map of key/value pairs.
     * @throws TerrastoreRequestException If Terrastore server returns a failure response.
     */
    public <T> Values<T> getAllValues(String bucket, int limit, Class<T> type) throws TerrastoreRequestException {
        try {
            String requestUri = UriBuilder.fromUri(baseUrl).path(bucket).queryParam("limit", limit).build().toString();
            ClientRequest request = requestFactory.createRequest(requestUri);
            ClientResponse<T> response = request.accept(JSON_CONTENT_TYPE).get();
            if (response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                return (Values<T>) response.getEntity(Values.class, type);
            } else {
                throw new TerrastoreRequestException(response.getResponseStatus().getStatusCode(), response.getEntity(String.class).toString());
            }
        } catch (TerrastoreRequestException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new TerrastoreCommunicationException(ex.getMessage(), ex);
        }
    }

    /**
     * Execute a range query on the given bucket, selecting all values whose key falls into the given range, as determined
     * by the given comparator (which must be configured on the server side), and which satisfy the given predicate expression.<br>
     * Predicate expression must have the following form: "type:expression", where "type" is the predicate type (which must be configured on the server side),
     * and "expression" is the actual conditional expression to evaluate.
     *
     * @param <T> Type of the objects to get (as contained in the given bucket).
     * @param bucket The bucket name.
     * @param startKey First key in range.
     * @param endKey Last key in range (inclusive); if null, all elements starting from start key and up to the limit will be selected.
     * @param limit Max number of elements to retrieve (even if not reaching the end of the range); if zero, all elements in range will be selected.
     * @param comparator The name of the comparator to use for determining if a key belongs to the range.
     * @param predicate The predicate expression; if null, no predicate will be evaluated.
     * @param timeToLive Number of milliseconds determining how fresh the retrieved data has to be; if set to 0, the query will be immediately computed
     * on current data.
     * @param type Type of the objects to get (as contained in the given bucket).
     * @return A map of key/value pairs.
     * @throws TerrastoreRequestException If Terrastore server returns a failure response.
     */
    public <T> Values<T> doRangeQuery(String bucket, String startKey, String endKey, int limit, String comparator, String predicate, long timeToLive, Class<T> type) throws TerrastoreRequestException {
        try {
            UriBuilder builder = UriBuilder.fromUri(baseUrl).path(bucket).path("range").
                    queryParam("startKey", startKey).
                    queryParam("limit", limit).
                    queryParam("comparator", comparator).
                    queryParam("timeToLive", timeToLive);
            if (endKey != null) {
                builder.queryParam("endKey", endKey);
            }
            if (predicate != null) {
                builder.queryParam("predicate", predicate);
            }
            String requestUri = builder.build().toString();
            ClientRequest request = requestFactory.createRequest(requestUri);
            ClientResponse<T> response = request.accept(JSON_CONTENT_TYPE).get();
            if (response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                return (Values<T>) response.getEntity(Values.class, type);
            } else {
                throw new TerrastoreRequestException(response.getResponseStatus().getStatusCode(), response.getEntity(String.class).toString());
            }
        } catch (TerrastoreRequestException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new TerrastoreCommunicationException(ex.getMessage(), ex);
        }
    }

    /**
     * Execute a predicate-based query returning all key/value pairs whose value satisfies the given predicate.<br>
     * Predicate expression must have the following form: "type:expression", where "type" is the predicate type (which must be configured on the server side),
     * and "expression" is the actual conditional expression to evaluate.
     *
     * @param <T> Type of the objects to get (as contained in the given bucket).
     * @param bucket The bucket name.
     * @param predicate The predicate expression.
     * @param type Type of the objects to get (as contained in the given bucket).
     * @return A map of key/value pairs.
     * @throws TerrastoreRequestException If Terrastore server returns a failure response.
     */
    public <T> Values<T> doPredicateQuery(String bucket, String predicate, Class<T> type) throws TerrastoreRequestException {
        try {
            String requestUri = UriBuilder.fromUri(baseUrl).path(bucket).path("predicate").
                    queryParam("predicate", predicate).
                    build().toString();
            ClientRequest request = requestFactory.createRequest(requestUri);
            ClientResponse<T> response = request.accept(JSON_CONTENT_TYPE).get();
            if (response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                return (Values<T>) response.getEntity(Values.class, type);
            } else {
                throw new TerrastoreRequestException(response.getResponseStatus().getStatusCode(), response.getEntity(String.class).toString());
            }
        } catch (TerrastoreRequestException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new TerrastoreCommunicationException(ex.getMessage(), ex);
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
     * @throws TerrastoreRequestException If Terrastore server returns a failure response.
     */
    public void executeUpdate(String bucket, String key, String function, long timeout, Parameters parameters) throws TerrastoreRequestException {
        try {
            String requestUri = UriBuilder.fromUri(baseUrl).path(bucket).path(key).path("update").queryParam("function", function).queryParam("timeout", timeout).
                    build().toString();
            ClientRequest request = requestFactory.createRequest(requestUri);
            ClientResponse response = request.body(JSON_CONTENT_TYPE, parameters).post();
            if (!response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                throw new TerrastoreRequestException(response.getResponseStatus().getStatusCode(), response.getEntity(String.class).toString());
            }
        } catch (TerrastoreRequestException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new TerrastoreCommunicationException(ex.getMessage(), ex);
        }
    }

    /**
     * Export all key/value entries contained into the given bucket to a file located on the Terrastore node receiveing the export request.<br>
     * In order to perform a backup export, you need to pass along a shared secret key, in order to be sure not to perform
     * an export operation by accident.
     *
     * @param bucket The name of the bucket whose entries must be exported.
     * @param destination The name of the backup file.
     * @param secret The shared secret key.
     * @throws TerrastoreRequestException If Terrastore server returns a failure response.
     */
    public void exportBackup(String bucket, String destination, String secret) throws TerrastoreRequestException {
        try {
            String requestUri = UriBuilder.fromUri(baseUrl).path(bucket).path("export").queryParam("destination", destination).queryParam("secret", secret).
                    build().toString();
            ClientRequest request = requestFactory.createRequest(requestUri);
            ClientResponse response = request.body(JSON_CONTENT_TYPE, "").post();
            if (!response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                throw new TerrastoreRequestException(response.getResponseStatus().getStatusCode(), response.getEntity(String.class).toString());
            }
        } catch (TerrastoreRequestException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new TerrastoreCommunicationException(ex.getMessage(), ex);
        }
    }

    /**
     * Import a backup of key/value entries into the given bucket, from a file located on the Terrastore node receiveing the import request.<br>
     * In order to perform a backup import, you need to pass along a shared secret key, in order to be sure not to perform
     * an import operation by accident.<br>
     * The import operation overwrites duplicated keys, but does not delete missed keys.
     *
     * @param bucket The name of the bucket into which importing backup entries.
     * @param source The name of the backup file.
     * @param secret The shared secret key.
     * @throws TerrastoreRequestException If Terrastore server returns a failure response.
     */
    public void importBackup(String bucket, String source, String secret) throws TerrastoreRequestException {
        try {
            String requestUri = UriBuilder.fromUri(baseUrl).path(bucket).path("import").queryParam("source", source).queryParam("secret", secret).
                    build().toString();
            ClientRequest request = requestFactory.createRequest(requestUri);
            ClientResponse response = request.body(JSON_CONTENT_TYPE, "").post();
            if (!response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                throw new TerrastoreRequestException(response.getResponseStatus().getStatusCode(), response.getEntity(String.class).toString());
            }
        } catch (TerrastoreRequestException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new TerrastoreCommunicationException(ex.getMessage(), ex);
        }
    }
}
