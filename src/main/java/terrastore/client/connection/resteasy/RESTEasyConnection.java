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
package terrastore.client.connection.resteasy;

import static org.jboss.resteasy.plugins.providers.RegisterBuiltin.registerProviders;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientRequestFactory;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.spi.ResteasyProviderFactory;
import terrastore.client.BackupOperation;
import terrastore.client.ConditionalOperation;
import terrastore.client.KeyOperation;
import terrastore.client.PredicateOperation;
import terrastore.client.RangeOperation;
import terrastore.client.TerrastoreClientException;
import terrastore.client.TerrastoreRequestException;
import terrastore.client.UpdateOperation;
import terrastore.client.Values;
import terrastore.client.ValuesOperation;
import terrastore.client.connection.Connection;
import terrastore.client.connection.TerrastoreConnectionException;
import terrastore.client.mapping.JsonBucketsReader;
import terrastore.client.mapping.JsonObjectDescriptor;
import terrastore.client.mapping.JsonObjectReader;
import terrastore.client.mapping.JsonObjectWriter;
import terrastore.client.mapping.JsonParametersWriter;
import terrastore.client.mapping.JsonValuesReader;

/**
 * Handles connections to Terrastore servers using the RESTEasy Client API
 * (http://www.jboss.org/resteasy)
 * 
 * @author Sven Johansson
 * @author Sergio Bossa
 * @since 2.0
 */
public class RESTEasyConnection implements Connection {

    private static final String JSON_CONTENT_TYPE = "application/json";
    private String serverHost;
    private ClientRequestFactory requestFactory = new ClientRequestFactory();

    public RESTEasyConnection(String serverHost, List<? extends JsonObjectDescriptor<?>> descriptors) {
        this.serverHost = serverHost;
        try {
            ResteasyProviderFactory providerFactory = ResteasyProviderFactory.getInstance();
            // Registration order matters: JsonObjectWriter must come last because writes all:
            providerFactory.addMessageBodyWriter(new JsonParametersWriter());
            providerFactory.addMessageBodyWriter(new JsonObjectWriter(descriptors));
            // Registration order matters: JsonObjectReader must come last because reads all:
            providerFactory.addMessageBodyReader(new JsonValuesReader(descriptors));
            providerFactory.addMessageBodyReader(new JsonBucketsReader());
            providerFactory.addMessageBodyReader(new JsonObjectReader(descriptors));

            registerProviders(providerFactory);
        } catch (Exception ex) {
            throw new IllegalStateException(ex.getMessage(), ex);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void removeBucket(String bucket) throws TerrastoreClientException {
        try {
            ClientRequest request = getBucketRequest(bucket);
            ClientResponse<String> response = request.delete();
            if (!response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                throw new TerrastoreRequestException(response.getResponseStatus().getStatusCode(), response.getEntity());
            }
        } catch (TerrastoreClientException e) {
            throw e;
        } catch (Exception e) {
            throw connectionException("Unable to remove bucket", e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Set<String> getBuckets() throws TerrastoreClientException {
        try {
            ClientRequest request = requestFactory.createRequest(serverHost);
            ClientResponse response = request.accept(JSON_CONTENT_TYPE).get();
            if (response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                return (Set<String>) response.getEntity(Set.class);
            } else {
                throw new TerrastoreRequestException(response.getResponseStatus().getStatusCode(), response.getEntity(String.class).toString());
            }
        } catch (TerrastoreClientException e) {
            throw e;
        } catch (Exception e) {
            throw connectionException("Unable to list available buckets", e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> void putValue(KeyOperation.Context context, T value) throws TerrastoreClientException {
        try {
            ClientRequest request = getKeyRequest(context.getBucket(), context.getKey());
            ClientResponse response = request.body(JSON_CONTENT_TYPE, value).put();
            if (!response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                throw new TerrastoreRequestException(response.getResponseStatus().getStatusCode(), response.getEntity(String.class).toString());
            }
        } catch (TerrastoreClientException e) {
            throw e;
        } catch (Exception e) {
            throw connectionException("Unable to put value", e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> void putValue(ConditionalOperation.Context context, T value) throws TerrastoreClientException {
        try {
            String requestUri = UriBuilder.fromUri(serverHost).path(context.getBucket()).path(context.getKey()).queryParam("predicate", context.getPredicate()).
                    build().toString();
            ClientRequest request = requestFactory.createRequest(requestUri);
            ClientResponse response = request.body(JSON_CONTENT_TYPE, value).put();
            if (!response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                throw new TerrastoreRequestException(response.getResponseStatus().getStatusCode(), response.getEntity(String.class).toString());
            }
        } catch (TerrastoreClientException e) {
            throw e;
        } catch (Exception e) {
            throw connectionException("Unable to put value", e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getValue(KeyOperation.Context context, Class<T> type) throws TerrastoreClientException {
        try {
            ClientRequest request = getKeyRequest(context.getBucket(), context.getKey());
            ClientResponse<T> response = request.get();
            if (response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                return response.getEntity(type);
            } else {
                throw new TerrastoreRequestException(response.getResponseStatus().getStatusCode(), response.getEntity(String.class).toString());
            }
        } catch (TerrastoreClientException e) {
            throw e;
        } catch (Exception e) {
            throw connectionException("Unable to get value", e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getValue(ConditionalOperation.Context context, Class<T> type) throws TerrastoreClientException {
        try {
            String requestUri = UriBuilder.fromUri(serverHost).path(context.getBucket()).path(context.getKey()).queryParam("predicate", context.getPredicate()).
                    build().toString();
            ClientRequest request = requestFactory.createRequest(requestUri);
            ClientResponse<T> response = request.get();
            if (response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                return response.getEntity(type);
            } else {
                throw new TerrastoreRequestException(response.getResponseStatus().getStatusCode(), response.getEntity(String.class).toString());
            }
        } catch (TerrastoreClientException e) {
            throw e;
        } catch (Exception e) {
            throw connectionException("Unable to get value", e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void removeValue(KeyOperation.Context context) throws TerrastoreClientException {
        try {
            ClientRequest request = getKeyRequest(context.getBucket(), context.getKey());
            ClientResponse response = request.delete();
            if (!response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                throw new TerrastoreRequestException(response.getResponseStatus().getStatusCode(), response.getEntity(String.class).toString());
            }
        } catch (TerrastoreClientException e) {
            throw e;
        } catch (Exception e) {
            throw connectionException("Unable to remove value", e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Map<String, T> getAllValues(ValuesOperation.Context context, Class<T> type) throws TerrastoreClientException {
        try {
            ClientRequest request = getBucketRequest(context.getBucket()).queryParameter("limit", context.getLimit());
            ClientResponse response = request.get();
            if (response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                return (Values<T>) response.getEntity(Values.class, type);
            } else {
                throw new TerrastoreRequestException(response.getResponseStatus().getStatusCode(), response.getEntity(String.class).toString());
            }
        } catch (TerrastoreClientException e) {
            throw e;
        } catch (Exception e) {
            throw connectionException("Unable to list values", e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Values<T> queryByRange(RangeOperation.Context context, Class<T> type) throws TerrastoreClientException {
        try {
            UriBuilder uriBuilder = UriBuilder.fromUri(serverHost).path(context.getBucket()).path("range").queryParam("startKey", context.getStartKey()).
                    queryParam("limit", context.getLimit()).queryParam("timeToLive", context.getTimeToLive());
            if (null != context.getComparator()) {
                uriBuilder.queryParam("comparator", context.getComparator());
            }
            if (null != context.getEndKey()) {
                uriBuilder.queryParam("endKey", context.getEndKey());
            }
            if (null != context.getPredicate()) {
                uriBuilder.queryParam("predicate", context.getPredicate());
            }
            String requestUri = uriBuilder.build().toString();
            ClientRequest request = requestFactory.createRequest(requestUri);
            ClientResponse<T> response = request.accept(JSON_CONTENT_TYPE).get();
            if (response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                return (Values<T>) response.getEntity(Values.class, type);
            } else {
                throw new TerrastoreRequestException(response.getResponseStatus().getStatusCode(), response.getEntity(String.class).toString());
            }
        } catch (TerrastoreClientException e) {
            throw e;
        } catch (Exception e) {
            throw connectionException("Unable to perform query", e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Values<T> queryByPredicate(PredicateOperation.Context context, Class<T> type) throws TerrastoreClientException {
        try {
            String requestUri = UriBuilder.fromUri(serverHost).path(context.getBucket()).path("predicate").queryParam("predicate", context.getPredicate()).build().
                    toString();

            ClientRequest request = requestFactory.createRequest(requestUri);
            ClientResponse response = request.accept(JSON_CONTENT_TYPE).get();
            if (response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                return (Values<T>) response.getEntity(Values.class, type);
            } else {
                throw new TerrastoreRequestException(response.getResponseStatus().getStatusCode(), response.getEntity(String.class).toString());
            }
        } catch (TerrastoreClientException e) {
            throw e;
        } catch (Exception e) {
            throw connectionException("Unable to perform query", e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void exportBackup(BackupOperation.Context context) throws TerrastoreClientException {
        try {
            String requestUri = UriBuilder.fromUri(serverHost).path(context.getBucket()).path("export").queryParam("destination", context.getFileName()).
                    queryParam("secret", context.getSecretKey()).build().toString();
            ClientRequest request = requestFactory.createRequest(requestUri);
            ClientResponse response = request.body(JSON_CONTENT_TYPE, "").post();
            if (!response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                throw new TerrastoreRequestException(response.getResponseStatus().getStatusCode(), response.getEntity(String.class).toString());
            }
        } catch (TerrastoreClientException e) {
            throw e;
        } catch (Exception e) {
            throw connectionException("Unable to export backup", e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void importBackup(BackupOperation.Context context) throws TerrastoreClientException {
        try {
            String requestUri = UriBuilder.fromUri(serverHost).path(context.getBucket()).path("import").queryParam("source", context.getFileName()).queryParam("secret", context.
                    getSecretKey()).build().toString();
            ClientRequest request = requestFactory.createRequest(requestUri);
            ClientResponse response = request.body(JSON_CONTENT_TYPE, "").post();
            if (!response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                throw new TerrastoreRequestException(response.getResponseStatus().getStatusCode(), response.getEntity(String.class).toString());
            }
        } catch (TerrastoreClientException e) {
            throw e;
        } catch (Exception e) {
            throw connectionException("Unable to import backup", e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void executeUpdate(UpdateOperation.Context context) throws TerrastoreClientException {
        try {
            String requestUri = UriBuilder.fromUri(serverHost).path(context.getBucket()).path(context.getKey()).path("update").queryParam("function", context.
                    getFunction()).queryParam("timeout", context.getTimeOut()).build().toString();

            ClientRequest request = requestFactory.createRequest(requestUri);
            ClientResponse response = request.body(JSON_CONTENT_TYPE, context.getParameters()).post();
            if (!response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                throw new TerrastoreRequestException(response.getResponseStatus().getStatusCode(), response.getEntity(String.class).toString());
            }
        } catch (TerrastoreClientException e) {
            throw e;
        } catch (Exception e) {
            throw connectionException("Unable to perform update", e);
        }
    }

    private ClientRequest getKeyRequest(String bucket, String key) {
        String requestUri = UriBuilder.fromUri(serverHost).path(bucket).path(key).build().toString();
        ClientRequest request = requestFactory.createRequest(requestUri);
        return request.accept(JSON_CONTENT_TYPE);
    }

    private ClientRequest getBucketRequest(String bucket) {
        String requestUri = UriBuilder.fromUri(serverHost).path(bucket).build().toString();
        ClientRequest request = requestFactory.createRequest(requestUri);
        return request.accept(JSON_CONTENT_TYPE);
    }

    private TerrastoreClientException connectionException(String message, Exception e) {
        return new TerrastoreConnectionException(message, serverHost, e);
    }
}
