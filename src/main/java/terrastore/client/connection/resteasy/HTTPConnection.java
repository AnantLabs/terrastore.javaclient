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
package terrastore.client.connection.resteasy;

import static org.jboss.resteasy.plugins.providers.RegisterBuiltin.registerProviders;

import java.net.ConnectException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientRequestFactory;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.client.core.executors.ApacheHttpClientExecutor;
import org.jboss.resteasy.spi.ResteasyProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import terrastore.client.BackupOperation;
import terrastore.client.BulkOperation.Context;
import terrastore.client.ClusterStats;
import terrastore.client.ConditionalOperation;
import terrastore.client.KeyOperation;
import terrastore.client.PredicateOperation;
import terrastore.client.RangeOperation;
import terrastore.client.TerrastoreClientException;
import terrastore.client.UpdateOperation;
import terrastore.client.Values;
import terrastore.client.ValuesOperation;
import terrastore.client.connection.Connection;
import terrastore.client.connection.HostManager;
import terrastore.client.connection.TerrastoreConnectionException;
import terrastore.client.connection.resteasy.ExceptionTranslator.Operation;
import terrastore.client.mapping.JsonClusterStatsReader;
import terrastore.client.mapping.JsonObjectDescriptor;
import terrastore.client.mapping.JsonObjectReader;
import terrastore.client.mapping.JsonObjectWriter;
import terrastore.client.mapping.JsonParametersWriter;
import terrastore.client.mapping.JsonValuesReader;
import terrastore.client.mapping.JsonValuesWriter;
import terrastore.client.mapreduce.MapReduceOperation;
import terrastore.client.merge.MergeOperation;

/**
 * Handles connections to Terrastore servers using the RESTEasy Client API
 * (http://www.jboss.org/resteasy)
 * 
 * @author Sven Johansson
 * @author Sergio Bossa
 *  
 */
public class HTTPConnection implements Connection {

    private static final Logger LOG = LoggerFactory.getLogger(HTTPConnection.class);
    private static final String JSON_CONTENT_TYPE = "application/json";
    //
    private final HostManager hostManager;
    private final ClientRequestFactory requestFactory;
    private final ExceptionTranslator exceptionTranslator = new ExceptionTranslator();

    public HTTPConnection(HostManager hostManager, List<JsonObjectDescriptor<?>> descriptors) {
        this(hostManager, descriptors, new HttpClient(new MultiThreadedHttpConnectionManager()));
    }

    public HTTPConnection(HostManager hostManager, List<JsonObjectDescriptor<?>> descriptors, HttpClient httpClient) {
        ResteasyProviderFactory providerFactory = ResteasyProviderFactory.getInstance();
        this.hostManager = hostManager;
        this.requestFactory = new ClientRequestFactory(new ApacheHttpClientExecutor(httpClient), providerFactory);
        try {
            // Registration order matters: JsonObjectWriter must come last because writes all:
            providerFactory.addMessageBodyWriter(new JsonParametersWriter());
            providerFactory.addMessageBodyWriter(new JsonValuesWriter(descriptors));
            providerFactory.addMessageBodyWriter(new JsonObjectWriter(descriptors));
            // Registration order matters: JsonObjectReader must come last because reads all:
            providerFactory.addMessageBodyReader(new JsonClusterStatsReader());
            providerFactory.addMessageBodyReader(new JsonValuesReader(descriptors));
            providerFactory.addMessageBodyReader(new JsonObjectReader(descriptors));

            registerProviders(providerFactory);
        } catch (Exception ex) {
            throw new IllegalStateException(ex.getMessage(), ex);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public ClusterStats getClusterStats() throws TerrastoreClientException {
        String serverHost = hostManager.getHost();
        ClientRequest request = null;
        ClientResponse<ClusterStats> response = null;
        try {
            request = getStatsRequest(serverHost, "cluster");
            response = request.get();
            if (response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                return response.getEntity(ClusterStats.class);
            } else {
                throw exceptionTranslator.generalException(response);
            }
        } catch (TerrastoreClientException e) {
            throw e;
        } catch (Exception e) {
            throw getClientSideException(serverHost, e);
        } finally {
            if (response != null) {
                response.releaseConnection();
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void clearBucket(String bucket) throws TerrastoreClientException {
        String serverHost = hostManager.getHost();
        ClientRequest request = null;
        ClientResponse<String> response = null;
        try {
            request = getBucketRequest(serverHost, bucket);
            response = request.delete();
            if (!response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                throw exceptionTranslator.generalException(response);
            }
        } catch (TerrastoreClientException e) {
            throw e;
        } catch (Exception e) {
            throw getClientSideException(serverHost, e);
        } finally {
            if (response != null) {
                response.releaseConnection();
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Set<String> getBuckets() throws TerrastoreClientException {
        String serverHost = hostManager.getHost();
        ClientRequest request = null;
        ClientResponse<Set<String>> response = null;
        try {
            request = requestFactory.createRequest(serverHost);
            response = request.accept(JSON_CONTENT_TYPE).get();
            if (response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                return response.getEntity(Set.class);
            } else {
                throw exceptionTranslator.generalException(response);
            }
        } catch (TerrastoreClientException e) {
            throw e;
        } catch (Exception e) {
            throw getClientSideException(serverHost, e);
        } finally {
            if (response != null) {
                response.releaseConnection();
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> void putValue(KeyOperation.Context context, T value) throws TerrastoreClientException {
        String serverHost = hostManager.getHost();
        ClientRequest request = null;
        ClientResponse response = null;
        try {
            request = getKeyRequest(serverHost, context.getBucket(), context.getKey());
            response = request.body(JSON_CONTENT_TYPE, value).put();
            if (!response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                throw exceptionTranslator.generalException(response);
            }
        } catch (TerrastoreClientException e) {
            throw e;
        } catch (Exception e) {
            throw getClientSideException(serverHost, e);
        } finally {
            if (response != null) {
                response.releaseConnection();
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> void putValue(ConditionalOperation.Context context, T value) throws TerrastoreClientException {
        String serverHost = hostManager.getHost();
        ClientRequest request = null;
        ClientResponse response = null;
        try {
            String requestUri = UriBuilder.fromUri(serverHost).path(context.getBucket()).path(context.getKey()).queryParam("predicate", context.getPredicate()).
                    build().toString();
            request = requestFactory.createRequest(requestUri);
            response = request.body(JSON_CONTENT_TYPE, value).put();
            if (!response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                throw exceptionTranslator.translate(Operation.CONDITIONAL, response);
            }
        } catch (TerrastoreClientException e) {
            throw e;
        } catch (Exception e) {
            throw getClientSideException(serverHost, e);
        } finally {
            if (response != null) {
                response.releaseConnection();
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getValue(KeyOperation.Context context, Class<T> type) throws TerrastoreClientException {
        String serverHost = hostManager.getHost();
        ClientRequest request = null;
        ClientResponse<T> response = null;
        try {
            request = getKeyRequest(serverHost, context.getBucket(), context.getKey());
            response = request.get();
            if (response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                return response.getEntity(type);
            } else {
                throw exceptionTranslator.translate(Operation.GET, response);
            }
        } catch (TerrastoreClientException e) {
            throw e;
        } catch (Exception e) {
            throw getClientSideException(serverHost, e);
        } finally {
            if (response != null) {
                response.releaseConnection();
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getValue(ConditionalOperation.Context context, Class<T> type) throws TerrastoreClientException {
        String serverHost = hostManager.getHost();
        ClientRequest request = null;
        ClientResponse<T> response = null;
        try {
            String requestUri = UriBuilder.fromUri(serverHost).path(context.getBucket()).path(context.getKey()).queryParam("predicate", context.getPredicate()).
                    build().toString();
            request = requestFactory.createRequest(requestUri);
            response = request.get();
            if (response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                return response.getEntity(type);
            } else {
                throw exceptionTranslator.translate(Operation.CONDITIONAL, response);
            }
        } catch (TerrastoreClientException e) {
            throw e;
        } catch (Exception e) {
            throw getClientSideException(serverHost, e);
        } finally {
            if (response != null) {
                response.releaseConnection();
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void removeValue(KeyOperation.Context context) throws TerrastoreClientException {
        String serverHost = hostManager.getHost();
        ClientRequest request = null;
        ClientResponse response = null;
        try {
            request = getKeyRequest(serverHost, context.getBucket(), context.getKey());
            response = request.delete();
            if (!response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                throw exceptionTranslator.generalException(response);
            }
        } catch (TerrastoreClientException e) {
            throw e;
        } catch (Exception e) {
            throw getClientSideException(serverHost, e);
        } finally {
            if (response != null) {
                response.releaseConnection();
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Values<T> getAllValues(ValuesOperation.Context context, Class<T> type) throws TerrastoreClientException {
        String serverHost = hostManager.getHost();
        ClientRequest request = null;
        ClientResponse<Values<T>> response = null;
        try {
            request = getBucketRequest(serverHost, context.getBucket()).queryParameter("limit", context.getLimit());
            response = request.get();
            if (response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                return response.getEntity(Values.class, type);
            } else {
                throw exceptionTranslator.generalException(response);
            }
        } catch (TerrastoreClientException e) {
            throw e;
        } catch (Exception e) {
            throw getClientSideException(serverHost, e);
        } finally {
            if (response != null) {
                response.releaseConnection();
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Values<T> queryByRange(RangeOperation.Context context, Class<T> type) throws TerrastoreClientException {
        String serverHost = hostManager.getHost();
        ClientRequest request = null;
        ClientResponse<Values<T>> response = null;
        try {
            String requestUri = buildRangeURI(context, serverHost);
            request = requestFactory.createRequest(requestUri);
            response = request.accept(JSON_CONTENT_TYPE).get();
            if (response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                return response.getEntity(Values.class, type);
            } else {
                throw exceptionTranslator.generalException(response);
            }
        } catch (TerrastoreClientException e) {
            throw e;
        } catch (Exception e) {
            throw getClientSideException(serverHost, e);
        } finally {
            if (response != null) {
                response.releaseConnection();
            }
        }
    }

    @Override
    public Set<String> removeByRange(RangeOperation.Context context) throws TerrastoreClientException {
        String serverHost = hostManager.getHost();
        ClientRequest request = null;
        ClientResponse<String> response = null;
        try {
            String requestUri = buildRangeURI(context, serverHost);
            request = requestFactory.createRequest(requestUri);
            response = request.accept(JSON_CONTENT_TYPE).delete();
            if (response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                return response.getEntity(HashSet.class);
            } else {
                throw exceptionTranslator.generalException(response);
            }
        } catch (TerrastoreClientException e) {
            throw e;
        } catch (Exception e) {
            throw getClientSideException(serverHost, e);
        } finally {
            if (response != null) {
                response.releaseConnection();
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Values<T> queryByPredicate(PredicateOperation.Context context, Class<T> type) throws TerrastoreClientException {
        String serverHost = hostManager.getHost();
        ClientRequest request = null;
        ClientResponse<Values<T>> response = null;
        try {
            String requestUri = UriBuilder.fromUri(serverHost).path(context.getBucket()).path("predicate").queryParam("predicate", context.getPredicate()).build().
                    toString();

            request = requestFactory.createRequest(requestUri);
            response = request.accept(JSON_CONTENT_TYPE).get();
            if (response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                return response.getEntity(Values.class, type);
            } else {
                throw exceptionTranslator.generalException(response);
            }
        } catch (TerrastoreClientException e) {
            throw e;
        } catch (Exception e) {
            throw getClientSideException(serverHost, e);
        } finally {
            if (response != null) {
                response.releaseConnection();
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T queryByMapReduce(MapReduceOperation.Context context, Class<T> returnType) {
        String serverHost = hostManager.getHost();
        ClientRequest request = null;
        ClientResponse<T> response = null;
        try {
            String requestUri = UriBuilder.fromUri(serverHost).path(context.getBucket()).path("mapReduce").build().toString();
            request = requestFactory.createRequest(requestUri);
            response = request.body(JSON_CONTENT_TYPE, context.getQuery()).post();
            if (!response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                throw exceptionTranslator.translate(Operation.MAP_REDUCE, response);
            }
            return response.getEntity(returnType);
        } catch (TerrastoreClientException e) {
            throw e;
        } catch (Exception e) {
            throw getClientSideException(serverHost, e);
        } finally {
            if (response != null) {
                response.releaseConnection();
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void exportBackup(BackupOperation.Context context) throws TerrastoreClientException {
        String serverHost = hostManager.getHost();
        ClientRequest request = null;
        ClientResponse response = null;
        try {
            String requestUri = UriBuilder.fromUri(serverHost).path(context.getBucket()).path("export").queryParam("destination", context.getFile()).
                    queryParam("secret", context.getSecretKey()).build().toString();
            request = requestFactory.createRequest(requestUri);
            response = request.body(JSON_CONTENT_TYPE, "").post();
            if (!response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                throw exceptionTranslator.generalException(response);
            }
        } catch (TerrastoreClientException e) {
            throw e;
        } catch (Exception e) {
            throw getClientSideException(serverHost, e);
        } finally {
            if (response != null) {
                response.releaseConnection();
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void importBackup(BackupOperation.Context context) throws TerrastoreClientException {
        String serverHost = hostManager.getHost();
        ClientRequest request = null;
        ClientResponse response = null;
        try {
            String requestUri = UriBuilder.fromUri(serverHost).path(context.getBucket()).path("import").queryParam("source", context.getFile()).queryParam("secret", context.
                    getSecretKey()).build().toString();
            request = requestFactory.createRequest(requestUri);
            response = request.body(JSON_CONTENT_TYPE, "").post();
            if (!response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                throw exceptionTranslator.generalException(response);
            }
        } catch (TerrastoreClientException e) {
            throw e;
        } catch (Exception e) {
            throw getClientSideException(serverHost, e);
        } finally {
            if (response != null) {
                response.releaseConnection();
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T executeUpdate(UpdateOperation.Context context, Class<T> type) throws TerrastoreClientException {
        String serverHost = hostManager.getHost();
        ClientRequest request = null;
        ClientResponse<T> response = null;
        try {
            String requestUri = UriBuilder.fromUri(serverHost).path(context.getBucket()).path(context.getKey()).path("update").queryParam("function", context.
                    getFunction()).queryParam("timeout", context.getTimeOut()).build().toString();

            request = requestFactory.createRequest(requestUri);
            response = request.body(JSON_CONTENT_TYPE, context.getParameters()).post();
            if (response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                return response.getEntity(type);
            } else {
                throw exceptionTranslator.translate(Operation.UPDATE, response);
            }
        } catch (TerrastoreClientException e) {
            throw e;
        } catch (Exception e) {
            throw getClientSideException(serverHost, e);
        } finally {
            if (response != null) {
                response.releaseConnection();
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T executeMerge(MergeOperation.Context context, Class<T> type) throws TerrastoreClientException {
        String serverHost = hostManager.getHost();
        ClientRequest request = null;
        ClientResponse<T> response = null;
        try {
            String requestUri = UriBuilder.fromUri(serverHost).path(context.getBucket()).path(context.getKey()).path("merge").build().toString();
            request = requestFactory.createRequest(requestUri);
            response = request.body(JSON_CONTENT_TYPE, context.getDescriptor()).post();
            if (response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                return response.getEntity(type);
            } else {
                throw exceptionTranslator.translate(Operation.MERGE, response);
            }
        } catch (TerrastoreClientException e) {
            throw e;
        } catch (Exception e) {
            throw getClientSideException(serverHost, e);
        } finally {
            if (response != null) {
                response.releaseConnection();
            }
        }
    }

    @Override
    public <T> Values<T> bulkGet(Context context, Class<T> type) throws TerrastoreClientException {
        String serverHost = hostManager.getHost();
        ClientRequest request = null;
        ClientResponse<Values<T>> response = null;
        try {
            String requestUri = UriBuilder.fromUri(serverHost).path(context.getBucket()).path("bulk").path("get").build().toString();
            request = requestFactory.createRequest(requestUri);
            response = request.body(JSON_CONTENT_TYPE, context.getKeys()).post();
            if (response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                return response.getEntity(Values.class, type);
            } else {
                throw exceptionTranslator.generalException(response);
            }
        } catch (TerrastoreClientException e) {
            throw e;
        } catch (Exception e) {
            throw getClientSideException(serverHost, e);
        } finally {
            if (response != null) {
                response.releaseConnection();
            }
        }
    }

    @Override
    public Set<String> bulkPut(Context context) throws TerrastoreClientException {
        String serverHost = hostManager.getHost();
        ClientRequest request = null;
        ClientResponse<Set<String>> response = null;
        try {
            String requestUri = UriBuilder.fromUri(serverHost).path(context.getBucket()).path("bulk").path("put").build().toString();
            request = requestFactory.createRequest(requestUri);
            response = request.body(JSON_CONTENT_TYPE, context.getValues()).post();
            if (response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                return response.getEntity(Set.class);
            } else {
                throw exceptionTranslator.generalException(response);
            }
        } catch (TerrastoreClientException e) {
            throw e;
        } catch (Exception e) {
            throw getClientSideException(serverHost, e);
        } finally {
            if (response != null) {
                response.releaseConnection();
            }
        }
    }

    private String buildRangeURI(RangeOperation.Context context,
            String serverHost) {
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
        return requestUri;
    }

    private ClientRequest getStatsRequest(String serverHost, String stats) {
        String requestUri = UriBuilder.fromUri(serverHost).path("_stats").path(stats).build().toString();
        ClientRequest request = requestFactory.createRequest(requestUri);
        return request.accept(JSON_CONTENT_TYPE);
    }

    private ClientRequest getKeyRequest(String serverHost, String bucket, String key) {
        String requestUri = UriBuilder.fromUri(serverHost).path(bucket).path(key).build().toString();
        ClientRequest request = requestFactory.createRequest(requestUri);
        return request.accept(JSON_CONTENT_TYPE);
    }

    private ClientRequest getBucketRequest(String serverHost, String bucket) {
        String requestUri = UriBuilder.fromUri(serverHost).path(bucket).build().toString();
        ClientRequest request = requestFactory.createRequest(requestUri);
        return request.accept(JSON_CONTENT_TYPE);
    }

    private TerrastoreClientException getClientSideException(String serverHost, Exception e) {
        if (e instanceof ConnectException) {
            LOG.error(e.getMessage(), e);
            hostManager.suspect(serverHost);
            return new TerrastoreConnectionException("Unable to connect to: " + serverHost, serverHost, e);
        }

        return new TerrastoreClientException("Could not service your request: " + e, e);
    }

}
