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
import terrastore.client.BucketOperation;
import terrastore.client.KeyOperation;
import terrastore.client.PredicateQuery;
import terrastore.client.RangeQuery;
import terrastore.client.TerrastoreClientException;
import terrastore.client.TerrastoreRequestException;
import terrastore.client.UpdateOperation;
import terrastore.client.Values;
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
 * @date 24 apr 2010
 * @since 2.0
 */
public class RESTEasyConnection implements Connection {

    private static final String JSON_CONTENT_TYPE = "application/json";
	
    private String serverHost;
    private ClientRequestFactory requestFactory = new ClientRequestFactory();

    public RESTEasyConnection(String serverHost, List<JsonObjectDescriptor<?>> descriptors) {
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
    public void removeBucket(BucketOperation bucket) throws TerrastoreClientException {
        try {
            ClientRequest request = getBucketRequest(bucket.bucketName());
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
    public <T> void putValue(KeyOperation key, T value) throws TerrastoreClientException {
        try {
            ClientRequest request = getKeyRequest(key);
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
    public <T> T getValue(KeyOperation key, Class<T> type) throws TerrastoreClientException {
        try {
            ClientRequest request = getKeyRequest(key);
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
    public void removeValue(KeyOperation key) throws TerrastoreClientException {
        try {
            ClientRequest request = getKeyRequest(key);
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
    public <T> Map<String, T> getAllValues(BucketOperation bucket, Class<T> type) throws TerrastoreClientException {
        try {
            ClientRequest request = getBucketRequest(bucket.bucketName()).queryParameter("limit", bucket.limit());
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
    public <T> Values<T> doRangeQuery(RangeQuery query, Class<T> type) throws TerrastoreClientException {
        try {
            UriBuilder uriBuilder = UriBuilder.fromUri(serverHost).path(query.bucketName()).path("range")
                .queryParam("startKey", query.from())
                .queryParam("limit", query.limit())
                .queryParam("timeToLive", query.timeToLive());
            if (null != query.comparator()) {
                uriBuilder.queryParam("comparator", query.comparator());
            }
            if (null != query.to()) {
                uriBuilder.queryParam("endKey", query.to());
            }
            if (null != query.predicate()) {
                uriBuilder.queryParam("predicate", query.predicate());
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
    public <T> Values<T> doPredicateQuery(PredicateQuery query, Class<T> type) throws TerrastoreClientException {
        try {
            String requestUri = UriBuilder.fromUri(serverHost)
                .path(query.bucketName())
                .path("predicate")
                .queryParam("predicate", query.predicate())
                .build().toString();
    
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
    public void exportBackup(BackupOperation backup) throws TerrastoreClientException {
        try {
            String requestUri = UriBuilder.fromUri(serverHost).path(backup.bucketName()).path("export").queryParam("destination", backup.fileName()).queryParam("secret", backup.secretKey()).build().toString();
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
    public void importBackup(BackupOperation backup) throws TerrastoreClientException{
        try {
            String requestUri = UriBuilder.fromUri(serverHost).path(backup.bucketName()).path("import").queryParam("source", backup.fileName()).queryParam("secret", backup.secretKey()).build().toString();
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
    public void executeUpdate(UpdateOperation update) throws TerrastoreClientException {
        try {
            String requestUri = UriBuilder.fromUri(serverHost).path(update.bucketName()).path(update.key()).path("update")
                .queryParam("function", update.functionId())
                .queryParam("timeout", update.timeOut())
                .build().toString();
            
            ClientRequest request = requestFactory.createRequest(requestUri);
            ClientResponse response = request.body(JSON_CONTENT_TYPE, update.parameters()).post();
            if (!response.getResponseStatus().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
                throw new TerrastoreRequestException(response.getResponseStatus().getStatusCode(), response.getEntity(String.class).toString());
            }
        } catch (TerrastoreClientException e) {
            throw e;
        } catch (Exception e) {
            throw connectionException("Unable to perform update", e);
        }
    }

    private ClientRequest getKeyRequest(KeyOperation key) {
        String requestUri = UriBuilder.fromUri(serverHost).path(key.bucketName()).path(key.key()).build().toString();
        ClientRequest request = requestFactory.createRequest(requestUri);
        return request.accept(JSON_CONTENT_TYPE);
    }

    private ClientRequest getBucketRequest(String bucketName) {
        String requestUri = UriBuilder.fromUri(serverHost).path(bucketName).build().toString();
        ClientRequest request = requestFactory.createRequest(requestUri);
        return request.accept(JSON_CONTENT_TYPE);
    }
    
    private TerrastoreClientException connectionException(String message, Exception e) {
        return new TerrastoreConnectionException(message, serverHost, e);
    }

    @Override
    public String getServerHost() {
        return serverHost;
    }
    
}
