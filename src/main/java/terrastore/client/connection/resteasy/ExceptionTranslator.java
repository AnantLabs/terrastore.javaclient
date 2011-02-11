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

import org.jboss.resteasy.client.ClientResponse;

import terrastore.client.ConditionalOperation;
import terrastore.client.TerrastoreClientException;
import terrastore.client.TerrastoreRequestException;
import terrastore.client.connection.ClusterUnavailableException;
import terrastore.client.connection.ErrorMessage;
import terrastore.client.connection.MapReduceQueryException;
import terrastore.client.connection.NoSuchKeyException;
import terrastore.client.connection.TerrastoreServerException;
import terrastore.client.connection.UnsatisfiedConditionException;

/**
 * 
 * @author Sven Johansson
 *
 */
public class ExceptionTranslator {

    public enum Operation {
        GET,
        CONDITIONAL,
        MAP_REDUCE,
        MERGE,
        UPDATE
    }
    
    @SuppressWarnings("unchecked")
    public TerrastoreClientException translate(Operation operation, ClientResponse response) {
        switch (operation) {
        case CONDITIONAL:
            return conditionalException(response);
        case GET:
            return getException(response);
        case MAP_REDUCE:
            return mapReduceException(response);
        case UPDATE:
            return updateException(response);
        default:
            return generalException(response);
        }
    }
    
    private TerrastoreClientException conditionalException(ClientResponse response) {
        switch (response.getStatus()) {
            case 400:
                return new TerrastoreRequestException((ErrorMessage) response.getEntity(ErrorMessage.class));
            case 404:
            case 409:
                return new UnsatisfiedConditionException((ErrorMessage) response.getEntity(ErrorMessage.class));
            default:
                return generalException(response);
        }
    }

    @SuppressWarnings("unchecked")
    private TerrastoreClientException getException(ClientResponse response) {
        switch (response.getStatus()) {
            case 404:
                return new NoSuchKeyException((ErrorMessage) response.getEntity(ErrorMessage.class));
            default:
                return generalException(response);
        }
    }
    
    @SuppressWarnings("unchecked")
    private TerrastoreClientException updateException(ClientResponse response) {
        switch (response.getStatus()) {
            case 404:
                return new NoSuchKeyException((ErrorMessage) response.getEntity(ErrorMessage.class));
            default:
                return generalException(response);
        }
    }

    @SuppressWarnings("unchecked")
    private TerrastoreClientException mergeException(ClientResponse response) {
        switch (response.getStatus()) {
            case 404:
                return new NoSuchKeyException((ErrorMessage) response.getEntity(ErrorMessage.class));
            default:
                return generalException(response);
        }
    }

    @SuppressWarnings("unchecked")
    private TerrastoreClientException mapReduceException(ClientResponse response) {
        switch (response.getStatus()) {
            case 400:
                return new MapReduceQueryException(((ErrorMessage) response.getEntity(ErrorMessage.class)));
            default:
                return generalException(response);
        }
    }

    @SuppressWarnings("unchecked")
    TerrastoreClientException generalException(ClientResponse response) {
        switch (response.getStatus()) {
            case 500:
                try {
                    return new TerrastoreRequestException((ErrorMessage) response.getEntity(ErrorMessage.class));
                } catch (Exception e) {
                    return new TerrastoreServerException("Unexpected server error.");
                }
            case 503:
                return new ClusterUnavailableException("The server cluster, or parts of the cluster, is not not available.");
            default:
                return new TerrastoreRequestException((ErrorMessage) response.getEntity(ErrorMessage.class));
        }
    }
    
}
