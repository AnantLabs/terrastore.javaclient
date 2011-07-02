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

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;

import terrastore.client.connection.Connection;
import terrastore.client.connection.ConnectionFactory;
import terrastore.client.connection.HostManager;
import terrastore.client.connection.TerrastoreConnectionException;
import terrastore.client.mapping.JsonObjectDescriptor;

/**
 * HTTP connection factory based on org.apache.commons.httpclient.HttpClient.
 *
 * @author Sven Johansson
 * @author Sergio Bossa
 */
public class HTTPConnectionFactory implements ConnectionFactory {

    private final HttpClient client;

    public HTTPConnectionFactory(HttpClient client) {
        this.client = client;
    }

    public HTTPConnectionFactory() {
        HttpConnectionManagerParams httpParams = new HttpConnectionManagerParams();
        httpParams.setDefaultMaxConnectionsPerHost(Runtime.getRuntime().availableProcessors() * 10);
        httpParams.setMaxTotalConnections(Runtime.getRuntime().availableProcessors() * 10);
        HttpConnectionManager httpManager = new MultiThreadedHttpConnectionManager();
        httpManager.setParams(httpParams);
        this.client = new HttpClient(httpManager);
    }

    @Override
    public Connection makeConnection(HostManager hostManager, List<JsonObjectDescriptor<?>> descriptors) throws TerrastoreConnectionException {
        List<JsonObjectDescriptor<?>> jsonDescriptors = new ArrayList<JsonObjectDescriptor<?>>();
        jsonDescriptors.addAll(descriptors);
        jsonDescriptors.add(new ErrorMessageDescriptor());
        return new HTTPConnection(hostManager, jsonDescriptors);
    }
}
