/**
 * Copyright 2009 - 2010 Sergio Bossa (sergio.bossa@gmail.com)
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

import java.util.List;

import terrastore.client.connection.Connection;
import terrastore.client.connection.ConnectionFactory;
import terrastore.client.connection.HostManager;
import terrastore.client.connection.TerrastoreConnectionException;
import terrastore.client.mapping.JsonObjectDescriptor;

/**
 * @author Sven Johansson
 * @author Sergio Bossa
 *  
 */
public class HTTPConnectionFactory implements ConnectionFactory {

    @Override
    public Connection makeConnection(HostManager hostManager, List<? extends JsonObjectDescriptor<?>> descriptors) throws TerrastoreConnectionException {
        return new HTTPConnection(hostManager, descriptors);
    }
}
