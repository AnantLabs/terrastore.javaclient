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

package terrastore.client.connection;

import java.util.List;

import terrastore.client.mapping.JsonObjectDescriptor;

/**
 * 
 * @author Sven Johansson
 * @date 24 apr 2010
 * @since 2.0
 */
public interface ConnectionFactory {

    /**
     * Establishes a connection to a Terrastore server residing at the specified
     * host/url.
     * 
     * @param serverHost The host/url of the server to connect to.
     * @return A connection
     * @throws TerrastoreConnectionException
     */
    Connection makeConnection(String serverHost,
            List<JsonObjectDescriptor<?>> descriptors)
            throws TerrastoreConnectionException;

}
