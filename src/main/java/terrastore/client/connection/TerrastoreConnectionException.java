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
package terrastore.client.connection;

import terrastore.client.TerrastoreClientException;

/**
 * Checked exception that is thrown to signify that a problem occurred while
 * establishing a {@link Connection} to a Terrastore server instance, or that a
 * problem has occured with an existing {@link Connection} instance.
 * 
 * @author Sven Johansson
 * @date 24 apr 2010
 *  
 */
public class TerrastoreConnectionException extends TerrastoreClientException {

    private static final long serialVersionUID = -671123602089781186L;

    /**
     * The server address to which a {@link Connection} could not be
     * established.
     */
    private String serverHost;

    /**
     * Constructs an exception that siginifies that a {@link Connection} could
     * not be established to a Terrastore server presumably residing at the
     * location specified by the <code>serverHost</code> address.
     * 
     * @param message The failure message.
     * @param serverHost The host to which a connection could not be
     *            established.
     */
    public TerrastoreConnectionException(String message, String serverHost) {
        super(message);
        this.serverHost = serverHost;
    }

    public TerrastoreConnectionException(String message, String serverHost,
            Throwable cause) {
        super(message, cause);
        this.serverHost = serverHost;
    }

    /**
     * @return The server host to which a connection could not be established.
     */
    public String getServerHost() {
        return serverHost;
    }

}
