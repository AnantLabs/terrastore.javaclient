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
package terrastore.client;

import terrastore.client.connection.Connection;

/**
 * StatsOperation is used to retrieve Terrastore statistics and other kinds of information.
 * 
 * @author Sergio Bossa
 */
public class StatsOperation extends AbstractOperation {

    /**
     * Sets up a StatsOperation for the specified Terrastore
     * {@link Connection}
     * 
     * @param connection The connection to be used for bucket operations
     */
    StatsOperation(Connection connection) {
        super(connection);
    }

    /**
     * Retrieves cluster statistics.
     * 
     * @throws TerrastoreClientException if the request fails
     */
    public ClusterStats cluster() throws TerrastoreClientException {
        return connection.getClusterStats();
    }
}
