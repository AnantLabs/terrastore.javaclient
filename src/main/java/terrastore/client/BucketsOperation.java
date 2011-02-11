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

import java.util.Set;

import terrastore.client.connection.Connection;

/**
 * BucketsOperation pertains to operations on all buckets on the
 * Terrastore server.
 * 
 * @author Sven Johansson
 *  
 */
public class BucketsOperation extends AbstractOperation {

    /**
     * Sets up a BucketsOperation for the provided {@link Connection}
     * 
     * @param connection The Connection to be used.
     */
    BucketsOperation(Connection connection) {
        super(connection);
    }

    /**
     * Retrieves the names of all currently existing buckets on the 
     * Terrastore server.
     * 
     * @return A {@link Set} containing all existing bucket names.
     * @throws TerrastoreClientException If server communication fails.
     */
    public Set<String> list() throws TerrastoreClientException {
        return connection.getBuckets();
    }

}
