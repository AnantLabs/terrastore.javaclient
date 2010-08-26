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
package terrastore.client.test.integration;

import java.util.Arrays;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import terrastore.client.TerrastoreClient;
import terrastore.client.connection.OrderedHostManager;
import terrastore.client.connection.SingleHostManager;
import terrastore.client.connection.TerrastoreConnectionException;
import terrastore.client.connection.resteasy.HTTPConnectionFactory;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class HostManagerIntegrationTest {

    @Test
    public void testSingleHostManagerAlwaysConnectsToTheSameHost() {
        TerrastoreClient client = new TerrastoreClient(new SingleHostManager("http://localhost:9000"), new HTTPConnectionFactory());
        try {
            client.buckets().list();
            fail("Expected to fail because trying to connect a non existent server.");
        } catch (TerrastoreConnectionException ex) {
            try {
                client.buckets().list();
                fail("Expected to fail because trying to connect a non existent server.");
            } catch (TerrastoreConnectionException ex2) {
            }
        }
    }

    @Test
    public void testOrderedHostManagerAlwaysConnectsToTheSameHostIfSuccessful() {
        TerrastoreClient client = new TerrastoreClient(new OrderedHostManager(Arrays.asList(new String[]{"http://localhost:8080", "http://localhost:9000"})), new HTTPConnectionFactory());
        client.buckets().list();
        client.buckets().list();
    }

    @Test
    public void testOrderedHostManagerChangesHostIfUnsuccessful() {
        TerrastoreClient client = new TerrastoreClient(new OrderedHostManager(Arrays.asList(new String[]{"http://localhost:9000", "http://localhost:8080"})), new HTTPConnectionFactory());
        try {
            client.buckets().list();
            fail("Expected to fail because trying to connect a non existent server.");
        } catch (TerrastoreConnectionException ex) {
            client.buckets().list();
            // Now always uses the successful host:
            client.buckets().list();
        }
    }
}
