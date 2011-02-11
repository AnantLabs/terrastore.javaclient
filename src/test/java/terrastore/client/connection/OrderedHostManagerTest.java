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

import java.util.Arrays;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class OrderedHostManagerTest {

    @Test
    public void testAlwaysReturnsSameHostIfNotSuspected() {
        String host1 = "http://localhost:8080";
        String host2 = "http://localhost:8081";
        OrderedHostManager hostManager = new OrderedHostManager(Arrays.asList(new String[]{host1, host2}));
        
        String unsuspected = hostManager.getHost();
        assertEquals(host1, unsuspected);
        unsuspected = hostManager.getHost();
        assertEquals(host1, unsuspected);
    }

    @Test
    public void testChangesHostWhenSuspected() {
        String host1 = "http://localhost:8080";
        String host2 = "http://localhost:8081";
        OrderedHostManager hostManager = new OrderedHostManager(Arrays.asList(new String[]{host1, host2}));

        String unsuspected = hostManager.getHost();
        assertEquals(host1, unsuspected);
        hostManager.suspect(host1);
        unsuspected = hostManager.getHost();
        assertEquals(host2, unsuspected);
        unsuspected = hostManager.getHost();
        assertEquals(host2, unsuspected);
    }
}