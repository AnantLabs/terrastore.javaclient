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

import java.util.LinkedList;
import java.util.List;

/**
 * {@link HostManager} implementation managing an ordered list of Terrastore server hosts.<br/>
 * It always gets the first working connection in the list: in case of failure, the connection is moved at the end of the list and the
 * next one is used.
 *
 * @author Sergio Bossa
 */
public class OrderedHostManager implements HostManager {

    private final List<String> hosts;

    public OrderedHostManager(List<String> hosts) {
        this.hosts = new LinkedList<String>(hosts);
    }

    @Override
    public synchronized String getHost() {
        return hosts.get(0);
    }

    @Override
    public synchronized void suspect(String suspected) {
        if (hosts.contains(suspected)) {
            moveToEndOfList(suspected);
        }
    }

    private void moveToEndOfList(String suspected) {
        hosts.remove(suspected);
        hosts.add(suspected);
    }
}
