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

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Sergio Bossa
 */
public class Values<T> extends AbstractMap<String, T> {

    private final Map<String, T> values;

    public Values(Map<String, T> values) {
        this.values = values;
    }
    
    /**
     * Returns the value for the specified key, or null if no value exists
     * for that key.
     */
    @Override
    public T get(Object key) {
        return values.get(key);
    }

    /**
     * Returns a set of entries contained in this instance.
     */
    @Override
    public Set<Entry<String, T>> entrySet() {
        return values.entrySet();
    }
}
