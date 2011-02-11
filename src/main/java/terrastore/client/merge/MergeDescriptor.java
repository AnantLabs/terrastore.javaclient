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
package terrastore.client.merge;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.codehaus.jackson.annotate.JsonValue;

/**
 * @author Sergio Bossa
 */
public class MergeDescriptor {

    private final Map<String, Object> descriptor;

    public MergeDescriptor() {
        descriptor = new HashMap<String, Object>();
    }

    private MergeDescriptor(MergeDescriptor other) {
        this.descriptor = new HashMap<String, Object>(other.descriptor);
    }

    public MergeDescriptor add(Map<String, Object> entries) {
        descriptor.put("+", new HashMap<String, Object>(entries));
        return new MergeDescriptor(this);
    }

    public MergeDescriptor replace(Map<String, Object> entries) {
        descriptor.put("*", new HashMap<String, Object>(entries));
        return new MergeDescriptor(this);
    }

    public MergeDescriptor remove(Set<String> keys) {
        descriptor.put("-", new HashSet<String>(keys));
        return new MergeDescriptor(this);

    }

    public MergeDescriptor addToArray(String arrayKey, List<Object> values) {
        List<Object> adds = new LinkedList<Object>();
        adds.add("+");
        adds.addAll(values);
        descriptor.put(arrayKey, adds);
        return new MergeDescriptor(this);
    }

    public MergeDescriptor removeFromArray(String arrayKey, List<String> values) {
        List<String> removes = new LinkedList<String>();
        removes.add("-");
        removes.addAll(values);
        descriptor.put(arrayKey, removes);
        return new MergeDescriptor(this);
    }

    public MergeDescriptor merge(String key, MergeDescriptor mergeDescriptor) {
        descriptor.put(key, mergeDescriptor);
        return new MergeDescriptor(this);
    }

    @JsonValue
    public Map<String, Object> exportAsMap() {
        return descriptor;
    }

}
