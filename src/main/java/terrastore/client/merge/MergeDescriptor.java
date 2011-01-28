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
