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
package terrastore.client.mapreduce;

import java.util.Map;
import org.codehaus.jackson.annotate.JsonProperty;

public class MapReduceQuery {

    @JsonProperty
    private volatile Range range;
    @JsonProperty
    private volatile Task task;

    public MapReduceQuery() {
    }

    protected MapReduceQuery(MapReduceQuery other) {
        this.range = other.range;
        this.task = other.task;
    }

    public MapReduceQuery range(Range range) {
        MapReduceQuery newInstance = new MapReduceQuery(this);
        newInstance.range = range;
        return newInstance;
    }

    public MapReduceQuery task(Task task) {
        MapReduceQuery newInstance = new MapReduceQuery(this);
        newInstance.task = task;
        return newInstance;
    }

    public static class Range {

        @JsonProperty
        private volatile String startKey;
        @JsonProperty
        private volatile String endKey;
        @JsonProperty
        private volatile String comparator;
        @JsonProperty
        private volatile Long timeToLive;

        public Range() {
        }

        protected Range(Range other) {
            this.startKey = other.startKey;
            this.endKey = other.endKey;
            this.comparator = other.comparator;
            this.timeToLive = other.timeToLive;
        }

        public Range from(String key) {
            Range newInstance = new Range(this);
            newInstance.startKey = key;
            return newInstance;
        }

        public Range to(String key) {
            Range newInstance = new Range(this);
            newInstance.endKey = key;
            return newInstance;
        }

        public Range comparator(String comparatorName) {
            Range newInstance = new Range(this);
            newInstance.comparator = comparatorName;
            return newInstance;
        }

        public Range timeToLive(long timeToLive) {
            Range newInstance = new Range(this);
            newInstance.timeToLive = timeToLive;
            return newInstance;
        }

    }

    public static class Task {

        @JsonProperty
        private volatile String mapper;
        @JsonProperty
        private volatile String combiner;
        @JsonProperty
        private volatile String reducer;
        @JsonProperty
        private volatile long timeout;
        @JsonProperty
        private volatile Map<String, Object> parameters;

        public Task() {
        }

        protected Task(Task other) {
            this.mapper = other.mapper;
            this.combiner = other.combiner;
            this.reducer = other.reducer;
            this.timeout = other.timeout;
            this.parameters = other.parameters;
        }

        public Task mapper(String mapper) {
            Task newInstance = new Task(this);
            newInstance.mapper = mapper;
            return newInstance;
        }

        public Task combiner(String combiner) {
            Task newInstance = new Task(this);
            newInstance.combiner = combiner;
            return newInstance;
        }

        public Task reducer(String reducer) {
            Task newInstance = new Task(this);
            newInstance.reducer = reducer;
            return newInstance;
        }

        public Task timeout(long timeout) {
            Task newInstance = new Task(this);
            newInstance.timeout = timeout;
            return newInstance;
        }

        public Task parameters(Map<String, Object> parameters) {
            Task newInstance = new Task(this);
            newInstance.parameters = parameters;
            return newInstance;
        }

    }
}
