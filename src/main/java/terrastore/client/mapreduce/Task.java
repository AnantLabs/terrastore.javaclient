package terrastore.client.mapreduce;

import java.util.Map;

public class Task {

    private String mapper;
    private String combiner;
    private String reducer;
    
    private long timeout;
    private Map<String, Object> parameters;
 
    public Task mapper(String mapperFunction) {
        setMapper(mapperFunction);
        return this;
    }
    
    public Task combiner(String combiner) {
        setCombiner(combiner);
        return this;
    }
    
    public Task reducer(String reducer) {
        setReducer(reducer);
        return this;
    }
    
    public Task timeout(long timeOut) {
        setTimeout(timeOut);
        return this;
    }
    
    public Task parameters(Map<String, Object> parameters) {
        setParameters(parameters);
        return this;
    }

    public String getMapper() {
        return mapper;
    }

    public void setMapper(String mapper) {
        this.mapper = mapper;
    }

    public String getCombiner() {
        return combiner;
    }

    public void setCombiner(String combiner) {
        this.combiner = combiner;
    }

    public String getReducer() {
        return reducer;
    }

    public void setReducer(String reducer) {
        this.reducer = reducer;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, Object> parameters) {
        this.parameters = parameters;
    }
    
}
