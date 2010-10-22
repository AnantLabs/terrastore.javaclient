package terrastore.client.mapreduce;

public class Range {
    
    private String startKey;
    private String endKey;
    private String comparator;
    private Long timeToLive;
    
    public Range from(String key) {
        setStartKey(key);
        return this;
    }
    
    public Range to(String key) {
        setEndKey(key);
        return this;
    }
    
    public Range comparator(String comparatorName) {
        setComparator(comparatorName);
        return this;
    }
    
    public Range timeToLive(long timeToLive) {
        setTimeToLive(timeToLive);
        return this;
    }

    public String getStartKey() {
        return startKey;
    }

    public void setStartKey(String startKey) {
        this.startKey = startKey;
    }

    public String getEndKey() {
        return endKey;
    }

    public void setEndKey(String endKey) {
        this.endKey = endKey;
    }

    public String getComparator() {
        return comparator;
    }

    public void setComparator(String comparator) {
        this.comparator = comparator;
    }

    public Long getTimeToLive() {
        return timeToLive;
    }

    public void setTimeToLive(Long timeToLive) {
        this.timeToLive = timeToLive;
    }
    
    
    
}
