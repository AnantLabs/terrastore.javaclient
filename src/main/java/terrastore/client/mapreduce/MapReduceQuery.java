package terrastore.client.mapreduce;


public class MapReduceQuery {
    
    private Range range;
    private Task task;

    public MapReduceQuery range(Range range) {
        setRange(range);
        return this;
    }
    
    public MapReduceQuery task(Task task) {
        setTask(task);
        return this;
    }
    
    public Range getRange() {
        return range;
    }

    public void setRange(Range range) {
        this.range = range;
    }

    public Task getTask() {
        return task;
    }

    public void setTask(Task task) {
        this.task = task;
    }
    

}
