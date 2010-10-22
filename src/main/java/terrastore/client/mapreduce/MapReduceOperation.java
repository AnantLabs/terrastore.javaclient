package terrastore.client.mapreduce;

import terrastore.client.connection.Connection;

public class MapReduceOperation {

    private String bucket;
    private MapReduceQuery query;
    private Connection connection;

    public MapReduceOperation(Connection connection, String bucket, MapReduceQuery query) {
        this.bucket = bucket;
        this.query = query;
        this.connection = connection;
    }
    
    public <T> T execute(Class<T> returnType) {
        return connection.mapReduce(this, returnType);
    }
    
    public MapReduceQuery query() {
        return query;
    }
    
    public String bucket() {
        return bucket;
    }
    
}
