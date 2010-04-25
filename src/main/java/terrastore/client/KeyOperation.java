/**
 * Copyright 2009 Sergio Bossa (sergio.bossa@gmail.com)
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

import terrastore.client.connection.Connection;

/**
 * KeyOperations serves as the root command for all operations to be directly
 * performed on keys, such as reading/writing values.
 * 
 * @author Sven Johansson
 * @date 24 apr 2010
 * @since 2.0
 */
public class KeyOperation extends AbstractBucketOperation {

	private String key;
	
	/**
	 * Sets up a KeyOperation for the specified bucket, connection and key.
	 * 
	 * @param bucket The parent {@link BucketOperation}
	 * @param connection The Connection to be used for server communication.
	 * @param key The key to perform operations on.
	 */
	KeyOperation(BucketOperation bucket, Connection connection, String key) {
		super(bucket, connection);
		this.key = key;
	}

	/**
	 * Writes a value/document for this key.
	 * 
	 * If a value is already present for this key, it will be
	 * overwritten/replaced by the specified value.
	 * 
	 * @param <T> The Java type for the value
	 * @param value The value to be written.
	 * @throws TerrastoreClientException
	 *             If server communication fails, or the value is rejected, i.e.
	 *             because it cannot be serialized.
	 */
	public <T> void put(T value) throws TerrastoreClientException {
		connection.putValue(this, value);
	}

	/**
	 * Removes/deletes this key and its value from the current bucket.
	 * 
	 * @throws TerrastoreClientException if server communication fails.
	 */
	public void remove() throws TerrastoreClientException {
		connection.removeValue(this);
	}

	/**
	 * Retrieves the stored value for this key, as an instance of the specified
	 * Java type.
	 * 
	 * @param <T> The Java type for this value.
	 * @param type The Java type for this value.
	 * @return The value for the current key, as an instance of <T>/type
	 * @throws TerrastoreClientException
	 *             if server communication fails, or the key does not exist
	 *             within the current bucket.
	 */
	public <T> T get(Class<T> type) throws TerrastoreClientException {
		return connection.getValue(this, type);
	}

	/**
	 * Sets up an {@link UpdateOperation} for the value of the current key.
	 * 
	 * @return an UpdateOperation for the current key.
	 */
	public UpdateOperation update() {
		return new UpdateOperation(this, connection);
	}

	/**
	 * @return The name of the key that this KeyOperation pertains to.
	 */
	public String key() {
		return key;
	}

	/**
	 * @return The name of the bucket that this KeyOperation operates within.
	 */
	public String bucketName() {
		return bucket.bucketName();
	}
	
}
