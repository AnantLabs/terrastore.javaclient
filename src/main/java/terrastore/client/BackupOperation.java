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

import terrastore.client.connection.Connection;

/**
 * @author Sven Johansson
 * @author Sergio Bossa
 *  
 */
public class BackupOperation extends AbstractOperation {

    private final String bucket;
    //
    private volatile String file;
    private volatile String secretKey;

    /**
     * Sets up a {@link BackupOperation} for a specific bucket and
     * {@link Connection}.
     * 
     * @param bucket The parent {@link BucketOperation}
     * @param connection The Terrastore server {@link Connection}
     */
    BackupOperation(Connection connection, String bucket) {
        super(connection);
        this.bucket = bucket;
    }

    BackupOperation(BackupOperation other) {
        super(other.connection);
        this.bucket = other.bucket;
        this.file = other.file;
        this.secretKey = other.secretKey;
    }

    /**
     * Specifies the source or destination file name for the backup operation.
     * 
     * @param file The name of the server side file to read/write from.
     */
    public BackupOperation file(String file) {
        BackupOperation newInstance = new BackupOperation(this);
        newInstance.file = file;
        return newInstance;
    }

    /**
     * Specifies the "secret key" used to validate/authenticate the backup
     * operation.
     * The secret key serves as a safety-mechanism to guard against accidental
     * execution of backup operations.
     * 
     * @param secretKey The "secret key" to be used in server communication.
     */
    public BackupOperation secretKey(String secretKey) {
        BackupOperation newInstance = new BackupOperation(this);
        newInstance.secretKey = secretKey;
        return newInstance;
    }

    /**
     * Executes an export of the buckets contents to the specified file.
     * 
     * @throws TerrastoreClientException If server communication fails, or the
     *             request is not valid - i.e, the secret key is rejected.
     * 
     * @see #file(String)
     * @see #secretKey()
     */
    public void executeExport() throws TerrastoreClientException {
        connection.exportBackup(new Context());
    }

    /**
     * Executes an import of bucket contents from the specified file on the
     * Terrastore server.
     * 
     * @throws TerrastoreClientException If server communication fails, or the
     *             request is not valid - i.e, the secret key is rejected.
     */
    public void executeImport() throws TerrastoreClientException {
        connection.importBackup(new Context());
    }

    public class Context {

        public String getBucket() {
            return bucket;
        }

        public String getFile() {
            return file;
        }

        public String getSecretKey() {
            return secretKey;
        }
    }
}
