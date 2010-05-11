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
 * {@link BackupOperation}s are used for reading/writing a complete backup of a
 * specific buckets contents to/from disk.
 * 
 * @author Sven Johansson
 * @author Sergio Bossa
 * @since 2.0
 */
public class BackupOperation extends AbstractOperation {

    private final String bucket;
    //
    private String fileName;
    private String secretKey;

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

    /**
     * Specifies the source or destination file name for the backup operation.
     * 
     * @param fileName The name of the server side file to read/write from.
     */
    public BackupOperation file(String fileName) {
        this.fileName = fileName;
        return this;
    }

    /**
     * Specifies the "secret key" used to validate/authenticate the backup
     * operation.
     * 
     * @param secretKey The "secret key" to be used in server communication.
     */
    public BackupOperation secretKey(String secretKey) {
        this.secretKey = secretKey;
        return this;
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
     * TODO: Does this operation restore state completely from the dumped file?
     * Merge/replace?
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

        public String getFileName() {
            return fileName;
        }

        public String getSecretKey() {
            return secretKey;
        }
    }
}
