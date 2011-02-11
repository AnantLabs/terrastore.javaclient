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
package terrastore.server;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.io.File;

import org.apache.commons.io.FileUtils;

import terrastore.startup.Constants;
import terrastore.test.embedded.TerrastoreEmbeddedServer;

/**
 * Wrapper/Facade class that simplifies usage of the {@link TerrastoreEmbeddedServer}.
 * 
 * @author Sven Johansson
 */
public class EmbeddedServerWrapper {
    
    private TerrastoreEmbeddedServer server;
    private File backupDirectory;
    
    public EmbeddedServerWrapper() throws Exception {
        this("127.0.0.1", 8080);
    }
    
    public EmbeddedServerWrapper(String host, int port) throws Exception {
        resolveBackupDirectory();
        createBackupDir();
        server = new TerrastoreEmbeddedServer();
        server.start(host, port);
        createBackupDir();
    }
    
    public void stop() throws Exception {
        server.stop();
        removeBackupDir();
    }
    
    private void resolveBackupDirectory() {
        String tmpDirName = System.getProperty("java.io.tmpdir");
        System.setProperty("TERRASTORE_HOME", tmpDirName);

        String backupPath = tmpDirName + File.separator + Constants.BACKUPS_DIR;
        backupDirectory = new File(backupPath);
    }
    
    private void createBackupDir() {
        if (!backupDirectory.exists()) {
            boolean backupDirCreated = backupDirectory.mkdirs();
            assertTrue("Unable to create backup directory: " + backupDirectory.getAbsolutePath(), backupDirCreated);
        }
    }
    
    private void removeBackupDir() throws Exception {
        FileUtils.deleteDirectory(backupDirectory);
        assertFalse("Unable to remove backup directory: " + backupDirectory.getAbsolutePath(), backupDirectory.exists());
    }

    public File getBackupFile(String fileName) {
        return new File(backupDirectory, fileName); 
    }
    
}
