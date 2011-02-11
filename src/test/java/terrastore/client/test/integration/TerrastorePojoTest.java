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
package terrastore.client.test.integration;

import org.junit.*;
import terrastore.client.BucketOperation;
import terrastore.client.TerrastoreClient;
import terrastore.client.TerrastoreClientException;
import terrastore.client.connection.NoSuchKeyException;
import terrastore.client.connection.resteasy.HTTPConnectionFactory;

import java.io.File;
import java.util.Map;
import terrastore.client.test.pojostest.Address;
import terrastore.client.test.pojostest.Address2;
import terrastore.client.test.pojostest.Customer;
import terrastore.client.test.pojostest.PhoneNumber;
import terrastore.server.EmbeddedServerWrapper;

import static org.junit.Assert.*;

/**
 * Tests CRUD on a small group of interconnected POJOs.
 *
 * @author mats@henricson.se
 */
public class TerrastorePojoTest {
    private static final String FILE_NAME = "realobjectstest.bak";

    private static final String CUSTOMERS_BUCKET = "customers";
    private static final String ADDRESSES_BUCKET = "addresses";

    private static final String LINDEX_KEY = "lindex";

    private static EmbeddedServerWrapper server;

    private TerrastoreClient client;

    private Customer lindex;

    @BeforeClass
    public static void startTerrastoreEmbeddedServer() throws Exception {
        server = new EmbeddedServerWrapper();
    }

    @AfterClass
    public static void stopTerrastoreEmbeddedServer() throws Exception {
        server.stop();
    }

    @Before
    public void setUp() throws Exception {
        client = new TerrastoreClient("http://127.0.0.1:8080", new HTTPConnectionFactory());
        
        tearDown();    // Clean everything first

        lindex = new Customer("Lindex");

        // Set of relationships
        lindex.setAddress(new Address("Drottninggatan 77"));
        lindex.addPhoneNumber(new PhoneNumber("1234567", "work"));
        lindex.addPhoneNumber(new PhoneNumber("2345678", "home"));
    }

    @After
    public void tearDown() {
        client.bucket(CUSTOMERS_BUCKET).clear();
        client.bucket(ADDRESSES_BUCKET).clear();
    }

    @Test
    public void testAddAndGetValue() throws TerrastoreClientException {
        BucketOperation customersBucket = client.bucket(CUSTOMERS_BUCKET);

        customersBucket.key(LINDEX_KEY).put(lindex);
        Customer lindexFromStore = customersBucket.key(LINDEX_KEY).get(Customer.class);
        assertNotNull(lindexFromStore);
        assertEquals(lindex, lindexFromStore);   // Equals
        assertFalse(lindex == lindexFromStore);  // Not the same
        assertEquals(lindex.getAddress(), lindexFromStore.getAddress());   // Equals
        assertFalse(lindex.getAddress() == lindexFromStore.getAddress());  // Not the same
        assertEquals(lindex.getPhoneNumbers().get(0), lindexFromStore.getPhoneNumbers().get(0));   // Equals
        assertFalse(lindex.getPhoneNumbers().get(0) == lindexFromStore.getPhoneNumbers().get(0));  // Not the same
    }

    @Test(expected = NoSuchKeyException.class)
    public void testAddThenDelete() throws Exception {
        BucketOperation customersBucket = client.bucket(CUSTOMERS_BUCKET);

        customersBucket.key(LINDEX_KEY).put(lindex);
        customersBucket.key(LINDEX_KEY).remove();

        customersBucket.key(LINDEX_KEY).get(Customer.class);
    }

    @Test
    public void testConditionallyGetValue() throws Exception {
        BucketOperation customersBucket = client.bucket(CUSTOMERS_BUCKET);

        customersBucket.key(LINDEX_KEY).put(lindex);

        assertEquals(lindex, customersBucket.key(LINDEX_KEY).conditional("jxpath:/name").get(Customer.class));
    }

    @Test
    public void testGetAll() throws Exception {
        BucketOperation customersBucket = client.bucket(CUSTOMERS_BUCKET);

        customersBucket.key(LINDEX_KEY).put(lindex);

        Map<String, Customer> customers = customersBucket.values().get(Customer.class);

        assertEquals(1, customers.size());
        assertTrue(customers.containsValue(lindex));
    }

    @Test
    public void testGetAllWithLimit() throws Exception {
        BucketOperation customersBucket = client.bucket(CUSTOMERS_BUCKET);

        Customer hm = new Customer("Hennes & Mauritz");

        customersBucket.key(LINDEX_KEY).put(lindex);
        customersBucket.key("hm").put(hm);

        Map<String, Customer> customers = customersBucket.values().limit(1).get(Customer.class);

        assertEquals(1, customers.size());
        assertTrue(customers.containsValue(lindex) || customers.containsValue(hm) );
    }

    @Test
    public void testGetByName() throws Exception {
        BucketOperation customersBucket = client.bucket(CUSTOMERS_BUCKET);

        Customer zara = new Customer("Zara");

        customersBucket.key(LINDEX_KEY).put(lindex);
        customersBucket.key("zara").put(zara);

        String jxpath = "jxpath:/name[.='Lindex']";

        Map<String, Customer> lindexCustomers = customersBucket.predicate(jxpath).get(Customer.class);

        assertEquals(1, lindexCustomers.size());
        assertTrue(lindexCustomers.containsValue(lindex));
    }

    @Test
    public void testGetByKeyRange() throws Exception {
        BucketOperation customersBucket = client.bucket(CUSTOMERS_BUCKET);

        Customer brothers = new Customer("Brothers");
        Customer tiger = new Customer("Tiger");

        customersBucket.key("brothers").put(brothers);
        customersBucket.key(LINDEX_KEY).put(lindex);
        customersBucket.key("tiger").put(tiger);

        Map<String, Customer> customers = customersBucket.range("lexical-asc")
                                          .from("brothers").to(LINDEX_KEY).get(Customer.class);

        assertTrue(customers.containsValue(brothers) && customers.containsValue(lindex) );
    }

    @Test
    public void testGetByKeyRangeAndLimit() throws Exception {
        BucketOperation customersBucket = client.bucket(CUSTOMERS_BUCKET);

        Customer brothers = new Customer("Brothers");
        Customer tiger = new Customer("Tiger");

        customersBucket.key("brothers").put(brothers);
        customersBucket.key(LINDEX_KEY).put(lindex);
        customersBucket.key("tiger").put(tiger);

        Map<String, Customer> customers = customersBucket.range("lexical-asc")
                                          .from("brothers").to(LINDEX_KEY).limit(2).get(Customer.class);

        assertTrue(customers.containsValue(brothers) && customers.containsValue(lindex) );
    }

    @Test
    public void testConditionallyFollowLinkGetValue() throws Exception {
        BucketOperation customersBucket = client.bucket(CUSTOMERS_BUCKET);

        customersBucket.key(LINDEX_KEY).put(lindex);

        String jxpath = "jxpath:/address/street[.='Drottninggatan 77']";

        assertEquals(lindex, customersBucket.key(LINDEX_KEY).conditional(jxpath).get(Customer.class));
    }

    @Test
    public void testExportImportBackup() throws Exception {
        BucketOperation customersBucket = client.bucket(CUSTOMERS_BUCKET);

        customersBucket.key(LINDEX_KEY).put(lindex);

        assertEquals(1, customersBucket.values().get(Customer.class).size());

        customersBucket.backup().file(FILE_NAME).secretKey("SECRET-KEY").executeExport();

        customersBucket.clear();
        assertEquals(0, customersBucket.values().get(Customer.class).size());

        customersBucket.backup().file(FILE_NAME).secretKey("SECRET-KEY").executeImport();
        assertEquals(1, customersBucket.values().get(Customer.class).size());

        // Lets be a good citizen and remove the file after we're done
        File backupFile = server.getBackupFile(FILE_NAME);

        boolean wasDeleted = backupFile.delete();

        assertTrue(wasDeleted);
    }

    @Test
    public void testPutAddressGetAddress2() throws TerrastoreClientException {
        BucketOperation addressBucket = client.bucket(ADDRESSES_BUCKET);

        Address home = new Address("Apelvägen 6");

        addressBucket.key("home").put(home);
        Address2 newHome = addressBucket.key("home").get(Address2.class);
        assertNotNull(newHome);
        assertEquals(home.getStreet(), newHome.getStreet());
    }

    @Test(expected = TerrastoreClientException.class)
    public void testPutAddress2GetAddress() throws TerrastoreClientException {
        BucketOperation addressBucket = client.bucket(ADDRESSES_BUCKET);

        Address2 newHome = new Address2("Apelvägen 6", 13835);
        addressBucket.key("home").put(newHome);
        /* Address oldAddress = */ addressBucket.key("home").get(Address.class);
    }
}
