package au.org.ala.kvs;

import au.org.ala.kvs.cache.ALANameMatchKVStoreFactory;
import au.org.ala.kvs.client.ALANameUsageMatch;
import au.org.ala.kvs.client.ALASpeciesMatchRequest;
import au.org.ala.util.TestUtils;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.cache.KeyValueCache;
import org.junit.Test;

public class NameMatchKVStoreTest {

    /**
     * Tests the Get operation on {@link KeyValueCache} that wraps a simple KV store backed by a HashMap.
     */
    @Test
    public void getCacheTest() throws Exception {

        KeyValueStore<ALASpeciesMatchRequest, ALANameUsageMatch> kvs = ALANameMatchKVStoreFactory.create(TestUtils.getConfig());
        ALASpeciesMatchRequest req = ALASpeciesMatchRequest.builder().scientificName("Macropus rufus").build();
        ALANameUsageMatch match = kvs.get(req);
        assert match.getTaxonConceptID() != null;

        ALASpeciesMatchRequest req2 = ALASpeciesMatchRequest.builder().scientificName("Osphranter rufus").build();
        ALANameUsageMatch match2 = kvs.get(req2);
        assert match2.getTaxonConceptID() != null;

        kvs.close();
    }

//    /**
//     * Tests the Get operation on {@link KeyValueCache} that wraps a simple KV store backed by a HashMap.
//     */
//    @Test
//    public void getCacheFailTest() throws Exception {
//
//        ClientConfiguration cc = ClientConfiguration.builder()
//                .withBaseApiUrl("http://localhostXXXXXX:9179") //GBIF base API url
//                .withTimeOut(10000l) //Geocode service connection time-out
//                .build();
//        KeyValueStore<ALASpeciesMatchRequest, ALANameUsageMatch> kvs = ALANameMatchKVStoreFactory.create(TestUtils.getConfig());
//
//        try {
//            ALASpeciesMatchRequest req = ALASpeciesMatchRequest.builder().scientificName("Macropus rufus").build();
//            ALANameUsageMatch match = kvs.get(req);
//            fail("Exception should be thrown");
//        } catch (RuntimeException e){
//            //expected
//        }
//    }
}
