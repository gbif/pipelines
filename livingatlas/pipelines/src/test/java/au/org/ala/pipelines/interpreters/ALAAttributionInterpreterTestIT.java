package au.org.ala.pipelines.interpreters;

import static org.junit.Assert.*;

import au.org.ala.kvs.cache.ALACollectionKVStoreFactory;
import au.org.ala.kvs.client.ALACollectionLookup;
import au.org.ala.kvs.client.ALACollectionMatch;
import au.org.ala.pipelines.vocabulary.ALAOccurrenceIssue;
import au.org.ala.util.IntegrationTestUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.io.avro.ALAAttributionRecord;
import org.gbif.pipelines.io.avro.ALAMetadataRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.ClassRule;
import org.junit.Test;

public class ALAAttributionInterpreterTestIT {

  @ClassRule public static IntegrationTestUtils itUtils = IntegrationTestUtils.getInstance();

  @Test
  public void testCollectionLookup() {

    KeyValueStore<ALACollectionLookup, ALACollectionMatch> kvs =
        ALACollectionKVStoreFactory.create(itUtils.getConfig());

    ALAMetadataRecord mdr =
        ALAMetadataRecord.newBuilder().setDataResourceUid("test").setId("test").build();

    BiConsumer<ExtendedRecord, ALAAttributionRecord> fcn =
        ALAAttributionInterpreter.interpretCodes(kvs, mdr);

    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.institutionCode.namespace() + DwcTerm.institutionCode.simpleName(), "CSIRO");
    map.put(DwcTerm.collectionCode.namespace() + DwcTerm.collectionCode.simpleName(), "ANIC");

    ALAAttributionRecord aar = ALAAttributionRecord.newBuilder().setId("1").build();

    fcn.accept(ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build(), aar);
    assertNotNull(aar.getCollectionUid());
    assertEquals("co13", aar.getCollectionUid());
    assertEquals(0, aar.getIssues().getIssueList().size());
  }

  @Test
  public void testCollectionLookupBadValues() {

    KeyValueStore<ALACollectionLookup, ALACollectionMatch> kvs =
        ALACollectionKVStoreFactory.create(itUtils.getConfig());

    ALAMetadataRecord mdr =
        ALAMetadataRecord.newBuilder().setDataResourceUid("test").setId("test").build();

    BiConsumer<ExtendedRecord, ALAAttributionRecord> fcn =
        ALAAttributionInterpreter.interpretCodes(kvs, mdr);

    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.institutionCode.namespace() + DwcTerm.institutionCode.simpleName(), "ANIC");
    map.put(
        DwcTerm.collectionCode.namespace() + DwcTerm.collectionCode.simpleName(), "Insects$$%%%$$");

    ALAAttributionRecord aar = ALAAttributionRecord.newBuilder().setId("1").build();

    fcn.accept(ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build(), aar);
    assertNull(aar.getCollectionUid());
    assertEquals(1, aar.getIssues().getIssueList().size());
    assertEquals(
        ALAOccurrenceIssue.UNRECOGNISED_COLLECTION_CODE.name(),
        aar.getIssues().getIssueList().get(0));
  }
}
