package org.gbif.pipelines.transforms.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.gbif.api.model.collections.lookup.Match.MatchType;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Country;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.grscicoll.GrscicollLookupRequest;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.rest.client.grscicoll.GrscicollLookupResponse;
import org.gbif.rest.client.grscicoll.GrscicollLookupResponse.EntityMatchedResponse;
import org.gbif.rest.client.grscicoll.GrscicollLookupResponse.Match;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests the {@link GrscicollTransform}. */
@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class GrscicollTransformTest {

  private static final UUID DATASET_KEY = UUID.fromString("08f7aaa6-8113-43ef-9ac6-a70cbc48079f");
  private static final Country COUNTRY = Country.DENMARK;
  private static final UUID INSTITUTION_KEY =
      UUID.fromString("18f7aaa6-8113-43ef-9ac6-a70cbc48079f");
  private static final UUID COLLECTION_KEY =
      UUID.fromString("28f7aaa6-8113-43ef-9ac6-a70cbc48079f");
  private static final String INSTITUTION_CODE = "i1";
  private static final String COLLECTION_CODE = "c1";
  private static final String FAKE_INSTITUTION_CODE = "fake";

  private static final SerializableSupplier<
          KeyValueStore<GrscicollLookupRequest, GrscicollLookupResponse>>
      KV_STORE = createKvStore();

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void whenSpecimenRecordThenRecordsFlag() {
    // State
    final String[] verbatim = {
      BasisOfRecord.PRESERVED_SPECIMEN.name(), "1", INSTITUTION_CODE, "", "", COLLECTION_CODE, ""
    };

    // When
    PCollection<GrscicollRecord> recordCollection = transformRecords(verbatim);

    // Should
    PAssert.that(recordCollection)
        .satisfies(
            r -> {
              GrscicollRecord rec = r.iterator().next();
              assertNotNull(rec.getId());
              assertEquals(1, rec.getIssues().getIssueList().size());
              assertNotNull(rec.getInstitutionMatch().getKey());
              assertNotNull(rec.getCollectionMatch().getKey());
              return null;
            });

    // run pipeline with the options required
    p.run();
  }

  @Test
  public void whenNonSpecimenRecordThenRecordsNotFlag() {
    // State
    final String[] verbatim = {
      BasisOfRecord.OBSERVATION.name(), "1", INSTITUTION_CODE, "", "", COLLECTION_CODE, ""
    };

    // When
    PCollection<GrscicollRecord> recordCollection = transformRecords(verbatim);

    // Should
    PAssert.that(recordCollection)
        .satisfies(
            r -> {
              GrscicollRecord rec = r.iterator().next();
              assertNotNull(rec.getId());
              assertTrue(rec.getIssues().getIssueList().isEmpty());
              assertNotNull(rec.getInstitutionMatch().getKey());
              assertNotNull(rec.getCollectionMatch().getKey());
              return null;
            });

    // run pipeline with the options required
    p.run();
  }

  @Test
  public void whenInstitutionMatchNoneThenCollectionsSkipped() {
    // State
    final String[] verbatim = {
      BasisOfRecord.PRESERVED_SPECIMEN.name(),
      "1",
      FAKE_INSTITUTION_CODE,
      "",
      "",
      COLLECTION_CODE,
      ""
    };

    // When
    PCollection<GrscicollRecord> recordCollection = transformRecords(verbatim);

    // Should
    PAssert.that(recordCollection)
        .satisfies(
            r -> {
              GrscicollRecord rec = r.iterator().next();
              assertNotNull(rec.getId());
              assertEquals(1, rec.getIssues().getIssueList().size());
              assertNull(rec.getInstitutionMatch());
              assertNull(rec.getCollectionMatch());
              return null;
            });

    // run pipeline with the options required
    p.run();
  }

  private PCollection<GrscicollRecord> transformRecords(String... verbatimRecords) {

    final MetadataRecord mdr =
        MetadataRecord.newBuilder()
            .setId("1")
            .setDatasetPublishingCountry(COUNTRY.getIso2LetterCode())
            .setDatasetKey(DATASET_KEY.toString())
            .build();

    final List<ExtendedRecord> records = createExtendedRecordList(mdr, verbatimRecords);

    PCollectionView<MetadataRecord> metadataView =
        p.apply("Create test metadata", Create.of(mdr))
            .apply("Convert into view", View.asSingleton());

    PCollection<ExtendedRecord> extendedRecordsKv = p.apply("er", Create.of(records));

    return extendedRecordsKv.apply(
        "grscicoll transform",
        GrscicollTransform.builder()
            .kvStoreSupplier(KV_STORE)
            .metadataView(metadataView)
            .create()
            .interpret());
  }

  private List<ExtendedRecord> createExtendedRecordList(
      MetadataRecord metadataRecord, String[]... records) {
    return Arrays.stream(records)
        .map(
            x -> {
              ExtendedRecord record = ExtendedRecord.newBuilder().setId(x[1]).build();
              Map<String, String> terms = record.getCoreTerms();
              terms.put(DwcTerm.basisOfRecord.qualifiedName(), x[0]);
              terms.put(DwcTerm.institutionCode.qualifiedName(), x[2]);
              terms.put(DwcTerm.institutionID.qualifiedName(), x[3]);
              terms.put(DwcTerm.ownerInstitutionCode.qualifiedName(), x[4]);
              terms.put(DwcTerm.collectionCode.qualifiedName(), x[5]);
              terms.put(DwcTerm.collectionID.qualifiedName(), x[6]);
              terms.put(
                  GbifTerm.publishingCountry.qualifiedName(),
                  metadataRecord.getDatasetPublishingCountry());
              return record;
            })
        .collect(Collectors.toList());
  }

  private static SerializableSupplier<
          KeyValueStore<GrscicollLookupRequest, GrscicollLookupResponse>>
      createKvStore() {
    GrscicollLookupRequest request1 = new GrscicollLookupRequest();
    request1.setInstitutionCode(INSTITUTION_CODE);
    request1.setCollectionCode(COLLECTION_CODE);
    request1.setDatasetKey(DATASET_KEY.toString());
    request1.setCountry(COUNTRY.getIso2LetterCode());

    GrscicollLookupResponse response1 = new GrscicollLookupResponse();
    Match institutionMatch = new Match();
    institutionMatch.setMatchType(MatchType.EXACT);
    EntityMatchedResponse instMatchResponse1 = new EntityMatchedResponse();
    instMatchResponse1.setKey(INSTITUTION_KEY);
    institutionMatch.setEntityMatched(instMatchResponse1);
    response1.setInstitutionMatch(institutionMatch);

    Match collectionMatch = new Match();
    collectionMatch.setMatchType(MatchType.FUZZY);
    EntityMatchedResponse collMatchResponse1 = new EntityMatchedResponse();
    collMatchResponse1.setKey(COLLECTION_KEY);
    collectionMatch.setEntityMatched(collMatchResponse1);
    response1.setCollectionMatch(collectionMatch);

    GrscicollLookupRequest request2 = new GrscicollLookupRequest();
    request2.setInstitutionCode(FAKE_INSTITUTION_CODE);
    request2.setCollectionCode(COLLECTION_CODE);
    request2.setDatasetKey(DATASET_KEY.toString());
    request2.setCountry(COUNTRY.getIso2LetterCode());

    GrscicollLookupResponse response2 = new GrscicollLookupResponse();
    Match institutionMatch2 = new Match();
    institutionMatch2.setMatchType(MatchType.NONE);
    response2.setInstitutionMatch(institutionMatch2);

    KeyValueTestStoreStub<GrscicollLookupRequest, GrscicollLookupResponse> kvStore =
        new KeyValueTestStoreStub<>();
    kvStore.put(request1, response1);
    kvStore.put(request2, response2);

    return () -> kvStore;
  }
}
