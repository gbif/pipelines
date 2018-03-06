package org.gbif.pipelines.transform;

import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.api.v2.NameUsageMatch2;
import org.gbif.api.v2.RankedName;
import org.gbif.api.vocabulary.Rank;
import org.gbif.pipelines.core.TypeDescriptors;
import org.gbif.pipelines.core.utils.ExtendedRecordCustomBuilder;
import org.gbif.pipelines.interpretation.parsers.taxonomy.TaxonRecordConverter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.ws.MockServer;

import java.io.IOException;
import java.util.Collections;

import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.gbif.api.vocabulary.TaxonomicStatus.ACCEPTED;

@RunWith(JUnit4.class)
public class TaxonRecordTransformTest extends MockServer {

  private static final String TEST_ID = "1";

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @BeforeClass
  public static void setUp() throws IOException {
    mockServerSetUp();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    mockServerTearDown();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testTransformation() throws IOException {

    enqueueResponse(DUMMY_RESPONSE);

    // State
    ExtendedRecord extendedRecord = new ExtendedRecordCustomBuilder().id(TEST_ID).name("foo").build();

    // When
    TaxonRecordTransform taxonRecordTransform = new TaxonRecordTransform().withAvroCoders(p);

    PCollection<ExtendedRecord> inputStream = p.apply(Create.of(extendedRecord));

    PCollectionTuple tuple = inputStream.apply(taxonRecordTransform);

    PCollection<TaxonRecord> recordCollection = tuple.get(taxonRecordTransform.getDataTag())
      .apply(MapElements.into(TypeDescriptors.taxonRecord()).via(KV::getValue));

    // Should
    PAssert.that(recordCollection).containsInAnyOrder(createTaxonRecordExpected());
    p.run();

  }

  private TaxonRecord createTaxonRecordExpected() {
    TaxonRecord taxonRecord = new TaxonRecord();
    TaxonRecordConverter.convert(createDummyNameUsageMatch(), taxonRecord);
    taxonRecord.setId(TEST_ID);
    return taxonRecord;
  }

  /**
   * Sets the same values as in DUMMY_RESPONSE (dummy-response.json)
   */
  private NameUsageMatch2 createDummyNameUsageMatch() {
    NameUsageMatch2 nameUsageMatch2 = new NameUsageMatch2();

    RankedName usage = new RankedName();
    usage.setKey(123);
    usage.setName("test");
    usage.setRank(Rank.SPECIES);
    nameUsageMatch2.setUsage(usage);

    RankedName kingdom = new RankedName();
    kingdom.setKey(1);
    kingdom.setName("Animalia");
    kingdom.setRank(Rank.KINGDOM);
    nameUsageMatch2.setClassification(Collections.singletonList(kingdom));

    NameUsageMatch2.Diagnostics diagnostics = new NameUsageMatch2.Diagnostics();
    diagnostics.setMatchType(NameUsageMatch.MatchType.EXACT);
    diagnostics.setStatus(ACCEPTED);
    nameUsageMatch2.setDiagnostics(diagnostics);

    return nameUsageMatch2;
  }

}