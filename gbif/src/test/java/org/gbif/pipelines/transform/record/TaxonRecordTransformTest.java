package org.gbif.pipelines.transform.record;

import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.api.v2.NameUsageMatch2;
import org.gbif.api.v2.RankedName;
import org.gbif.api.vocabulary.Rank;
import org.gbif.pipelines.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.core.parsers.taxonomy.TaxonRecordConverter;
import org.gbif.pipelines.core.utils.ExtendedRecordCustomBuilder;
import org.gbif.pipelines.core.ws.config.Config;
import org.gbif.pipelines.core.ws.config.HttpConfigFactory;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.taxon.TaxonRecord;
import org.gbif.pipelines.transform.Kv2Value;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okio.BufferedSource;
import okio.Okio;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.gbif.api.vocabulary.TaxonomicStatus.ACCEPTED;

@RunWith(JUnit4.class)
public class TaxonRecordTransformTest {

  private static final String TEST_ID = "1";

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  /**
   * Public field because {@link ClassRule} requires it.
   */
  @ClassRule
  public static final MockWebServer mockServer = new MockWebServer();

  private static Config wsConfig;

  @ClassRule
  public static final ExternalResource configResource = new ExternalResource() {

    @Override
    protected void before() {
      wsConfig = HttpConfigFactory.createConfigFromUrl(mockServer.url("/").toString());
    }
  };

  @Test
  @Category(NeedsRunner.class)
  public void testTransformation() throws IOException {

    enqueueDummyResponse();

    // State
    ExtendedRecord extendedRecord = ExtendedRecordCustomBuilder.create().id(TEST_ID).name("foo").build();

    // When
    TaxonRecordTransform taxonRecordTransform = TaxonRecordTransform.create(wsConfig).withAvroCoders(p);

    PCollection<ExtendedRecord> inputStream = p.apply(Create.of(extendedRecord));

    PCollectionTuple tuple = inputStream.apply(taxonRecordTransform);

    PCollection<TaxonRecord> recordCollection = tuple.get(taxonRecordTransform.getDataTag()).apply(Kv2Value.create());

    // Should
    PAssert.that(recordCollection).containsInAnyOrder(createTaxonRecordExpected());

    // run pipeline with the options required
    DataProcessingPipelineOptions options = PipelineOptionsFactory.create().as(DataProcessingPipelineOptions.class);
    p.run(options);
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

  private static void enqueueDummyResponse() throws IOException {
    InputStream inputStream =
      Thread.currentThread().getContextClassLoader().getResourceAsStream("dummy-match-response.json");
    BufferedSource source = Okio.buffer(Okio.source(inputStream));
    MockResponse mockResponse = new MockResponse();
    mockServer.enqueue(mockResponse.setBody(source.readString(StandardCharsets.UTF_8)));
  }

}