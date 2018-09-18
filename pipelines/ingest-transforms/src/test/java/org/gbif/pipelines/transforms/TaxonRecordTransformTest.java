package org.gbif.pipelines.transforms;

import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.api.v2.NameUsageMatch2;
import org.gbif.api.v2.RankedName;
import org.gbif.api.vocabulary.Rank;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.parsers.parsers.taxonomy.TaxonRecordConverter;
import org.gbif.pipelines.parsers.ws.config.WsConfig;
import org.gbif.pipelines.parsers.ws.config.WsConfigFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okio.BufferedSource;
import okio.Okio;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.AfterClass;
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

  @Rule public final transient TestPipeline p = TestPipeline.create();

  /** Public field because {@link ClassRule} requires it. */
  @ClassRule public static final MockWebServer MOCK_SERVER = new MockWebServer();

  private static WsConfig wsConfig;

  @ClassRule
  public static final ExternalResource CONFIG_RESOURCE =
      new ExternalResource() {

        @Override
        protected void before() {
          wsConfig = WsConfigFactory.create(MOCK_SERVER.url("/").toString());
        }
      };

  @AfterClass
  public static void close() throws IOException {
    MOCK_SERVER.close();
  }

  @Test
  @Category(NeedsRunner.class)
  public void transformationTest() throws IOException {

    enqueueDummyResponse();

    // State
    ExtendedRecord extendedRecord =
        ExtendedRecordCustomBuilder.create().id("1").name("foo").build();

    // When
    PCollection<TaxonRecord> recordCollection =
        p.apply(Create.of(extendedRecord)).apply(RecordTransforms.taxonomy(wsConfig));

    // Should
    PAssert.that(recordCollection).containsInAnyOrder(createTaxonRecordExpected());

    p.run();
  }

  private TaxonRecord createTaxonRecordExpected() {
    TaxonRecord taxonRecord = TaxonRecord.newBuilder().build();
    TaxonRecordConverter.convert(createDummyNameUsageMatch(), taxonRecord);
    taxonRecord.setId("1");
    return taxonRecord;
  }

  /** Sets the same values as in DUMMY_RESPONSE (dummy-response.json) */
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
    BufferedSource source;
    try (InputStream inputStream =
        Thread.currentThread()
            .getContextClassLoader()
            .getResourceAsStream("dummy-match-response.json")) {
      source = Okio.buffer(Okio.source(inputStream));
      MockResponse mockResponse = new MockResponse();
      MOCK_SERVER.enqueue(mockResponse.setBody(source.readString(StandardCharsets.UTF_8)));
      source.close();
    }
  }
}
