package org.gbif.pipelines.spark.util;

import static org.gbif.api.model.Constants.NUB_DATASET_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.species.NameUsageMatchRequest;
import org.gbif.pipelines.core.config.model.WsConfig;
import org.gbif.pipelines.core.interpreters.core.DnaTaxonomyInterpreter;
import org.gbif.pipelines.core.ws.sequenceclassifier.SequenceClassifierServiceClient;
import org.gbif.pipelines.core.ws.sequenceclassifier.request.ClassifyOccurrenceRequest;
import org.gbif.pipelines.core.ws.sequenceclassifier.response.ClassifyOccurrenceResponse;
import org.gbif.pipelines.io.avro.DnaDerivedData;
import org.gbif.pipelines.io.avro.DnaDerivedDataRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.spark.pojo.Occurrence;
import org.gbif.rest.client.species.NameUsageMatchResponse;
import org.junit.Test;

public class SequenceClassifierUtilsTest {

  @Test
  public void sequenceClassifierUtilsFiltersInvalidSequencesAndMapsCoordinates() throws Exception {
    DnaDerivedData valid =
        DnaDerivedData.newBuilder()
            .setNucleotideSequenceID("seq-1")
            .setSequence("ACGT")
            .setInvalid(false)
            .build();
    DnaDerivedData invalid =
        DnaDerivedData.newBuilder()
            .setNucleotideSequenceID("seq-2")
            .setSequence("NNNN")
            .setInvalid(true)
            .build();
    DnaDerivedData empty = DnaDerivedData.newBuilder().setNucleotideSequenceID("seq-3").build();
    DnaDerivedDataRecord dnaRecord =
        DnaDerivedDataRecord.newBuilder()
            .setId("occ-1")
            .setDnaDerivedDataItems(List.of(valid, invalid, empty))
            .build();

    LocationRecord location =
        LocationRecord.newBuilder().setDecimalLatitude(51.5d).setDecimalLongitude(-1.2d).build();

    Occurrence occurrence =
        Occurrence.builder()
            .id("occ-1")
            .dnaDerivedData(new ObjectMapper().writeValueAsString(dnaRecord))
            .location(new ObjectMapper().writeValueAsString(location))
            .build();

    ClassifyOccurrenceRequest request =
        SequenceClassifierUtils.toClassifyOccurrenceRequest(occurrence);

    assertEquals("occ-1", request.getGbifID());
    assertEquals(51.5d, request.getDecimalLatitude(), 0.00001d);
    assertEquals(-1.2d, request.getDecimalLongitude(), 0.00001d);
    assertEquals(1, request.getNucleotideSequence().size());
    assertEquals("seq-1", request.getNucleotideSequence().get(0).getNucleotideSequenceID());
    assertEquals("ACGT", request.getNucleotideSequence().get(0).getSequence());
    assertEquals(Boolean.FALSE, request.getNucleotideSequence().get(0).getInvalid());
  }

  @Test
  public void sequenceClassifierClientReadsBatchResponse() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/occurrence/classify/batch", exchange -> writeJsonResponse(exchange));
    server.start();

    WsConfig wsConfig = new WsConfig();
    wsConfig.setWsUrl("http://127.0.0.1:" + server.getAddress().getPort() + "/");
    wsConfig.setTimeoutSec(5);

    List<ClassifyOccurrenceRequest> requests =
        List.of(
            createRequest("req-1", "ACGT", 1),
            createRequest("req-2", "TTGG", 2),
            createRequest("req-3", "GGGG", 3),
            createRequest("req-4", "CCCC", 4),
            createRequest("req-5", "AAAA", 5));

    List<ClassifyOccurrenceResponse> results;
    try {
      results = SequenceClassifierServiceClient.create(wsConfig).classifyBatch(requests);
    } finally {
      server.stop(0);
    }

    assertEquals(5, results.size());
    assertEquals("1413807310", results.get(0).getGbifID());
    assertEquals("Camponotus repens", results.get(0).getClassification().getScientificName());
    assertEquals("Camponotus", results.get(1).getClassification().getScientificName());
    assertNull(results.get(2).getClassification());
    assertNull(results.get(3).getClassification());
    assertNull(results.get(4).getClassification());

    // Force Log4j to use the simple context to avoid hostname lookup from core initialization.
    String previousLoggerContextFactory = System.getProperty("log4j2.loggerContextFactory");
    System.setProperty(
        "log4j2.loggerContextFactory",
        "org.apache.logging.log4j.simple.SimpleLoggerContextFactory");
    try {
      NameUsageMatchResponse matchedUsage = new NameUsageMatchResponse();
      matchedUsage.setUsage(
          NameUsageMatchResponse.Usage.builder()
              .withName("Camponotus repens")
              .withKey("12345")
              .withRank("SPECIES")
              .build());
      matchedUsage.setClassification(
          List.of(
              NameUsageMatchResponse.RankedName.builder()
                  .withName("Camponotus")
                  .withKey("333")
                  .withRank("GENUS")
                  .build(),
              NameUsageMatchResponse.RankedName.builder()
                  .withName("Camponotus repens")
                  .withKey("12345")
                  .withRank("SPECIES")
                  .build()));
      matchedUsage.setDiagnostics(new NameUsageMatchResponse.Diagnostics());
      matchedUsage.getDiagnostics().setMatchType(NameUsageMatchResponse.MatchType.EXACT);

      KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse> kvStore =
          new KeyValueStore<>() {
            @Override
            public NameUsageMatchResponse get(NameUsageMatchRequest request) {
              if ("Camponotus repens".equalsIgnoreCase(request.getScientificName())) {
                return matchedUsage;
              }
              return null;
            }

            @Override
            public void close() throws IOException {}
          };

      TaxonRecord resolved = TaxonRecord.newBuilder().setId("x").build();
      DnaTaxonomyInterpreter.dnaTaxonomyInterpreter(kvStore, NUB_DATASET_KEY.toString())
          .accept(results.get(0), resolved);

      assertEquals("1413807310", resolved.getId());
      assertEquals(NUB_DATASET_KEY + "_dna_assisted", resolved.getDatasetKey());
      assertNotNull(resolved.getUsage());
      assertEquals("Camponotus repens", resolved.getUsage().getName());
      assertEquals("12345", resolved.getUsage().getKey());
      assertFalse(
          resolved.getIssues().getIssueList().contains(OccurrenceIssue.TAXON_MATCH_NONE.name()));

      TaxonRecord noMatch = TaxonRecord.newBuilder().setId("y").build();
      DnaTaxonomyInterpreter.dnaTaxonomyInterpreter(kvStore, NUB_DATASET_KEY.toString())
          .accept(results.get(2), noMatch);

      assertNotNull(noMatch.getIssues());
      assertTrue(
          noMatch.getIssues().getIssueList().contains(OccurrenceIssue.TAXON_MATCH_NONE.name()));
    } finally {
      if (previousLoggerContextFactory == null) {
        System.clearProperty("log4j2.loggerContextFactory");
      } else {
        System.setProperty("log4j2.loggerContextFactory", previousLoggerContextFactory);
      }
    }
  }

  private static ClassifyOccurrenceRequest createRequest(
      String nucleotideSequenceId, String sequence, long gbifId) {
    ClassifyOccurrenceRequest request = new ClassifyOccurrenceRequest();
    request.setGbifID(String.valueOf(gbifId));
    request.setNucleotideSequence(
        List.of(
            new ClassifyOccurrenceRequest.NucleotideSequence(
                nucleotideSequenceId, sequence, false)));
    return request;
  }

  private static void writeJsonResponse(HttpExchange exchange) throws IOException {
    String responseJson =
        """
      [
        {
          "gbifID": 1413807310,
          "classification": {
            "scientificName": "Camponotus repens",
            "taxonRank": "species",
            "kingdom": "Animalia",
            "phylum": "Arthropoda",
            "class": "Insecta",
            "order": "Hymenoptera",
            "family": "Formicidae",
            "genus": "Camponotus",
            "species": "Camponotus repens",
            "remarks": "2 sequence(s) matched; selected highest identity (100%) from boldistilled coi (coi, seqID=6498a8da9eb9e0ae4f25d215edcf1686); all matches: [6498a8da9eb9e0ae4f25d215edcf1686: dataset=boldistilled coi gene=coi identity=100 qcovs=100; 9a5ab3e31ba1480b344114bce9fd981e: dataset=midori2 12s gene=ssu rrna 12s mitochondrial identity=92.6 qcovs=62.5]"
          }
        },
        {
          "gbifID": 1413807586,
          "classification": {
            "scientificName": "Camponotus",
            "taxonRank": "genus",
            "kingdom": "Animalia",
            "phylum": "Arthropoda",
            "class": "Insecta",
            "order": "Hymenoptera",
            "family": "Formicidae",
            "genus": "Camponotus",
            "remarks": "1 sequence(s) matched; selected highest identity (99.8%) from boldistilled coi (coi, seqID=1774a9ee66ac41b1402d1515c30ad90e); all matches: [1774a9ee66ac41b1402d1515c30ad90e: dataset=boldistilled coi gene=coi identity=99.8 qcovs=100]"
          }
        },
        {"gbifID": 1413810076, "classification": null},
        {"gbifID": 999000001, "classification": null},
        {"gbifID": 999000002, "classification": null}
      ]
      """;

    byte[] payload = responseJson.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/json");
    exchange.sendResponseHeaders(200, payload.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(payload);
    } finally {
      exchange.close();
    }
  }
}
