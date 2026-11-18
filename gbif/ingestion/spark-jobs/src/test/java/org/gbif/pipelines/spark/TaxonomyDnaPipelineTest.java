package org.gbif.pipelines.spark;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.function.BiConsumer;
import javax.xml.ws.WebServiceException;
import org.apache.spark.util.LongAccumulator;
import org.gbif.pipelines.core.config.model.DnaConfig;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.config.model.WsConfig;
import org.gbif.pipelines.core.converters.OccurrenceJsonConverter;
import org.gbif.pipelines.core.ws.sequenceclassifier.SequenceClassifierServiceClient;
import org.gbif.pipelines.core.ws.sequenceclassifier.request.ClassifyOccurrenceRequest;
import org.gbif.pipelines.core.ws.sequenceclassifier.response.ClassifyOccurrenceResponse;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.spark.pojo.Occurrence;
import org.junit.jupiter.api.Test;

class TaxonomyDnaPipelineTest {

  private static final String DNA_DERIVED_DATA =
      "{\"dnaDerivedDataItems\": [{\"sequence\": \"ACGTACGTACGTACGTACGT\"}]}";

  @Test
  void noRequestWithoutDnaDerivedData() {
    assertNull(
        TaxonomyDnaPipeline.toClassifyOccurrenceRequest(Occurrence.builder().id("1").build()));
  }

  @Test
  void requestWithoutLocation() {
    ClassifyOccurrenceRequest request =
        TaxonomyDnaPipeline.toClassifyOccurrenceRequest(
            Occurrence.builder().id("1").dnaDerivedData(DNA_DERIVED_DATA).build());

    assertEquals("1", request.getGbifID());
    assertEquals(1, request.getNucleotideSequence().size());
    assertNull(request.getDecimalLatitude());
  }

  /** A missing classifier ws fails on the driver with a clear message, not inside a Spark task. */
  @Test
  void missingSequenceClassifierWsFailsFast() {
    PipelinesConfig config = new PipelinesConfig();
    config.setDnaConfig(new DnaConfig());

    IllegalStateException ex =
        assertThrows(
            IllegalStateException.class, () -> TaxonomyDnaPipeline.validateDnaConfig(config));
    assertTrue(ex.getMessage().contains("dnaConfig.sequenceClassifierWsConfig.wsUrl"));
  }

  @Test
  void missingSequenceClassifierWsUrlFailsFast() {
    PipelinesConfig config = new PipelinesConfig();
    config.setDnaConfig(new DnaConfig());
    config.getDnaConfig().setSequenceClassifierWsConfig(new WsConfig());

    assertThrows(IllegalStateException.class, () -> TaxonomyDnaPipeline.validateDnaConfig(config));
  }

  @Test
  void configuredSequenceClassifierWsIsValid() {
    PipelinesConfig config = new PipelinesConfig();
    config.setDnaConfig(new DnaConfig());
    WsConfig wsConfig = new WsConfig();
    wsConfig.setWsUrl("http://localhost:8080");
    config.getDnaConfig().setSequenceClassifierWsConfig(wsConfig);

    assertDoesNotThrow(() -> TaxonomyDnaPipeline.validateDnaConfig(config));
  }

  @Test
  void defaultTaxonomyWhenNoDnaChecklistsConfigured() {
    PipelinesConfig config = new PipelinesConfig();
    config.setDnaConfig(new DnaConfig());

    assertEquals(
        List.of(OccurrenceJsonConverter.DEFAULT_TAXONOMY_KEY),
        TaxonomyDnaPipeline.getDnaChecklistKeys(config));
  }

  @Test
  void configuredDnaChecklists() {
    PipelinesConfig config = new PipelinesConfig();
    config.setDnaConfig(new DnaConfig());
    config.getDnaConfig().setChecklistKeys(List.of("checklist-a", "checklist-b"));

    assertEquals(
        List.of("checklist-a", "checklist-b"), TaxonomyDnaPipeline.getDnaChecklistKeys(config));
  }

  private static final List<BiConsumer<ClassifyOccurrenceResponse, TaxonRecord>> INTERPRETERS =
      List.of((response, tr) -> tr.setDatasetKey(OccurrenceJsonConverter.DEFAULT_TAXONOMY_KEY));

  private static List<Occurrence> occurrences() {
    return List.of(
        Occurrence.builder().id("1").dnaDerivedData(DNA_DERIVED_DATA).build(),
        // no sequences: not sent to the classifier
        Occurrence.builder().id("2").build());
  }

  /** The DNA classification is an enrichment: if it fails, the occurrences are kept without it. */
  @Test
  void failedClassificationKeepsOccurrencesWithoutDnaTaxon() {
    SequenceClassifierServiceClient client = mock(SequenceClassifierServiceClient.class);
    when(client.classifyBatch(any())).thenThrow(new WebServiceException("ws down"));
    LongAccumulator classified = new LongAccumulator();
    LongAccumulator failed = new LongAccumulator();
    LongAccumulator missing = new LongAccumulator();

    List<Occurrence> occurrences =
        TaxonomyDnaPipeline.interpretOccurrenceDnaTaxon(
            occurrences(), client, INTERPRETERS, "dataset", classified, failed, missing);

    assertEquals(2, occurrences.size());
    assertNull(occurrences.get(0).getDnaTaxon());
    assertNull(occurrences.get(1).getDnaTaxon());
    assertEquals(0L, classified.value().longValue());
    assertEquals(1L, failed.value().longValue());
    assertEquals(0L, missing.value().longValue());
  }

  @Test
  void classificationCountsClassifiedOccurrences() {
    SequenceClassifierServiceClient client = mock(SequenceClassifierServiceClient.class);
    ClassifyOccurrenceResponse response = new ClassifyOccurrenceResponse();
    response.setGbifID("1");
    when(client.classifyBatch(any())).thenReturn(List.of(response));
    LongAccumulator classified = new LongAccumulator();
    LongAccumulator failed = new LongAccumulator();
    LongAccumulator missing = new LongAccumulator();

    List<Occurrence> occurrences =
        TaxonomyDnaPipeline.interpretOccurrenceDnaTaxon(
            occurrences(), client, INTERPRETERS, "dataset", classified, failed, missing);

    assertNotNull(occurrences.get(0).getDnaTaxon());
    assertNull(occurrences.get(1).getDnaTaxon());
    assertEquals(1L, classified.value().longValue());
    assertEquals(0L, failed.value().longValue());
    assertEquals(0L, missing.value().longValue());
  }

  /** An occurrence without a result, or with a result for another gbifID, is counted as missing. */
  @Test
  void occurrenceWithoutResultIsCountedAsMissing() {
    SequenceClassifierServiceClient client = mock(SequenceClassifierServiceClient.class);
    ClassifyOccurrenceResponse unexpected = new ClassifyOccurrenceResponse();
    unexpected.setGbifID("unexpected");
    when(client.classifyBatch(any())).thenReturn(List.of(unexpected));
    LongAccumulator classified = new LongAccumulator();
    LongAccumulator failed = new LongAccumulator();
    LongAccumulator missing = new LongAccumulator();

    List<Occurrence> occurrences =
        TaxonomyDnaPipeline.interpretOccurrenceDnaTaxon(
            occurrences(), client, INTERPRETERS, "dataset", classified, failed, missing);

    assertNull(occurrences.get(0).getDnaTaxon());
    assertEquals(0L, classified.value().longValue());
    assertEquals(0L, failed.value().longValue());
    assertEquals(1L, missing.value().longValue());
  }
}
