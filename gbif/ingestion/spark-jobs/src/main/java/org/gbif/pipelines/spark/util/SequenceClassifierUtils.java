package org.gbif.pipelines.spark.util;

import static org.gbif.pipelines.spark.Directories.DNA_TAXON;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.converters.OccurrenceJsonConverter;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.core.ws.sequenceclassifier.SequenceClassifierServiceClient;
import org.gbif.pipelines.core.ws.sequenceclassifier.request.ClassifyOccurrenceRequest;
import org.gbif.pipelines.core.ws.sequenceclassifier.response.ClassifyOccurrenceResponse;
import org.gbif.pipelines.io.avro.DnaDerivedData;
import org.gbif.pipelines.io.avro.DnaDerivedDataRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.spark.pojo.Occurrence;
import org.gbif.pipelines.transform.factory.MultiTaxonomyKVSFactory;

public class SequenceClassifierUtils {

  private static final ObjectMapper MAPPER = MapperUtil.MAPPER_NON_EMPTY;

  /**
   * Classifies the occurrences that have DNA sequences, calling the ws in batches and writes the
   * resulting {@link TaxonRecord}s as parquet.
   */
  public static Dataset<TaxonRecord> interpretDnaTaxonAndSave(
      SparkSession spark, Dataset<Occurrence> input, PipelinesConfig config, String outputPath) {

    Dataset<TaxonRecord> output =
        input.mapPartitions(
            (MapPartitionsFunction<Occurrence, TaxonRecord>)
                partition -> interpretPartition(partition, config),
            Encoders.bean(TaxonRecord.class));

    output.write().mode(SaveMode.Overwrite).parquet(outputPath + "/" + DNA_TAXON);

    return spark.read().parquet(outputPath + "/" + DNA_TAXON).as(Encoders.bean(TaxonRecord.class));
  }

  private static Iterator<TaxonRecord> interpretPartition(
      Iterator<Occurrence> partition, PipelinesConfig config) {

    // created here because they are not serializable
    SequenceClassifierServiceClient client =
        SequenceClassifierServiceClient.create(
            config.getDnaConfig().getSequenceClassifierWsConfig());

    BiConsumer<ClassifyOccurrenceResponse, TaxonRecord> taxonomyInterpreter =
        org.gbif.pipelines.core.interpreters.core.DnaTaxonomyInterpreter.dnaTaxonomyInterpreter(
            MultiTaxonomyKVSFactory.getKvStore(config),
            OccurrenceJsonConverter.DEFAULT_TAXONOMY_KEY);

    List<TaxonRecord> output = new ArrayList<>();
    List<ClassifyOccurrenceRequest> batch =
        new ArrayList<>(config.getDnaConfig().getBatchSizeSequenceClassifier());

    while (partition.hasNext()) {
      ClassifyOccurrenceRequest request = toClassifyOccurrenceRequest(partition.next());
      if (request == null) {
        continue; // nothing to classify: don't spend a ws call on it
      }

      batch.add(request);
      if (batch.size() == config.getDnaConfig().getBatchSizeSequenceClassifier()) {
        output.addAll(classifyAndInterpret(client, taxonomyInterpreter, batch));
        batch.clear();
      }
    }
    if (!batch.isEmpty()) {
      output.addAll(classifyAndInterpret(client, taxonomyInterpreter, batch));
    }

    return output.iterator();
  }

  /** Returns null if the occurrence has no valid nucleotide sequences. */
  static ClassifyOccurrenceRequest toClassifyOccurrenceRequest(Occurrence record) {
    DnaDerivedDataRecord dna = read(record.getDnaDerivedData(), DnaDerivedDataRecord.class);
    if (dna == null || dna.getDnaDerivedDataItems() == null) {
      return null;
    }

    // Same rule as the ws: non-empty sequence and not flagged as invalid
    List<ClassifyOccurrenceRequest.NucleotideSequence> sequences = new ArrayList<>();
    for (DnaDerivedData item : dna.getDnaDerivedDataItems()) {
      boolean hasSequence = item.getSequence() != null && !item.getSequence().isEmpty();
      // avoid sending invalid sequences to minimize the ws calls
      if (hasSequence && !Boolean.TRUE.equals(item.getInvalid())) {
        sequences.add(
            new ClassifyOccurrenceRequest.NucleotideSequence(
                item.getNucleotideSequenceID(), item.getSequence(), item.getInvalid()));
      }
    }
    if (sequences.isEmpty()) {
      return null;
    }

    ClassifyOccurrenceRequest occurrence = new ClassifyOccurrenceRequest();
    occurrence.setGbifID(record.getId());
    occurrence.setNucleotideSequence(sequences);

    LocationRecord location = read(record.getLocation(), LocationRecord.class);
    if (location != null) {
      occurrence.setDecimalLatitude(location.getDecimalLatitude());
      occurrence.setDecimalLongitude(location.getDecimalLongitude());
    }

    return occurrence;
  }

  private static <T> T read(String json, Class<T> clazz) {
    if (json == null || json.trim().isEmpty()) {
      return null;
    }
    try {
      return MAPPER.readValue(json, clazz);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException("Cannot parse " + clazz.getSimpleName() + ": " + json, e);
    }
  }

  private static List<TaxonRecord> classifyAndInterpret(
      SequenceClassifierServiceClient client,
      BiConsumer<ClassifyOccurrenceResponse, TaxonRecord> taxonomyInterpreter,
      List<ClassifyOccurrenceRequest> batch) {

    List<ClassifyOccurrenceResponse> results = client.classifyBatch(batch);

    List<TaxonRecord> records = new ArrayList<>(batch.size());
    for (ClassifyOccurrenceResponse result : results) {
      TaxonRecord tr = TaxonRecord.newBuilder().setCreated(System.currentTimeMillis()).build();
      taxonomyInterpreter.accept(result, tr);
      records.add(tr);
    }
    return records;
  }

  public static Dataset<TaxonRecord> loadDnaTaxonRecords(
      SparkSession spark, PipelinesConfig config, String outputPath) {
    String dnaTaxonPath = outputPath + "/" + DNA_TAXON;
    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(config.getHdfsSiteConfig(), config.getCoreSiteConfig());

    return FsUtils.fileExists(hdfsConfigs, dnaTaxonPath)
        ? spark.read().parquet(dnaTaxonPath).as(Encoders.bean(TaxonRecord.class))
        : spark.emptyDataset(Encoders.bean(TaxonRecord.class));
  }
}
