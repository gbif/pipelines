/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.pipelines.spark;

import static org.gbif.pipelines.spark.Directories.*;
import static org.gbif.pipelines.spark.util.PipelinesConfigUtil.loadConfig;
import static org.gbif.pipelines.spark.util.SparkUtil.getFileSystem;
import static org.gbif.pipelines.spark.util.SparkUtil.getSparkSession;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterators;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.logging.log4j.ThreadContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.converters.OccurrenceJsonConverter;
import org.gbif.pipelines.core.ws.sequenceclassifier.SequenceClassifierServiceClient;
import org.gbif.pipelines.core.ws.sequenceclassifier.request.ClassifyOccurrenceRequest;
import org.gbif.pipelines.core.ws.sequenceclassifier.response.ClassifyOccurrenceResponse;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.spark.pojo.Occurrence;
import org.gbif.pipelines.spark.util.MapperUtil;
import org.gbif.pipelines.spark.util.SingleDatasetPipelineArgs;
import org.gbif.pipelines.transform.factory.MultiTaxonomyKVSFactory;

/** Main class for the Spark pipeline that just reruns DNA taxonomy interpretation. */
@Slf4j
public class TaxonomyDnaPipeline {

  static final ObjectMapper MAPPER = MapperUtil.MAPPER;

  public static final String METRICS_FILENAME = "taxonomy.yml";

  @Parameters(separators = "=")
  private static class Args extends SingleDatasetPipelineArgs {}

  public static void main(String[] argsv) throws Exception {
    Args args = new Args();
    JCommander jCommander = new JCommander(args);
    jCommander.setAcceptUnknownOptions(true);
    jCommander.parse(argsv);

    if (args.help) {
      jCommander.usage();
      return;
    }

    PipelinesConfig config = loadConfig(args.config);
    String datasetId = args.datasetId;
    int attempt = args.attempt;
    SparkSession spark = null;
    FileSystem fileSystem = null;

    try {
      /* ############ standard init block ########## */
      spark = getSparkSession(args.master, args.appName, config, (builder, pipelinesConfig) -> {});
      fileSystem = getFileSystem(spark, config);
      /* ############ standard init block - end ########## */

      runDnaTaxonomy(spark, fileSystem, config, datasetId, attempt);
    } finally {
      if (fileSystem != null) {
        fileSystem.close();
      }
      if (spark != null) {
        spark.stop();
        spark.close();
      }
    }
    System.exit(0);
  }

  public static Dataset<Occurrence> runDnaTaxonomy(
      SparkSession spark, FileSystem fs, PipelinesConfig config, String datasetId, int attempt)
      throws Exception {

    long start = System.currentTimeMillis();

    ThreadContext.put("datasetKey", datasetId);
    log.info("Starting taxonomy refresh");

    String outputPath = String.format("%s/%s/%d", config.getOutputPath(), datasetId, attempt);

    Dataset<Occurrence> simpleRecords = loadSimpleRecords(spark, outputPath);

    long recordCount = simpleRecords.count();

    runDnaTaxonomyTransform(config, simpleRecords, outputPath);

    // replace directories
    fs.delete(new org.apache.hadoop.fs.Path(outputPath + "/" + SIMPLE_OCCURRENCE), true);
    fs.rename(
        new org.apache.hadoop.fs.Path(outputPath + "/" + DNA_TAXON_TEMP),
        new org.apache.hadoop.fs.Path(outputPath + "/" + SIMPLE_OCCURRENCE));

    log.info(
        "Finished taxonomy re-interpretation in {} secs, records: {}",
        (System.currentTimeMillis() - start) / 1000,
        recordCount);

    return spark
        .read()
        .parquet(outputPath + "/" + SIMPLE_OCCURRENCE)
        .as(Encoders.bean(Occurrence.class));
  }

  private static Dataset<Occurrence> loadSimpleRecords(SparkSession spark, String outputPath) {
    return spark
        .read()
        .parquet(outputPath + "/" + SIMPLE_OCCURRENCE)
        .as(Encoders.bean(Occurrence.class));
  }

  /**
   * Classifies the occurrences that have DNA sequences, calling the ws in batches and adds the
   * result to the existing {@link Occurrence} record.
   */
  private static void runDnaTaxonomyTransform(
      PipelinesConfig config, Dataset<Occurrence> input, String outputPath) {

    Dataset<Occurrence> output =
        input.mapPartitions(
            (MapPartitionsFunction<Occurrence, Occurrence>)
                occurrenceIterator -> {
                  // created here because they are not serializable
                  SequenceClassifierServiceClient client =
                      SequenceClassifierServiceClient.create(
                          config.getDnaConfig().getSequenceClassifierWsConfig());

                  BiConsumer<ClassifyOccurrenceResponse, TaxonRecord> taxonomyInterpreter =
                      org.gbif.pipelines.core.interpreters.core.DnaTaxonomyInterpreter
                          .dnaTaxonomyInterpreter(
                              MultiTaxonomyKVSFactory.getKvStore(config),
                              OccurrenceJsonConverter.DEFAULT_TAXONOMY_KEY);

                  int batchSize = config.getDnaConfig().getBatchSizeSequenceClassifier();

                  return Iterators.concat(
                      Iterators.transform(
                          Iterators.partition(occurrenceIterator, batchSize),
                          group ->
                              interpretOccurrenceDnaTaxon(group, client, taxonomyInterpreter)
                                  .iterator()));
                },
            Encoders.bean(Occurrence.class));

    output.write().mode(SaveMode.Overwrite).parquet(outputPath + "/" + DNA_TAXON_TEMP);
  }

  /** Sets dnaTaxon on the occurrences of the group that have sequences, the rest are untouched. */
  @SneakyThrows
  private static List<Occurrence> interpretOccurrenceDnaTaxon(
      List<Occurrence> occurrencePartition,
      SequenceClassifierServiceClient client,
      BiConsumer<ClassifyOccurrenceResponse, TaxonRecord> taxonomyInterpreter) {

    List<ClassifyOccurrenceRequest> requests = new ArrayList<>();
    List<Occurrence> occurrencesRequested = new ArrayList<>();
    for (Occurrence occurrence : occurrencePartition) {
      ClassifyOccurrenceRequest request = toClassifyOccurrenceRequest(occurrence);
      if (request != null) { // nothing to classify: don't spend a ws call on it
        requests.add(request);
        occurrencesRequested.add(occurrence);
      }
    }

    if (!requests.isEmpty()) {
      List<TaxonRecord> taxa = classifyAndInterpret(client, taxonomyInterpreter, requests);

      Map<String, TaxonRecord> dnaTaxonById = new HashMap<>();
      for (TaxonRecord taxon : taxa) {
        dnaTaxonById.put(taxon.getId(), taxon);
      }

      for (Occurrence occurrence : occurrencesRequested) {
        TaxonRecord taxon = dnaTaxonById.get(occurrence.getId());
        if (taxon != null) {
          occurrence.setDnaTaxon(MAPPER.writeValueAsString(taxon));
        }
      }
    }
    return occurrencePartition;
  }

  /** Returns null if the occurrence has no valid nucleotide sequences. */
  @SneakyThrows
  static ClassifyOccurrenceRequest toClassifyOccurrenceRequest(Occurrence record) {
    DnaDerivedDataRecord dna =
        MAPPER.readValue(record.getDnaDerivedData(), DnaDerivedDataRecord.class);
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

    LocationRecord location = MAPPER.readValue(record.getLocation(), LocationRecord.class);
    if (location != null) {
      occurrence.setDecimalLatitude(location.getDecimalLatitude());
      occurrence.setDecimalLongitude(location.getDecimalLongitude());
    }

    return occurrence;
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
}
