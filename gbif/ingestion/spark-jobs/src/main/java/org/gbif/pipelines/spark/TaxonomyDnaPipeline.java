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

import static org.gbif.pipelines.core.utils.MetricsUtil.writeMetricsYaml;
import static org.gbif.pipelines.spark.Directories.*;
import static org.gbif.pipelines.spark.util.PipelinesConfigUtil.loadConfig;
import static org.gbif.pipelines.spark.util.SparkUtil.getFileSystem;
import static org.gbif.pipelines.spark.util.SparkUtil.getSparkSession;
import static org.gbif.pipelines.spark.util.SparkUtil.readParquetAs;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterators;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.spark.util.LongAccumulator;
import org.gbif.pipelines.common.PipelinesVariables;
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

  // same as the other fields of the occurrence, written by OccurrenceInterpretationPipeline
  static final ObjectMapper MAPPER = MapperUtil.MAPPER_NON_EMPTY;

  public static final String METRICS_FILENAME = "dna-taxonomy.yml";

  @Parameters(separators = "=")
  private static class Args extends SingleDatasetPipelineArgs {}

  /** The remarks when the classifier returns no classification, as it doesn't say why. */
  static final String NO_CLASSIFICATION_REMARKS = "No sequences matched in the classifier";

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

  /**
   * Checks the configuration needed by the DNA taxonomy on the driver, so a missing setting fails
   * before running any Spark task, with a clear message.
   */
  static void validateDnaConfig(PipelinesConfig config) {
    if (config.getDnaConfig() == null
        || config.getDnaConfig().getSequenceClassifierWsConfig() == null
        || config.getDnaConfig().getSequenceClassifierWsConfig().getWsUrl() == null) {
      throw new IllegalStateException(
          "The DNA taxonomy needs the sequence classifier ws: set"
              + " dnaConfig.sequenceClassifierWsConfig.wsUrl in the pipelines config");
    }
  }

  public static Dataset<Occurrence> runDnaTaxonomy(
      SparkSession spark, FileSystem fs, PipelinesConfig config, String datasetId, int attempt)
      throws Exception {

    validateDnaConfig(config);

    long start = System.currentTimeMillis();

    ThreadContext.put("datasetKey", datasetId);
    log.info("Starting DNA taxonomy interpretation");

    String outputPath = String.format("%s/%s/%d", config.getOutputPath(), datasetId, attempt);

    Dataset<Occurrence> simpleRecords = loadSimpleRecords(spark, outputPath);

    // task retries can count a partition more than once, so they are approximate on failures
    LongAccumulator classified = spark.sparkContext().longAccumulator("dnaTaxonRecords");
    LongAccumulator failed = spark.sparkContext().longAccumulator("dnaTaxonFailedRecords");
    LongAccumulator missing = spark.sparkContext().longAccumulator("dnaTaxonMissingRecords");
    LongAccumulator noClassification =
        spark.sparkContext().longAccumulator("dnaTaxonNoClassificationRecords");

    runDnaTaxonomyTransform(
        config,
        simpleRecords,
        outputPath,
        datasetId,
        classified,
        failed,
        missing,
        noClassification);

    writeMetricsYaml(
        fs,
        Map.of(
            PipelinesVariables.Metrics.DNA_TAXON_RECORDS_COUNT,
            classified.value(),
            PipelinesVariables.Metrics.DNA_TAXON_FAILED_RECORDS_COUNT,
            failed.value(),
            PipelinesVariables.Metrics.DNA_TAXON_MISSING_RECORDS_COUNT,
            missing.value(),
            PipelinesVariables.Metrics.DNA_TAXON_NO_CLASSIFICATION_RECORDS_COUNT,
            noClassification.value()),
        outputPath + "/" + METRICS_FILENAME);
    if (failed.value() > 0) {
      log.warn(
          "The DNA classification failed for {} occurrences, they are kept without it",
          failed.value());
    }
    if (missing.value() > 0) {
      log.warn("The DNA classifier returned no result for {} occurrences", missing.value());
    }

    // replace directories
    fs.delete(new org.apache.hadoop.fs.Path(outputPath + "/" + SIMPLE_OCCURRENCE), true);
    fs.rename(
        new org.apache.hadoop.fs.Path(outputPath + "/" + DNA_TAXON_TEMP),
        new org.apache.hadoop.fs.Path(outputPath + "/" + SIMPLE_OCCURRENCE));

    log.info(
        "Finished DNA taxonomy interpretation in {} secs, classified occurrences: {}",
        (System.currentTimeMillis() - start) / 1000,
        classified.value());

    return spark
        .read()
        .parquet(outputPath + "/" + SIMPLE_OCCURRENCE)
        .as(Encoders.bean(Occurrence.class));
  }

  private static Dataset<Occurrence> loadSimpleRecords(SparkSession spark, String outputPath) {
    // the simple records can be from an older version, e.g. without the DNA taxonomy
    return readParquetAs(spark, outputPath + "/" + SIMPLE_OCCURRENCE, Occurrence.class);
  }

  /**
   * Classifies the occurrences that have DNA sequences, calling the ws in batches and adds the
   * result to the existing {@link Occurrence} record.
   */
  private static void runDnaTaxonomyTransform(
      PipelinesConfig config,
      Dataset<Occurrence> input,
      String outputPath,
      String datasetKey,
      LongAccumulator classified,
      LongAccumulator failed,
      LongAccumulator missing,
      LongAccumulator noClassification) {

    Dataset<Occurrence> output =
        input.mapPartitions(
            (MapPartitionsFunction<Occurrence, Occurrence>)
                occurrenceIterator -> {
                  // created here because they are not serializable
                  SequenceClassifierServiceClient client =
                      SequenceClassifierServiceClient.create(
                          config.getDnaConfig().getSequenceClassifierWsConfig());

                  // one interpreter per checklist to match the DNA-assisted classifications against
                  List<BiConsumer<ClassifyOccurrenceResponse, TaxonRecord>> taxonomyInterpreters =
                      new ArrayList<>();
                  for (String checklistKey : getDnaChecklistKeys(config)) {
                    taxonomyInterpreters.add(
                        org.gbif.pipelines.core.interpreters.core.DnaTaxonomyInterpreter
                            .dnaTaxonomyInterpreter(
                                MultiTaxonomyKVSFactory.getKvStore(config), checklistKey));
                  }

                  int batchSize = config.getDnaConfig().getBatchSizeSequenceClassifier();

                  return Iterators.concat(
                      Iterators.transform(
                          Iterators.partition(occurrenceIterator, batchSize),
                          partition ->
                              interpretOccurrenceDnaTaxon(
                                      partition,
                                      client,
                                      taxonomyInterpreters,
                                      datasetKey,
                                      classified,
                                      failed,
                                      missing,
                                      noClassification)
                                  .iterator()));
                },
            Encoders.bean(Occurrence.class));

    output.write().mode(SaveMode.Overwrite).parquet(outputPath + "/" + DNA_TAXON_TEMP);
  }

  /** The configured DNA checklists, or the default taxonomy if none is configured. */
  static List<String> getDnaChecklistKeys(PipelinesConfig config) {
    List<String> checklistKeys = config.getDnaConfig().getChecklistKeys();
    return checklistKeys == null || checklistKeys.isEmpty()
        ? List.of(OccurrenceJsonConverter.DEFAULT_TAXONOMY_KEY)
        : checklistKeys;
  }

  /**
   * Sets dnaTaxon on the occurrences of the group that have sequences. The dnaTaxon of a previous
   * run is always replaced, so the occurrences without a result in this run end up without it. The
   * DNA classification is an enrichment, like the name matching: if it fails, the error is logged,
   * counted as failed and the occurrences are kept without it. The occurrences the classifier
   * returns no result for are counted as missing, and the ones it returns without a classification
   * (no sequence matched) are counted apart, as they are also counted as classified.
   */
  @SneakyThrows
  static List<Occurrence> interpretOccurrenceDnaTaxon(
      List<Occurrence> occurrencePartition,
      SequenceClassifierServiceClient client,
      List<BiConsumer<ClassifyOccurrenceResponse, TaxonRecord>> taxonomyInterpreters,
      String datasetKey,
      LongAccumulator classified,
      LongAccumulator failed,
      LongAccumulator missing,
      LongAccumulator noClassification) {

    List<ClassifyOccurrenceRequest> requests = new ArrayList<>();
    List<Occurrence> occurrencesRequested = new ArrayList<>();
    for (Occurrence occurrence : occurrencePartition) {
      // a rerun replaces the classification of a previous run, also when it gets none now
      occurrence.setDnaTaxon(null);
      ClassifyOccurrenceRequest request = toClassifyOccurrenceRequest(occurrence);
      if (request != null) { // nothing to classify: don't spend a ws call on it
        requests.add(request);
        occurrencesRequested.add(occurrence);
      }
    }

    if (!requests.isEmpty()) {
      Map<String, DnaTaxonRecord> taxaById;
      Set<String> noClassificationIds = new HashSet<>();
      try {
        taxaById =
            classifyAndInterpret(client, taxonomyInterpreters, requests, noClassificationIds);
      } catch (Exception ex) {
        log.error(
            "DNA classification failed for {} occurrences of dataset {}, kept without it",
            requests.size(),
            datasetKey,
            ex);
        failed.add(requests.size());
        return occurrencePartition;
      }
      long classifiedCount = 0;
      long noClassificationCount = 0;
      for (Occurrence occurrence : occurrencesRequested) {
        DnaTaxonRecord taxon = taxaById.get(occurrence.getId());
        if (taxon != null) {
          occurrence.setDnaTaxon(MAPPER.writeValueAsString(taxon));
          classifiedCount++;
          if (noClassificationIds.contains(occurrence.getId())) {
            noClassificationCount++;
          }
        }
      }
      classified.add(classifiedCount);
      noClassification.add(noClassificationCount);
      missing.add(occurrencesRequested.size() - classifiedCount);
    }
    return occurrencePartition;
  }

  /** Returns null if the occurrence has no valid nucleotide sequences. */
  @SneakyThrows
  static ClassifyOccurrenceRequest toClassifyOccurrenceRequest(Occurrence record) {
    if (record.getDnaDerivedData() == null) {
      return null;
    }
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

    LocationRecord location =
        record.getLocation() != null
            ? MAPPER.readValue(record.getLocation(), LocationRecord.class)
            : null;
    if (location != null) {
      occurrence.setDecimalLatitude(location.getDecimalLatitude());
      occurrence.setDecimalLongitude(location.getDecimalLongitude());
    }

    return occurrence;
  }

  /**
   * Classifies the batch and matches each result against every DNA checklist. The records are keyed
   * by the gbifID echoed by the classifier, the results without it are ignored. The gbifIDs of the
   * results without a classification are added to noClassificationIds.
   */
  private static Map<String, DnaTaxonRecord> classifyAndInterpret(
      SequenceClassifierServiceClient client,
      List<BiConsumer<ClassifyOccurrenceResponse, TaxonRecord>> taxonomyInterpreters,
      List<ClassifyOccurrenceRequest> batch,
      Set<String> noClassificationIds) {

    List<ClassifyOccurrenceResponse> results = client.classifyBatch(batch);

    Map<String, DnaTaxonRecord> records = new HashMap<>(batch.size());
    for (ClassifyOccurrenceResponse result : results) {
      if (result.getGbifID() == null) {
        continue;
      }
      if (result.getClassification() == null) {
        noClassificationIds.add(result.getGbifID());
      }
      long created = System.currentTimeMillis();
      List<TaxonRecord> taxonRecords = new ArrayList<>(taxonomyInterpreters.size());
      for (BiConsumer<ClassifyOccurrenceResponse, TaxonRecord> taxonomyInterpreter :
          taxonomyInterpreters) {
        TaxonRecord tr = TaxonRecord.newBuilder().setCreated(created).build();
        taxonomyInterpreter.accept(result, tr);
        taxonRecords.add(tr);
      }

      // the remarks of the classifier are common to all the checklists
      // the first result wins if the classifier repeats a gbifID
      records.putIfAbsent(
          result.getGbifID(),
          DnaTaxonRecord.newBuilder()
              .setId(result.getGbifID())
              .setCreated(created)
              .setRemarks(
                  result.getClassification() != null
                      ? result.getClassification().getRemarks()
                      : NO_CLASSIFICATION_REMARKS)
              .setTaxonRecords(taxonRecords)
              .build());
    }
    return records;
  }
}
