package au.org.ala.pipelines.beam;

import static au.org.ala.utils.ValidationUtils.*;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.ALAPipelinesConfigFactory;
import au.org.ala.kvs.cache.ALAAttributionKVStoreFactory;
import au.org.ala.kvs.client.ALACollectoryMetadata;
import au.org.ala.pipelines.options.UUIDPipelineOptions;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.ValidationUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang.StringUtils;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.UnknownTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.utils.FsUtils;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.parsers.utils.ModelUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.MDC;

/**
 * Pipeline responsible for validation of unique key data. This pipeline produces a YAML report that
 * is read by {@link ALAUUIDMintingPipeline}. {@link ALAUUIDMintingPipeline} will not run if
 * validation has failed.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ALAUUIDValidationPipeline {

  public static void main(String[] args) throws Exception {
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "uuid");
    UUIDPipelineOptions options =
        PipelinesOptionsFactory.create(UUIDPipelineOptions.class, combinedArgs);
    MDC.put("datasetId", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", "VALIDATE_UUID");
    PipelinesOptionsFactory.registerHdfs(options);
    run(options);
    // FIXME: Issue logged here: https://github.com/AtlasOfLivingAustralia/la-pipelines/issues/105
    System.exit(0);
  }

  public static void run(UUIDPipelineOptions options) throws Exception {

    Pipeline p = Pipeline.create(options);

    // deletePreviousValidation
    deletePreviousValidation(options);

    // validation results
    PCollectionList<String> results = PCollectionList.<String>empty(p);

    ALAPipelinesConfig config =
        ALAPipelinesConfigFactory.getInstance(
                options.getHdfsSiteConfig(), options.getCoreSiteConfig(), options.getProperties())
            .get();

    // create key value store for data resource metadata
    KeyValueStore<String, ALACollectoryMetadata> dataResourceKvStore =
        ALAAttributionKVStoreFactory.create(config);

    // lookup collectory metadata for this data resource
    ALACollectoryMetadata collectoryMetadata = dataResourceKvStore.get(options.getDatasetId());
    if (collectoryMetadata.equals(ALACollectoryMetadata.EMPTY)) {
      log.error("Unable to retrieve dataset metadata for dataset: " + options.getDatasetId());
      PCollection<String> pc =
          p.apply(Create.of(METADATA_AVAILABLE + ": false").withCoder(StringUtf8Coder.of()));
      results = results.and(pc);
    } else {
      PCollection<String> pc =
          p.apply(Create.of(METADATA_AVAILABLE + ": true").withCoder(StringUtf8Coder.of()))
              .setCoder(StringUtf8Coder.of());
      ;
      results = results.and(pc);
    }

    List<String> uniqueTerms = Collections.emptyList();

    // construct unique list of darwin core terms

    if (collectoryMetadata.getConnectionParameters() != null) {
      uniqueTerms = collectoryMetadata.getConnectionParameters().getTermsForUniqueKey();
      if ((uniqueTerms == null || uniqueTerms.isEmpty())) {
        log.error(
            "Unable to proceed, No unique terms specified for dataset: " + options.getDatasetId());
        PCollection<String> pc =
            p.apply(Create.of(UNIQUE_TERMS_SPECIFIED + ": false").withCoder(StringUtf8Coder.of()));
        results = results.and(pc);
      } else {
        PCollection<String> pc =
            p.apply(Create.of(UNIQUE_TERMS_SPECIFIED + ": true").withCoder(StringUtf8Coder.of()));
        results = results.and(pc);
      }
    } else {
      PCollection<String> pc =
          p.apply(Create.of(UNIQUE_TERMS_SPECIFIED + ": false").withCoder(StringUtf8Coder.of()));
      results = results.and(pc);
    }

    // if we have unique terms, check each record is populated
    if (collectoryMetadata.getConnectionParameters() != null
        && uniqueTerms != null
        && !uniqueTerms.isEmpty()) {

      // retrieve the unique term fields
      final List<Term> uniqueDwcTerms = new ArrayList<Term>();
      for (String uniqueTerm : uniqueTerms) {
        Optional<DwcTerm> dwcTerm = getDwcTerm(uniqueTerm);
        if (dwcTerm.isPresent()) {
          uniqueDwcTerms.add(dwcTerm.get());
        } else {
          // create a UnknownTerm for non DWC fields
          uniqueDwcTerms.add(UnknownTerm.build(uniqueTerm.trim()));
        }
      }

      final String datasetID = options.getDatasetId();

      // read the extended records
      PCollection<ExtendedRecord> records =
          p.apply(
              AvroIO.read(ExtendedRecord.class)
                  .from(
                      String.join(
                          "/",
                          options.getTargetPath(),
                          options.getDatasetId().trim(),
                          options.getAttempt().toString(),
                          "interpreted",
                          "verbatim",
                          "*.avro")));

      // check all records have valid keys
      PCollection<String> invalidKeyResults =
          records
              .apply(
                  ParDo.of(
                      new DoFn<ExtendedRecord, Boolean>() {
                        @ProcessElement
                        public void processElement(
                            @Element ExtendedRecord source,
                            OutputReceiver<Boolean> out,
                            ProcessContext c) {
                          out.output(isValidRecord(datasetID, source, uniqueDwcTerms));
                        }
                      }))
              .apply(
                  Filter.by(
                      new SerializableFunction<Boolean, Boolean>() {
                        @Override
                        public Boolean apply(Boolean input) {
                          return !input;
                        }
                      }))
              .apply(Count.globally())
              .apply(
                  MapElements.into(TypeDescriptors.strings())
                      .via(longValue -> EMPTY_KEY_RECORDS + ": " + longValue.toString()));

      // add the invalid key records
      results = results.and(invalidKeyResults.setCoder(StringUtf8Coder.of()));

      // check all records for duplicates
      PCollection<KV<String, Long>> keyCounts =
          records
              .apply(
                  ParDo.of(
                      new DoFn<ExtendedRecord, String>() {
                        @ProcessElement
                        public void processElement(
                            @Element ExtendedRecord source,
                            OutputReceiver<String> out,
                            ProcessContext c) {
                          out.output(
                              ValidationUtils.generateUniqueKeyForValidation(
                                  datasetID, source, uniqueDwcTerms));
                        }
                      }))
              .apply(Count.<String>perElement());

      // filter keys that are used more than once
      PCollection<KV<String, Long>> duplicateKeyCounts =
          keyCounts.apply(
              Filter.by(
                  new SerializableFunction<KV<String, Long>, Boolean>() {
                    @Override
                    public Boolean apply(KV<String, Long> input) {
                      return input.getValue() > 1;
                    }
                  }));

      // retrieve a count of records with duplicate keys problems
      PCollection<String> duplicateKeyCount =
          duplicateKeyCounts
              .apply(
                  ParDo.of(
                      new DoFn<KV<String, Long>, Long>() {
                        @ProcessElement
                        public void processElement(
                            @Element KV<String, Long> kv,
                            OutputReceiver<Long> out,
                            ProcessContext c) {
                          out.output(kv.getValue());
                        }
                      }))
              .apply(Sum.longsGlobally())
              .apply(
                  MapElements.into(TypeDescriptors.strings())
                      .via(longValue -> DUPLICATE_RECORD_KEY_COUNT + ": " + longValue.toString()));
      results = results.and(duplicateKeyCount.setCoder(StringUtf8Coder.of()));

      // retrieve a count of duplicate keys
      PCollection<String> duplicateKeyResults =
          duplicateKeyCounts
              .apply(Count.globally())
              .apply(
                  MapElements.into(TypeDescriptors.strings())
                      .via(longValue -> DUPLICATE_KEY_COUNT + ": " + longValue.toString()));

      // add the duplicate key records
      results = results.and(duplicateKeyResults.setCoder(StringUtf8Coder.of()));

      // dump out duplicate keys to CSV
      duplicateKeyCounts
          .apply(
              MapElements.into(TypeDescriptors.strings())
                  .via(kv -> kv.getKey() + "," + kv.getValue()))
          .apply(
              TextIO.write()
                  .to(
                      String.join(
                          "/",
                          getValidationFilePath(options, VALIDATION_OUTPUT_DIR),
                          DUPLICATE_KEYS_OUTPUT))
                  .withoutSharding());
    }

    // write out all results to YAML file
    results
        .apply(Flatten.<String>pCollections())
        .setCoder(StringUtf8Coder.of())
        .apply(
            TextIO.write()
                .to(getValidationFilePath(options, VALIDATION_REPORT_FILE))
                .withoutSharding());

    PipelineResult result = p.run();
    result.waitUntilFinish();
    log.info(
        "Validation finished. Results written to: {}",
        getValidationFilePath(options, VALIDATION_REPORT_FILE));
  }

  @NotNull
  private static String getValidationFilePath(
      UUIDPipelineOptions options, String validationReportFile) {
    return String.join(
        "/",
        options.getTargetPath(),
        options.getDatasetId().trim(),
        options.getAttempt().toString(),
        validationReportFile);
  }

  public static void deletePreviousValidation(UUIDPipelineOptions options) {
    // delete output directory
    String dirPath = getValidationFilePath(options, VALIDATION_OUTPUT_DIR);
    FsUtils.deleteIfExist(options.getHdfsSiteConfig(), options.getCoreSiteConfig(), dirPath);

    // delete report
    String filePath = getValidationFilePath(options, VALIDATION_REPORT_FILE);
    FsUtils.deleteIfExist(options.getHdfsSiteConfig(), options.getCoreSiteConfig(), filePath);
  }

  /**
   * Generate a unique key based on the darwin core fields. This works the same was unique keys
   * where generated in the biocache-store. This is repeated to maintain backwards compatibility
   * with existing data holdings.
   *
   * @param source
   * @param uniqueTerms
   * @return
   * @throws RuntimeException
   */
  public static Boolean isValidRecord(
      String datasetID, ExtendedRecord source, List<Term> uniqueTerms) throws RuntimeException {

    List<String> uniqueValues = new ArrayList<String>();
    boolean allUniqueValuesAreEmpty = true;
    for (Term term : uniqueTerms) {
      String value = ModelUtils.extractNullAwareValue(source, term);
      if (value != null && StringUtils.trimToNull(value) != null) {
        // we have a term with a value
        allUniqueValuesAreEmpty = false;
        uniqueValues.add(value.trim());
      }
    }

    return !allUniqueValuesAreEmpty;
  }

  /**
   * Match the darwin core term which has been supplied in simple camel case format e.g.
   * catalogNumber.
   *
   * @param name
   * @return
   */
  static Optional<DwcTerm> getDwcTerm(String name) {
    try {
      return Optional.of(DwcTerm.valueOf(name));
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }
  }
}
