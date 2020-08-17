package au.org.ala.pipelines.beam;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.ALAPipelinesConfigFactory;
import au.org.ala.kvs.cache.ALAAttributionKVStoreFactory;
import au.org.ala.kvs.client.ALACollectoryMetadata;
import au.org.ala.pipelines.options.UUIDPipelineOptions;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.ValidationUtils;
import java.util.ArrayList;
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
import org.gbif.pipelines.io.avro.ALAUUIDRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.parsers.utils.ModelUtils;
import org.slf4j.MDC;

/**
 * Pipeline responsible for minting UUIDs on new records and rematching existing UUIDs to records
 * that have been previously loaded.
 *
 * <p>This works by:
 *
 * <p>1. Creating a map from ExtendedRecord of UniqueKey -> ExtendedRecord.getId(). The UniqueKey is
 * constructed using the unique terms specified in the collectory.
 *
 * <p>2. Creating a map from source of UniqueKey -> UUID from previous ingestions (this will be
 * blank for newly imported datasets)
 *
 * <p>3. Joining the two maps by the UniqueKey.
 *
 * <p>4. Writing out ALAUUIDRecords containing ExtendedRecord.getId() -> (UUID, UniqueKey). These
 * records are then used in SOLR index generation.
 *
 * <p>5. Backing up previous AVRO ALAUUIDRecords.
 *
 * <p>The end result is a AVRO export ALAUUIDRecords which are then used as a mandatory extension in
 * the generation of the SOLR index.
 *
 * @see ALAUUIDRecord
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

    // delete metrics if it exists
    Pipeline p = Pipeline.create(options);

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
          p.apply(Create.of("metadataAvailable: false").withCoder(StringUtf8Coder.of()));
      results = results.and(pc);
    } else {
      PCollection<String> pc =
          p.apply(Create.of("metadataAvailable: true").withCoder(StringUtf8Coder.of()))
              .setCoder(StringUtf8Coder.of());
      ;
      results = results.and(pc);
    }

    // construct unique list of darwin core terms
    List<String> uniqueTerms = collectoryMetadata.getConnectionParameters().getTermsForUniqueKey();
    if ((uniqueTerms == null || uniqueTerms.isEmpty())) {
      log.error(
          "Unable to proceed, No unique terms specified for dataset: " + options.getDatasetId());
      PCollection<String> pc =
          p.apply(Create.of("uniqueTermsSpecified: false").withCoder(StringUtf8Coder.of()));
      results = results.and(pc);
    } else {
      PCollection<String> pc =
          p.apply(Create.of("uniqueTermsSpecified: true").withCoder(StringUtf8Coder.of()));
      results = results.and(pc);
    }

    // if we have unique terms, check each record is populated
    if ((uniqueTerms != null && !uniqueTerms.isEmpty())) {

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
                  "FormatResults",
                  MapElements.into(TypeDescriptors.strings())
                      .via(longValue -> "invalidRecords: " + longValue.toString()));

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

      PCollection<String> duplicateKeyResults =
          keyCounts
              .apply(
                  Filter.by(
                      new SerializableFunction<KV<String, Long>, Boolean>() {
                        @Override
                        public Boolean apply(KV<String, Long> input) {
                          return input.getValue() > 1;
                        }
                      }))
              .apply(Count.globally())
              .apply(
                  "FormatResults",
                  MapElements.into(TypeDescriptors.strings())
                      .via(longValue -> "duplicateRecords: " + longValue.toString()));

      // add the duplicate key records
      results = results.and(duplicateKeyResults.setCoder(StringUtf8Coder.of()));
    }

    // write out all results to YAML file
    results
        .apply(Flatten.<String>pCollections())
        .setCoder(StringUtf8Coder.of())
        .apply(
            TextIO.write()
                .to(
                    String.join(
                        "/",
                        options.getTargetPath(),
                        options.getDatasetId().trim(),
                        options.getAttempt().toString(),
                        "validate-report"))
                .withSuffix(".yaml")
                .withoutSharding());

    PipelineResult result = p.run();
    result.waitUntilFinish();
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
