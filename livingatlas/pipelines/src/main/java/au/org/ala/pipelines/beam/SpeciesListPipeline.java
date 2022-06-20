package au.org.ala.pipelines.beam;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.ALL_AVRO;

import au.org.ala.pipelines.options.SpeciesLevelPipelineOptions;
import au.org.ala.pipelines.transforms.ALATaxonomyTransform;
import au.org.ala.pipelines.util.SpeciesListUtils;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.specieslists.SpeciesListDownloader;
import au.org.ala.utils.CombinedYamlConfiguration;
import java.io.IOException;
import java.util.function.UnaryOperator;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.io.avro.*;

/**
 * Beam based species list pipeline which will download the species list information and create a
 * TaxonProfile extension for a dataset.
 *
 * <p>This extension contains:
 *
 * <ul>
 *   <li>Links to species lists for records
 *   <li>stateProvince and country associated conservation status for the record
 *   <li>stateProvince and country associated invasive status for the record
 * </ul>
 *
 * This pipeline is left for debug purposes only. Species lists are joined to the records in the
 * {@link IndexRecordPipeline} so there is no need to run this pipeline separately.
 *
 * @see TaxonProfile
 * @see SpeciesListDownloader
 */
@Slf4j
public class SpeciesListPipeline {

  private static final DwcTerm CORE_TERM = DwcTerm.Occurrence;

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();

  public static void main(String[] args) throws Exception {
    VersionInfo.print();
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "speciesLists");
    SpeciesLevelPipelineOptions options =
        PipelinesOptionsFactory.create(SpeciesLevelPipelineOptions.class, combinedArgs);
    PipelinesOptionsFactory.registerHdfs(options);
    run(options);
  }

  public static void run(SpeciesLevelPipelineOptions options) throws Exception {

    log.info("Creating a pipeline from options");
    Pipeline p = Pipeline.create(options);
    PCollection<KV<String, TaxonProfile>> taxonProfilesCollection =
        generateTaxonProfileCollection(p, options);

    // construct output path
    String avroPath =
        String.join(
            "/",
            options.getInputPath(),
            options.getDatasetId(),
            options.getAttempt().toString(),
            "taxon_profiles",
            "taxon-profile-record");

    // write out the result to file
    taxonProfilesCollection
        .apply(Values.create())
        .apply(
            AvroIO.write(TaxonProfile.class)
                .to(avroPath)
                .withSuffix(".avro")
                .withCodec(BASE_CODEC));

    // run pipeline
    p.run().waitUntilFinish();

    log.info("Completed species list pipeline for dataset {}", options.getDatasetId());
  }

  /** Generate a PCollection of taxon profiles. */
  public static PCollection<KV<String, TaxonProfile>> generateTaxonProfileCollection(
      Pipeline p, SpeciesLevelPipelineOptions options) throws IOException {

    SpeciesListDownloader.run(options);

    log.info("Running species list pipeline for dataset {}", options.getDatasetId());
    UnaryOperator<String> pathFn =
        t -> PathBuilder.buildPathInterpretUsingTargetPath(options, CORE_TERM, t, ALL_AVRO);

    // now lets start the pipelines
    PCollection<SpeciesListRecord> speciesLists =
        p.apply(
            AvroIO.read(SpeciesListRecord.class)
                .from(options.getSpeciesAggregatesPath() + options.getSpeciesListCachePath()));

    // transform to taxonID -> List<SpeciesListRecord>
    PCollection<KV<String, Iterable<SpeciesListRecord>>> taxonID2Lists =
        speciesLists
            .apply(Filter.by(record -> record.getTaxonID() != null))
            .apply(
                MapElements.via(
                    new SimpleFunction<SpeciesListRecord, KV<String, SpeciesListRecord>>() {
                      @Override
                      public KV<String, SpeciesListRecord> apply(SpeciesListRecord record) {
                        return KV.of(record.getTaxonID(), record);
                      }
                    }))
            .apply(GroupByKey.create());

    // read taxonomy extension,
    ALATaxonomyTransform alaTaxonomyTransform = ALATaxonomyTransform.builder().create();

    // generate a taxonID -> occurrenceID PCollection
    PCollection<KV<String, String>> alaTaxonID =
        p.apply("Read Taxon", alaTaxonomyTransform.read(pathFn))
            .apply("Map Taxon to KV", alaTaxonomyTransform.toKv())
            .apply(Filter.by(record -> record.getValue().getTaxonConceptID() != null))
            .apply(
                MapElements.via(
                    new SimpleFunction<KV<String, ALATaxonRecord>, KV<String, String>>() {
                      @Override
                      public KV<String, String> apply(KV<String, ALATaxonRecord> record) {
                        return KV.of(record.getValue().getTaxonConceptID(), record.getKey());
                      }
                    }));

    final TupleTag<String> t1 = new TupleTag<String>() {};
    final TupleTag<Iterable<SpeciesListRecord>> t2 = new TupleTag<Iterable<SpeciesListRecord>>() {};

    PCollection<KV<String, CoGbkResult>> result =
        KeyedPCollectionTuple.of(t1, alaTaxonID)
            .and(t2, taxonID2Lists)
            .apply(CoGroupByKey.create());

    final boolean includeConservationStatus = options.getIncludeConservationStatus();
    final boolean includeInvasiveStatus = options.getIncludeInvasiveStatus();

    // join collections
    return result.apply(
        ParDo.of(
            new DoFn<KV<String, CoGbkResult>, KV<String, TaxonProfile>>() {
              @ProcessElement
              public void processElement(ProcessContext c) {

                KV<String, CoGbkResult> e = c.element();
                CoGbkResult result = e.getValue();

                // Retrieve all integers associated with this key from pt1
                Iterable<String> occurrenceIDs = result.getAll(t1);
                Iterable<SpeciesListRecord> speciesLists = result.getOnly(t2, null);

                if (speciesLists != null) {
                  TaxonProfile.Builder builder =
                      SpeciesListUtils.createTaxonProfileBuilder(
                          speciesLists, includeConservationStatus, includeInvasiveStatus);
                  // output a link to each occurrence record we've matched by taxonID
                  for (String occurrenceID : occurrenceIDs) {
                    builder.setId(occurrenceID);
                    c.output(KV.of(occurrenceID, builder.build()));
                  }
                }
              }
            }));
  }
}
