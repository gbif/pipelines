package au.org.ala.pipelines.beam;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;

import au.org.ala.pipelines.options.SpeciesLevelPipelineOptions;
import au.org.ala.pipelines.transforms.ALATaxonomyTransform;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.specieslists.SpeciesListDownloader;
import au.org.ala.utils.CombinedYamlConfiguration;
import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.io.avro.*;

@Slf4j
public class SpeciesListPipeline {

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
            "taxonprofiles",
            "taxon-profile-record");

    // write out the result to file
    taxonProfilesCollection
        .apply(Values.<TaxonProfile>create())
        .apply(
            AvroIO.write(TaxonProfile.class)
                .to(avroPath)
                .withSuffix(".avro")
                .withCodec(BASE_CODEC));

    // run pipeline
    p.run().waitUntilFinish();

    log.info("Completed species list pipeline for dataset {}", options.getDatasetId());
  }

  /**
   * Generate a PCollection of taxon profiles.
   *
   * @param p
   * @param options
   * @return
   */
  public static PCollection<KV<String, TaxonProfile>> generateTaxonProfileCollection(
      Pipeline p, SpeciesLevelPipelineOptions options) throws Exception {

    SpeciesListDownloader.run(options);

    log.info("Running species list pipeline for dataset {}", options.getDatasetId());
    UnaryOperator<String> pathFn =
        t -> PathBuilder.buildPathInterpretUsingTargetPath(options, t, "*" + AVRO_EXTENSION);

    // now lets start the pipelines
    PCollection<SpeciesListRecord> speciesLists =
        p.apply(
            AvroIO.read(SpeciesListRecord.class)
                .from(options.getSpeciesAggregatesPath() + "/species-lists/species-lists.avro"));

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
            .apply(GroupByKey.<String, SpeciesListRecord>create());

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

    final TupleTag<String> t1 = new TupleTag<>();
    final TupleTag<Iterable<SpeciesListRecord>> t2 = new TupleTag<>();

    PCollection<KV<String, CoGbkResult>> result =
        KeyedPCollectionTuple.of(t1, alaTaxonID)
            .and(t2, taxonID2Lists)
            .apply(CoGroupByKey.create());

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
                  Iterator<SpeciesListRecord> iter = speciesLists.iterator();

                  List<String> speciesListIDs = new ArrayList<String>();
                  List<ConservationStatus> conservationStatusList =
                      new ArrayList<ConservationStatus>();
                  List<InvasiveStatus> invasiveStatusList = new ArrayList<InvasiveStatus>();

                  while (iter.hasNext()) {

                    SpeciesListRecord speciesListRecord = iter.next();
                    speciesListIDs.add(speciesListRecord.getSpeciesListID());

                    if (speciesListRecord.getIsThreatened()
                        && (!Strings.isNullOrEmpty(speciesListRecord.getSourceStatus())
                            || !Strings.isNullOrEmpty(speciesListRecord.getStatus()))) {
                      conservationStatusList.add(
                          ConservationStatus.newBuilder()
                              .setSpeciesListID(speciesListRecord.getSpeciesListID())
                              .setRegion(speciesListRecord.getRegion())
                              .setSourceStatus(speciesListRecord.getSourceStatus())
                              .setStatus(speciesListRecord.getStatus())
                              .build());
                    } else if (speciesListRecord.getIsInvasive()) {
                      invasiveStatusList.add(
                          InvasiveStatus.newBuilder()
                              .setSpeciesListID(speciesListRecord.getSpeciesListID())
                              .setRegion(speciesListRecord.getRegion())
                              .build());
                    }
                  }

                  // output a link to each occurrence record we've matched by taxonID
                  for (String occurrenceID : occurrenceIDs) {
                    TaxonProfile.Builder builder = TaxonProfile.newBuilder();
                    builder.setId(occurrenceID);
                    builder.setSpeciesListID(speciesListIDs);
                    builder.setConservationStatuses(conservationStatusList);
                    builder.setInvasiveStatuses(invasiveStatusList);
                    c.output(KV.of(occurrenceID, builder.build()));
                  }
                }
              }
            }));
  }
}
