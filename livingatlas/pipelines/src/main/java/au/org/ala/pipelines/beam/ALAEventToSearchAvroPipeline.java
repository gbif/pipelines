package au.org.ala.pipelines.beam;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.ALL_AVRO;

import au.org.ala.pipelines.transforms.ALAEventToSearchTransform;
import au.org.ala.pipelines.transforms.ALAMetadataTransform;
import au.org.ala.pipelines.transforms.ALATaxonomyTransform;
import au.org.ala.utils.CombinedYamlConfiguration;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.transforms.core.*;
import org.gbif.pipelines.transforms.extension.MeasurementOrFactTransform;
import org.slf4j.MDC;

/**
 * Pipeline that generates an AVRO index that will support a Spark SQL query interface for
 * downloads.
 *
 * <p>This pipelines creates: 1) An Event an AVRO export for querying 2) An Interpreted Occurrence
 * export joining 3) A verbatim Occurrence export
 */
@Slf4j
public class ALAEventToSearchAvroPipeline {

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();

  private static final TupleTag<Iterable<String>> TAXON_IDS_TAG = new TupleTag<>();

  public static void main(String[] args) throws IOException {
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "index");
    InterpretationPipelineOptions options = PipelinesOptionsFactory.createIndexing(combinedArgs);
    run(options);
  }

  public static void run(InterpretationPipelineOptions options) {
    run(options, Pipeline::create);
  }

  public static void run(
      InterpretationPipelineOptions options,
      Function<InterpretationPipelineOptions, Pipeline> pipelinesFn) {

    MDC.put("datasetKey", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());

    log.info("Adding step 1: Options");
    UnaryOperator<String> eventPathFn =
        t -> PathBuilder.buildPathInterpretUsingTargetPath(options, DwcTerm.Event, t, ALL_AVRO);

    UnaryOperator<String> occurrencesPathFn =
        t ->
            PathBuilder.buildPathInterpretUsingTargetPath(options, DwcTerm.Occurrence, t, ALL_AVRO);

    options.setAppName("Event indexing of " + options.getDatasetId());
    Pipeline p = pipelinesFn.apply(options);

    log.info("Adding step 2: Creating transformations");
    ALAMetadataTransform alaMetadataTransform = ALAMetadataTransform.builder().create();
    // Core
    EventCoreTransform eventCoreTransform = EventCoreTransform.builder().create();
    TemporalTransform temporalTransform = TemporalTransform.builder().create();
    LocationTransform locationTransform = LocationTransform.builder().create();

    // Taxonomy transform for loading occurrence taxonomy info
    ALATaxonomyTransform alaTaxonomyTransform = ALATaxonomyTransform.builder().create();

    MeasurementOrFactTransform measurementOrFactTransform =
        MeasurementOrFactTransform.builder().create();

    log.info("Adding step 3: Creating beam pipeline");
    PCollectionView<ALAMetadataRecord> metadataView =
        p.apply("Read Metadata", alaMetadataTransform.read(eventPathFn))
            .apply("Convert to view", View.asSingleton());

    PCollection<KV<String, EventCoreRecord>> eventCoreCollection =
        p.apply("Read Event core", eventCoreTransform.read(eventPathFn))
            .apply("Map Event core to KV", eventCoreTransform.toKv());

    PCollection<KV<String, TemporalRecord>> temporalCollection =
        p.apply("Read Temporal", temporalTransform.read(eventPathFn))
            .apply("Map Temporal to KV", temporalTransform.toKv());

    PCollection<KV<String, LocationRecord>> locationCollection =
        p.apply("Read Location", locationTransform.read(eventPathFn))
            .apply("Map Location to KV", locationTransform.toKv());

    PCollection<KV<String, MeasurementOrFactRecord>> measurementOrFactCollection =
        p.apply("Read Measurement or fact", measurementOrFactTransform.read(eventPathFn))
            .apply("Map Measurement or fact to KV", measurementOrFactTransform.toKv());

    // load the taxonomy from the occurrence records
    PCollection<KV<String, ALATaxonRecord>> taxonCollection =
        p.apply("Read taxa", alaTaxonomyTransform.read(occurrencesPathFn))
            .apply("Map taxa to KV", alaTaxonomyTransform.toKv());

    PCollection<KV<String, Iterable<String>>> taxonIDCollection = keyByCoreID(taxonCollection);

    log.info("Adding step 3: Converting into a json object");
    ParDo.SingleOutput<KV<String, CoGbkResult>, EventSearchRecord> eventSearchAvroFn =
        ALAEventToSearchTransform.builder()
            .eventCoreRecordTag(eventCoreTransform.getTag())
            .temporalRecordTag(temporalTransform.getTag())
            .locationRecordTag(locationTransform.getTag())
            .measurementOrFactRecordTag(measurementOrFactTransform.getTag())
            .taxonIDsTag(TAXON_IDS_TAG)
            .metadataView(metadataView)
            .build()
            .converter();

    PCollection<EventSearchRecord> eventSearchCollection =
        KeyedPCollectionTuple
            // Core
            .of(eventCoreTransform.getTag(), eventCoreCollection)
            .and(temporalTransform.getTag(), temporalCollection)
            .and(locationTransform.getTag(), locationCollection)
            .and(measurementOrFactTransform.getTag(), measurementOrFactCollection)
            .and(TAXON_IDS_TAG, taxonIDCollection)
            // Apply
            .apply("Grouping objects", CoGroupByKey.create())
            .apply("Merging to json", eventSearchAvroFn);

    String avroPath =
        String.join(
            "/",
            options.getInputPath(),
            options.getDatasetId(),
            options.getAttempt().toString(),
            "search",
            DwcTerm.Event.simpleName().toLowerCase(),
            "search");

    eventSearchCollection.apply(
        AvroIO.write(EventSearchRecord.class)
            .to(avroPath)
            .withSuffix(".avro")
            .withCodec(BASE_CODEC));

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();
    log.info("Save metrics into the file and set files owner");
    MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());

    log.info("Pipeline has been finished");
  }

  private static PCollection<KV<String, Iterable<String>>> keyByCoreID(
      PCollection<KV<String, ALATaxonRecord>> taxonCollection) {
    return taxonCollection
        .apply(
            ParDo.of(
                new DoFn<KV<String, ALATaxonRecord>, KV<String, Iterable<String>>>() {
                  @ProcessElement
                  public void processElement(
                      @Element KV<String, ALATaxonRecord> source,
                      OutputReceiver<KV<String, Iterable<String>>> out,
                      ProcessContext c) {
                    ALATaxonRecord taxon = source.getValue();
                    List<String> taxonID = new ArrayList<>();
                    taxonID.add(taxon.getKingdomID());
                    taxonID.add(taxon.getPhylumID());
                    taxonID.add(taxon.getOrderID());
                    taxonID.add(taxon.getClassID());
                    taxonID.add(taxon.getFamilyID());
                    taxonID.add(taxon.getGenusID());
                    taxonID.add(taxon.getSpeciesID());
                    taxonID.add(taxon.getTaxonConceptID());
                    taxonID.stream().filter(x -> x != null);
                    out.output(
                        KV.of(
                            taxon.getCoreId(),
                            taxonID.stream().filter(x -> x != null).collect(Collectors.toList())));
                  }
                })
            // group by eventID, distinct
            )
        .apply(GroupByKey.create())
        .apply(
            ParDo.of(
                new DoFn<KV<String, Iterable<Iterable<String>>>, KV<String, Iterable<String>>>() {
                  @ProcessElement
                  public void processElement(
                      @Element KV<String, Iterable<Iterable<String>>> in,
                      OutputReceiver<KV<String, Iterable<String>>> out,
                      ProcessContext c) {
                    Iterable<Iterable<String>> taxonIDs = in.getValue();
                    Set<String> taxonIDSet = new HashSet<String>();
                    taxonIDs.forEach(list -> list.forEach(taxonIDSet::add));
                    out.output(
                        KV.of(in.getKey(), taxonIDSet.stream().collect(Collectors.toList())));
                  }
                }));
  }
}
