package org.gbif.pipelines.demo;

import org.gbif.dwca.avro.Event;
import org.gbif.dwca.avro.ExtendedOccurence;
import org.gbif.dwca.avro.Location;

import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.common.beam.DwCAIO;

import org.gbif.pipelines.core.functions.interpretation.ExtendedRecordToEventTransformer;
import org.gbif.pipelines.core.functions.interpretation.ExtendedRecordToLocationTransformer;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;

import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.pipelines.demo.DwCA2AvroPipeline2.RawToInterpretedCategoryTransformer.spatialView;
import static org.gbif.pipelines.demo.DwCA2AvroPipeline2.RawToInterpretedCategoryTransformer.temporalView;

/**
 * A simple demonstration showing a pipeline running locally which will read UntypedOccurrence from a DwC-A file and
 * save the result as an Avro file.
 */
public class DwCA2AvroPipeline2 {
  private static final Logger LOG = LoggerFactory.getLogger(DwCA2AvroPipeline2.class);

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(DirectRunner.class); // forced for this demo
    Pipeline p = Pipeline.create(options);

    // register Avro coders for serializing our messages
    Coders.registerAvroCoders(p, ExtendedRecord.class, Event.class, Location.class,ExtendedOccurence.class);

    // Read the DwC-A using our custom reader
    PCollection<ExtendedRecord> rawRecords = p.apply(
      "Read from Darwin Core Archive", DwCAIO.Read.withPaths("demo/dwca.zip", "demo/target/tmp"));
    rawRecords.apply(
      "Save the interpreted records as Avro", AvroIO.write(ExtendedRecord.class).to("demo/output/raw-data"));

    PCollectionTuple interpretedCategory = rawRecords.apply(new RawToInterpretedCategoryTransformer());
    interpretedCategory.get(temporalView);
    interpretedCategory.get(RawToInterpretedCategoryTransformer.spatialView);

    final TupleTag<Event> temporalTag = new TupleTag<>();
    final TupleTag<Location> spatialTag = new TupleTag<>();

    PCollection<KV<String, CoGbkResult>> joinedCollection =
      KeyedPCollectionTuple.of(temporalTag, interpretedCategory.get(temporalView))
        .and(spatialTag, interpretedCategory.get(spatialView))
        .apply(CoGroupByKey.<String>create());

    PCollection<ExtendedOccurence> interpretedRecords =
      joinedCollection.apply(ParDo.of(new CoGbkResultToFlattenedInterpretedRecord()));


    // Write the result as an Avro file
    interpretedRecords.apply(
      "Save the interpreted records as Avro", AvroIO.write(ExtendedOccurence.class).to("demo/output/interpreted-data"));

    LOG.info("Starting the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();
    LOG.info("Pipeline finished with state: {} ", result.getState());
  }

  static class RawToInterpretedCategoryTransformer extends PTransform<PCollection<ExtendedRecord>,PCollectionTuple> {

    static TupleTag<KV<String,Event>> temporalView = new TupleTag<>();
    static TupleTag<KV<String,Location>> spatialView = new TupleTag<>();


    /**
     * Override this method to specify how this {@code PTransform} should be expanded
     * on the given {@code InputT}.
     * <p>
     * <p>NOTE: This method should not be called directly. Instead apply the
     * {@code PTransform} should be applied to the {@code InputT} using the {@code apply}
     * method.
     * <p>
     * <p>Composite transforms, which are defined in terms of other transforms,
     * should return the output of one of the composed transforms.  Non-composite
     * transforms, which do not apply any transforms internally, should return
     * a new unbound output and register evaluators (via backend-specific
     * registration methods).
     */
    @Override
    public PCollectionTuple expand(PCollection<ExtendedRecord> input) {
      PCollection<KV<String,Event>> eventView = input.apply(ParDo.of(new ExtendedRecordToEventTransformer()));
      PCollection<KV<String,Location>> locationView = input.apply(ParDo.of(new ExtendedRecordToLocationTransformer()));
      final PCollectionTuple and = PCollectionTuple.of(temporalView, eventView).and(spatialView, locationView);
      return and;
    }
  }

  static class CoGbkResultToFlattenedInterpretedRecord extends DoFn<KV<String,CoGbkResult>,ExtendedOccurence>{
    @ProcessElement
    public void processElement(ProcessContext ctx){
      KV<String,CoGbkResult> result = ctx.element();

      Event evt = result.getValue().getOnly(new TupleTag<Event>());
      Location loc = result.getValue().getOnly(new TupleTag<Location>());

      ExtendedOccurence occurence = new ExtendedOccurence();
      occurence.setOccurrenceID(result.getKey());
      occurence.setBasisOfRecord(evt.getBasisOfRecord());
      occurence.setDay(evt.getDay());
      occurence.setMonth(evt.getMonth());
      occurence.setYear(evt.getYear());
      occurence.setEventDate(evt.getEventDate());
      occurence.setDecimalLatitude(loc.getDecimalLatitude());
      occurence.setDecimalLongitude(loc.getDecimalLongitude());
      occurence.setCountry(loc.getCountry());
      occurence.setCountryCode(loc.getCountryCode());
      occurence.setContinent(loc.getContinent());

      ctx.output(occurence);
    }
  }


}
