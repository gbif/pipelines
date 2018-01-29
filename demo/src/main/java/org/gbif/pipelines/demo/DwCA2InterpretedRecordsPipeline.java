package org.gbif.pipelines.demo;

import org.gbif.dwca.avro.Event;
import org.gbif.dwca.avro.ExtendedOccurence;
import org.gbif.dwca.avro.Location;
import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.common.beam.DwCAIO;
import org.gbif.pipelines.core.functions.interpretation.error.Issue;
import org.gbif.pipelines.core.functions.interpretation.error.IssueLineageRecord;
import org.gbif.pipelines.core.functions.interpretation.error.Lineage;
import org.gbif.pipelines.core.functions.transforms.RawToInterpretedCategoryTransformer;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
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

import static org.gbif.pipelines.core.functions.transforms.RawToInterpretedCategoryTransformer.spatialCategory;
import static org.gbif.pipelines.core.functions.transforms.RawToInterpretedCategoryTransformer.spatialCategoryIssues;
import static org.gbif.pipelines.core.functions.transforms.RawToInterpretedCategoryTransformer.temporalCategory;
import static org.gbif.pipelines.core.functions.transforms.RawToInterpretedCategoryTransformer.temporalCategoryIssues;

/**
 * A simple demonstration showing a pipeline running locally which will read UntypedOccurrence from a DwC-A file
 * transform it into interpreted occurence records
 * .
 */
public class DwCA2InterpretedRecordsPipeline {

  private static final TupleTag<Event> temporalTag = new TupleTag<Event>() {};
  private static final TupleTag<Location> spatialTag = new TupleTag<Location>() {};
  private static final TupleTag<IssueLineageRecord> temporalIssueTag = new TupleTag<IssueLineageRecord>() {};
  private static final TupleTag<IssueLineageRecord> spatialIssueTag = new TupleTag<IssueLineageRecord>() {};
  private static final Logger LOG = LoggerFactory.getLogger(DwCA2InterpretedRecordsPipeline.class);

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(DirectRunner.class); // forced for this demo
    Pipeline p = Pipeline.create(options);

    // register Avro coders for serializing our messages
    Coders.registerAvroCoders(p,
                              ExtendedRecord.class,
                              Event.class,
                              Location.class,
                              ExtendedOccurence.class,
                              Issue.class,
                              Lineage.class,
                              IssueLineageRecord.class);
    p.getCoderRegistry()
      .registerCoderForType(temporalTag.getTypeDescriptor(), AvroCoder.of(temporalTag.getTypeDescriptor()));
    p.getCoderRegistry()
      .registerCoderForType(spatialTag.getTypeDescriptor(), AvroCoder.of(spatialTag.getTypeDescriptor()));
    p.getCoderRegistry()
      .registerCoderForType(temporalIssueTag.getTypeDescriptor(), AvroCoder.of(temporalIssueTag.getTypeDescriptor()));
    p.getCoderRegistry()
      .registerCoderForType(spatialIssueTag.getTypeDescriptor(), AvroCoder.of(spatialIssueTag.getTypeDescriptor()));

    p.getCoderRegistry()
      .registerCoderForType(temporalCategory.getTypeDescriptor(),
                            KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(Event.class)));
    p.getCoderRegistry()
      .registerCoderForType(spatialCategory.getTypeDescriptor(),
                            KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(Location.class)));
    p.getCoderRegistry()
      .registerCoderForType(spatialCategoryIssues.getTypeDescriptor(),
                            KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(IssueLineageRecord.class)));
    p.getCoderRegistry()
      .registerCoderForType(temporalCategoryIssues.getTypeDescriptor(),
                            KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(IssueLineageRecord.class)));

    // Read the DwC-A using our custom reader
    PCollection<ExtendedRecord> rawRecords =
      p.apply("Read from Darwin Core Archive", DwCAIO.Read.withPaths("demo/dwca.zip", "demo/target/tmp"));

    // Write records in an avro file, this will be location of the hive table which has raw records
    rawRecords.apply("Save the interpreted records as Avro",
                     AvroIO.write(ExtendedRecord.class).to("demo/output/raw-data"));

    //Interpret the raw records as a tuple, which has both different categories of data and issue related to them
    PCollectionTuple interpretedCategory = rawRecords.apply(new RawToInterpretedCategoryTransformer());

    //Dumping the temporal category of interpreted records in an defined hive table location.
    interpretedCategory.get(temporalCategory)
      .apply(ParDo.of(new DoFn<KV<String, Event>, Event>() {
        @ProcessElement
        public void processElement(ProcessContext ctx) {
          ctx.output(ctx.element().getValue());
        }
      }))
      .apply("Dumping the temporal category of interpreted records in an defined hive table location.",
             AvroIO.write(Event.class).to("demo/output/interpreted-temporaldata"));

    //Dumping the spatial category of interpreted records in a defined hive table location.
    interpretedCategory.get(spatialCategory)
      .apply(ParDo.of(new DoFn<KV<String, Location>, Location>() {
        @ProcessElement
        public void processElement(ProcessContext ctx) {
          ctx.output(ctx.element().getValue());
        }
      }))
      .apply("Dumping the spatial category of interpreted records in a defined hive table location.",
             AvroIO.write(Location.class).to("demo/output/interpreted-spatialdata"));

    //Dumping the temporal category of issues and lineages while interpreting the records in a defined hive table location.
    interpretedCategory.get(temporalCategoryIssues)
      .setCoder(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(IssueLineageRecord.class)))
      .apply(ParDo.of(new DoFn<KV<String, IssueLineageRecord>, IssueLineageRecord>() {
        @ProcessElement
        public void processElement(ProcessContext ctx) {
          ctx.output(ctx.element().getValue());
        }
      }))
      .apply(
        "Dumping the temporal category of issues and lineages while interpreting the records in a defined hive table location",
        AvroIO.write(IssueLineageRecord.class).to("demo/output/interpreted-temporalissue"));
    //Dumping the spatial category of issues and lineages while interpreting the records in a defined hive table location.
    interpretedCategory.get(spatialCategoryIssues)
      .setCoder(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(IssueLineageRecord.class)))
      .apply(ParDo.of(new DoFn<KV<String, IssueLineageRecord>, IssueLineageRecord>() {
        @ProcessElement
        public void processElement(ProcessContext ctx) {
          ctx.output(ctx.element().getValue());
        }
      }))
      .apply(
        "Dumping the spatial category of issues and lineages while interpreting the records in a defined hive table location",
        AvroIO.write(IssueLineageRecord.class).to("demo/output/interpreted-spatialissue"));
    ;

    /*
      Joining temporal category and spatial category to get the big flat interpreted record.
     */
    PCollection<KV<String, CoGbkResult>> joinedCollection =
      KeyedPCollectionTuple.of(temporalTag, interpretedCategory.get(temporalCategory))
        .and(spatialTag, interpretedCategory.get(spatialCategory))
        .apply(CoGroupByKey.create());

    PCollection<ExtendedOccurence> interpretedRecords = joinedCollection.apply(
      "Applying join on interpreted category of records to make a flat big interpreted record",
      ParDo.of(new CoGbkResultToFlattenedInterpretedRecord()));
    /*
      Joining temporal category issues and spatial category issues to get the overall issues together.
     */
    PCollection<KV<String, CoGbkResult>> joinedIssueCollection =
      KeyedPCollectionTuple.of(temporalIssueTag, interpretedCategory.get(temporalCategoryIssues))
        .and(spatialIssueTag, interpretedCategory.get(spatialCategoryIssues))
        .apply(CoGroupByKey.create());

    PCollection<IssueLineageRecord> interpretedIssueLineageRecords = joinedIssueCollection.apply(
      "Aplying join on the issues and lineages obtained",
      ParDo.of(new CoGbkResultToFlattenedInterpretedIssueRecord()));

    // Write the big flat final interpreted records as an Avro file in defined hive table
    interpretedRecords.apply("Save the interpreted records as Avro",
                             AvroIO.write(ExtendedOccurence.class).to("demo/output/interpreted-data"));
    // Write the issue and lineage result as an Avro file in defined table
    interpretedIssueLineageRecords.apply("Save the interpreted records issues and lineages as Avro",
                                         AvroIO.write(IssueLineageRecord.class)
                                           .to("demo/output/interpreted_issue_lineage-data"));

    LOG.info("Starting the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();
    LOG.info("Pipeline finished with state: {} ", result.getState());
  }

  /**
   * Convert's Beam's represented Joined PCollection to an Interpreted Occurrence
   */
  static class CoGbkResultToFlattenedInterpretedRecord extends DoFn<KV<String, CoGbkResult>, ExtendedOccurence> {

    @ProcessElement
    public void processElement(ProcessContext ctx) {
      KV<String, CoGbkResult> result = ctx.element();
      //get temporal and spatial info from the joined beam collection with tags
      Event evt = result.getValue().getOnly(temporalTag);
      Location loc = result.getValue().getOnly(spatialTag);

      //create final interpreted record with values from the interpreted category
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

  /**
   * Convert's Beam's represented Joined Issues PCollection to an IssueAndLineageRecord
   */
  static class CoGbkResultToFlattenedInterpretedIssueRecord extends DoFn<KV<String, CoGbkResult>, IssueLineageRecord> {

    @ProcessElement
    public void processElement(ProcessContext ctx) {
      KV<String, CoGbkResult> result = ctx.element();
      //get temporal and spatial issues info from the joined beam collection with tags
      IssueLineageRecord evt = result.getValue().getOnly(temporalIssueTag);
      IssueLineageRecord loc = result.getValue().getOnly(spatialIssueTag);

      Map<CharSequence, List<Issue>> fieldIssueMap = new HashMap<CharSequence, List<Issue>>();
      fieldIssueMap.putAll(evt.getFieldIssuesMap());
      fieldIssueMap.putAll(loc.getFieldIssuesMap());

      Map<CharSequence, List<Lineage>> fieldLineageMap = new HashMap<CharSequence, List<Lineage>>();
      fieldLineageMap.putAll(evt.getFieldLineageMap());
      fieldLineageMap.putAll(loc.getFieldLineageMap());
      //construct a final IssueLineageRecord for all categories
      IssueLineageRecord record = IssueLineageRecord.newBuilder()
        .setOccurenceId(evt.getOccurenceId())
        .setFieldIssuesMap(fieldIssueMap)
        .setFieldLineageMap(fieldLineageMap)
        .build();
      ctx.output(record);
    }
  }

}
