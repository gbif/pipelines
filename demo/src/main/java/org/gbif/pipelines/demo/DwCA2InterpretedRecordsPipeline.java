package org.gbif.pipelines.demo;

import org.gbif.dwca.avro.Event;
import org.gbif.dwca.avro.ExtendedOccurrence;
import org.gbif.dwca.avro.Location;
import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.common.beam.DwCAIO;
import org.gbif.pipelines.core.config.DataFlowPipelineOptions;
import org.gbif.pipelines.core.config.RecordInterpretation;
import org.gbif.pipelines.core.functions.interpretation.error.Issue;
import org.gbif.pipelines.core.functions.interpretation.error.IssueLineageRecord;
import org.gbif.pipelines.core.functions.interpretation.error.Lineage;
import org.gbif.pipelines.demo.utils.PipelineUtils;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transform.ExtendedOccurrenceAvroDump;
import org.gbif.pipelines.transform.ExtendedOccurrenceTransform;
import org.gbif.pipelines.transform.InterpretedCategoryAvroDump;
import org.gbif.pipelines.transform.InterpretedCategoryTransform;

import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple demonstration showing a pipeline running locally which will read UntypedOccurrence from a DwC-A file
 * transform it into interpreted occurence records
 * .
 * <p>
 * Run it
 * On LocalFileSystem
 * mvn compile exec:java -Dexec.mainClass=org.gbif.pipelines.demo.DwCA2InterpretedRecordsPipeline -Dexec.args="--datasetId=abc123 --inputFile=data/dwca.zip" -Pdirect-runner
 * On HDFS
 * mvn compile exec:java -Dexec.mainClass=org.gbif.pipelines.demo.DwCA2InterpretedRecordsPipeline -Dexec.args="--datasetId=abc123 --inputFile=data/dwca.zip --HDFSConfigurationDirectory=/path/to/hadoop-conf/ --defaultTargetDirectory=hdfs://ha-nn/user/hive/warehouse/gbif-data/abc123/" -Pdirect-runner
 */
public class DwCA2InterpretedRecordsPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(DwCA2InterpretedRecordsPipeline.class);

  public static void main(String[] args) {
    DataFlowPipelineOptions options = PipelineUtils.createPipelineOptions(args);
    Map<RecordInterpretation, String> targetPaths = options.getTargetPaths();
    InterpretedCategoryTransform transformer = new InterpretedCategoryTransform();
    ExtendedOccurrenceTransform interpretedRecordTransform = new ExtendedOccurrenceTransform(transformer);
    Pipeline p = Pipeline.create(options);
    //register coders for the pipeline
    registerPipeLineCoders(p, transformer, interpretedRecordTransform);

    // Read the DwC-A using our custom reader
    PCollection<ExtendedRecord> rawRecords = p.apply("Read from Darwin Core Archive",
                                                     DwCAIO.Read.withPaths(options.getInputFile(),
                                                                           targetPaths.get(RecordInterpretation.TEMP_DwCA_PATH)));

    // Write records in an avro file, this will be location of the hive table which has raw records
    rawRecords.apply("Save the interpreted records as Avro",
                     AvroIO.write(ExtendedRecord.class).to(targetPaths.get(RecordInterpretation.RAW_OCCURRENCE)));

    //Interpret the raw records as a tuple, which has both different categories of data and issue related to them

    PCollectionTuple interpretedCategory =
      rawRecords.apply("split raw record to different category with isues and lineages", transformer);

    // Write the individual category of the raw records to avro file
    InterpretedCategoryAvroDump avroFileDumper = new InterpretedCategoryAvroDump(transformer, targetPaths);
    interpretedCategory.apply("write the individual categories to different avro files", avroFileDumper);
    // join the individual categories and create a flat interepreted occurence and write them to avro file.
    PCollectionTuple interpretedoccurence = interpretedCategory.apply(
      "join indiviual categories and issues to a flat interpreted occurence and issue lineage collections",
      interpretedRecordTransform);
    //writing interpreted occurence and issues to the avro file
    interpretedoccurence.apply("Write interpreted occurence and issues to avro file",
                               new ExtendedOccurrenceAvroDump(interpretedRecordTransform, targetPaths));

    LOG.info("Starting the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();
    LOG.info("Pipeline finished with state: {} ", result.getState());
  }

  /**
   * register Avro coders for serializing our messages
   */
  private static void registerPipeLineCoders(
    Pipeline p, InterpretedCategoryTransform transformer, ExtendedOccurrenceTransform interpretRecordTransform
  ) {

    Coders.registerAvroCoders(p,
                              ExtendedRecord.class,
                              Event.class,
                              Location.class,
                              ExtendedOccurrence.class,
                              Issue.class,
                              Lineage.class,
                              IssueLineageRecord.class);
    Coders.registerAvroCodersForTypes(p,
                                      interpretRecordTransform.getTemporalTag(),
                                      interpretRecordTransform.getSpatialTag(),
                                      interpretRecordTransform.getTemporalIssueTag(),
                                      interpretRecordTransform.getSpatialIssueTag());
    Coders.registerAvroCodersForKVTypes(p,
                                        new TupleTag[] {transformer.getSpatialCategory(),
                                          transformer.getSpatialCategory(), transformer.getTemporalCategoryIssues(),
                                          transformer.getSpatialCategoryIssues()},
                                        Event.class,
                                        Location.class,
                                        IssueLineageRecord.class,
                                        IssueLineageRecord.class);
  }

}
