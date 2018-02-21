package org.gbif.pipelines.transforms;

import org.gbif.dwca.avro.Event;
import org.gbif.dwca.avro.Location;
import org.gbif.pipelines.core.config.Interpretation;
import org.gbif.pipelines.core.functions.interpretation.error.IssueLineageRecord;

import java.util.Map;

import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionTuple;

/**
 * This transform dumps the avro file of different categories in provided paths
 */
public class InterpretedCategoryAvroDump extends PTransform<PCollectionTuple, PCollectionTuple> {

  private final InterpretedCategoryTransform transformer;
  private final Map<Interpretation, String> targetPaths;

  public InterpretedCategoryAvroDump(
    InterpretedCategoryTransform transformer, Map<Interpretation, String> targetPaths
  ) {
    this.transformer = transformer;
    this.targetPaths = targetPaths;
  }

  @Override
  public PCollectionTuple expand(PCollectionTuple interpretedCategory) {

    //Dumping the temporal category of interpreted records in an defined hive table location.
    interpretedCategory.get(transformer.getTemporalCategory())
      .apply(ParDo.of(transformToValueFn()))
      .apply("Dumping the temporal category of interpreted records in an defined hive table location.",
             AvroIO.write(Event.class).to(targetPaths.get(Interpretation.TEMPORAL)));

    //Dumping the spatial category of interpreted records in a defined hive table location.
    interpretedCategory.get(transformer.getSpatialCategory())
      .apply(ParDo.of(transformToValueFn()))
      .apply("Dumping the spatial category of interpreted records in a defined hive table location.",
             AvroIO.write(Location.class).to(targetPaths.get(Interpretation.LOCATION)));

    //Dumping the temporal category of issues and lineages while interpreting the records in a defined hive table location.
    interpretedCategory.get(transformer.getTemporalCategoryIssues())
      .apply(ParDo.of(transformToValueFn()))
      .apply(
        "Dumping the temporal category of issues and lineages while interpreting the records in a defined hive table location",
        AvroIO.write(IssueLineageRecord.class).to(targetPaths.get(Interpretation.TEMPORAL_ISSUE)));

    //Dumping the spatial category of issues and lineages while interpreting the records in a defined hive table location.
    interpretedCategory.get(transformer.getSpatialCategoryIssues())
      .apply(ParDo.of(transformToValueFn()))
      .apply(
        "Dumping the spatial category of issues and lineages while interpreting the records in a defined hive table location",
        AvroIO.write(IssueLineageRecord.class).to(targetPaths.get(Interpretation.LOCATION_ISSUE)));

    return PCollectionTuple.empty(interpretedCategory.getPipeline());

  }

  private <T> DoFn<KV<String, T>, T> transformToValueFn() {
    return new DoFn<KV<String, T>, T>() {
      @ProcessElement
      public void processElement(ProcessContext ctx) {
        ctx.output(ctx.element().getValue());
      }
    };
  }
}
