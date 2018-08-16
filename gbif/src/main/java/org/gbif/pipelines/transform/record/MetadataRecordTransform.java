package org.gbif.pipelines.transform.record;

import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.core.interpretation.InterpreterHandler;
import org.gbif.pipelines.core.interpretation.MultimediaInterpreter;
import org.gbif.pipelines.core.ws.config.Config;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.issue.OccurrenceIssue;
import org.gbif.pipelines.transform.RecordTransform;

import java.util.Objects;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import static org.gbif.pipelines.core.interpretation.MetadataInterpreter.interpretDataset;
import static org.gbif.pipelines.core.interpretation.MetadataInterpreter.interpretId;
import static org.gbif.pipelines.core.interpretation.MetadataInterpreter.interpretInstallation;
import static org.gbif.pipelines.core.interpretation.MetadataInterpreter.interpretOrganization;

/**
 * {@link org.apache.beam.sdk.transforms.PTransform} that runs the {@link MultimediaInterpreter}.
 */
public class MetadataRecordTransform extends RecordTransform<String, MetadataRecord> {

  private final Config wsConfig;

  private MetadataRecordTransform(Config wsConfig) {
    super("Interpret location record");
    this.wsConfig = wsConfig;
  }

  public static MetadataRecordTransform create(Config wsConfig) {
    Objects.requireNonNull(wsConfig);
    return new MetadataRecordTransform(wsConfig);
  }

  @Override
  public DoFn<String, KV<String, MetadataRecord>> interpret() {
    return new DoFn<String, KV<String, MetadataRecord>>() {
      @ProcessElement
      public void processElement(ProcessContext context) {
        // get the record
        String id = context.element();

        // Interpret metadata terms and add validations
        InterpreterHandler.of(id, new MetadataRecord())
            .withId(id)
            .using(interpretId())
            .using(interpretDataset(wsConfig))
            .using(interpretInstallation(wsConfig))
            .using(interpretOrganization(wsConfig))
            .consumeData(d -> context.output(getDataTag(), KV.of(id, d)))
            .consumeIssue(i -> context.output(getIssueTag(), KV.of(id, i)));
      }
    };
  }

  @Override
  public MetadataRecordTransform withAvroCoders(Pipeline pipeline) {
    Coders.registerAvroCoders(pipeline, OccurrenceIssue.class, MetadataRecord.class);
    return this;
  }
}
