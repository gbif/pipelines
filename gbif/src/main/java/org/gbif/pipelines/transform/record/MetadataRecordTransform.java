package org.gbif.pipelines.transform.record;

import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.core.interpretation.Interpretation;
import org.gbif.pipelines.core.interpretation.MetadataInerpreter;
import org.gbif.pipelines.core.interpretation.MultimediaInterpreter;
import org.gbif.pipelines.core.ws.config.Config;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.issue.OccurrenceIssue;
import org.gbif.pipelines.io.avro.issue.Validation;
import org.gbif.pipelines.transform.RecordTransform;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

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
        String datasetId = context.element();

        // create the target record
        MetadataRecord metadataRecord = MetadataRecord.newBuilder().setDatasetId(datasetId).build();
        // list to collect the validations
        List<Validation> validations = new ArrayList<>();

        // Interpret metadata terms and add validations
        Interpretation.of(datasetId)
            .using(MetadataInerpreter.interpretDataset(metadataRecord, wsConfig))
            .using(MetadataInerpreter.interpretInstallation(metadataRecord, wsConfig))
            .using(MetadataInerpreter.interpretOrganization(metadataRecord, wsConfig))
            .forEachValidation(trace -> validations.add(toValidation(trace.getContext())));

        // Add validations to the additional output
        if (!validations.isEmpty()) {
          OccurrenceIssue issue =
              OccurrenceIssue.newBuilder().setId(datasetId).setIssues(validations).build();
          context.output(getIssueTag(), KV.of(datasetId, issue));
        }

        // Main output
        context.output(getDataTag(), KV.of(datasetId, metadataRecord));
      }
    };
  }

  @Override
  public MetadataRecordTransform withAvroCoders(Pipeline pipeline) {
    Coders.registerAvroCoders(pipeline, OccurrenceIssue.class, MetadataRecord.class);
    return this;
  }
}
