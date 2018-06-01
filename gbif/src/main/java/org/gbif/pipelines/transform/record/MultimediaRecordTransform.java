package org.gbif.pipelines.transform.record;

import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.core.interpretation.Interpretation;
import org.gbif.pipelines.core.interpretation.MultimediaInterpreter;
import org.gbif.pipelines.io.avro.record.ExtendedRecord;
import org.gbif.pipelines.io.avro.record.issue.OccurrenceIssue;
import org.gbif.pipelines.io.avro.record.issue.Validation;
import org.gbif.pipelines.io.avro.record.multimedia.MultimediaRecord;
import org.gbif.pipelines.transform.RecordTransform;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * {@link org.apache.beam.sdk.transforms.PTransform} that runs the {@link MultimediaInterpreter}.
 */
public class MultimediaRecordTransform extends RecordTransform<ExtendedRecord, MultimediaRecord> {

  private MultimediaRecordTransform() {
    super("Interpret multimedia record");
  }

  public static MultimediaRecordTransform create() {
    return new MultimediaRecordTransform();
  }

  @Override
  public DoFn<ExtendedRecord, KV<String, MultimediaRecord>> interpret() {
    return new DoFn<ExtendedRecord, KV<String, MultimediaRecord>>() {
      @ProcessElement
      public void processElement(ProcessContext context) {
        // get the record
        ExtendedRecord extendedRecord = context.element();
        String id = extendedRecord.getId();
        // create the target record
        MultimediaRecord multimediaRecord = MultimediaRecord.newBuilder().setId(id).build();
        // list to collect the validations
        List<Validation> validations = new ArrayList<>();

        // Interpret multimedia terms and add validations
        Interpretation.of(extendedRecord)
          .using(MultimediaInterpreter.interpretMultimedia(multimediaRecord))
          .forEachValidation(trace -> validations.add(toValidation(trace.getContext())));

        // Add validations to the additional output
        if (!validations.isEmpty()) {
          OccurrenceIssue issue = OccurrenceIssue.newBuilder().setId(id).setIssues(validations).build();
          context.output(getIssueTag(), KV.of(id, issue));
        }

        // Main output
        context.output(getDataTag(), KV.of(id, multimediaRecord));
      }
    };
  }

  @Override
  public MultimediaRecordTransform withAvroCoders(Pipeline pipeline) {
    Coders.registerAvroCoders(pipeline, OccurrenceIssue.class, MultimediaRecord.class, ExtendedRecord.class);
    return this;
  }
}
