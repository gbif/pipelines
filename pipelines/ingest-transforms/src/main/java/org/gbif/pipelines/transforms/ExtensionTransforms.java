package org.gbif.pipelines.transforms;

import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.core.interpreters.core.MultimediaInterpreter;
import org.gbif.pipelines.core.predicates.ExtensionPredicates;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MULTIMEDIA_RECORDS_COUNT;

/**
 * Contains ParDo functions for Beam, each method returns GBIF transformation (basic, temporal,
 * multimedia, location, metadata, taxonomy). Transformation uses {@link
 * org.gbif.pipelines.core.interpreters} to interpret and convert source data to target data
 *
 * <p>You can apply this functions to your Beam pipeline:
 *
 * <pre>{@code
 * PCollection<ExtendedRecord> records = ...
 * PCollection<TemporalRecord> t = records.apply(ParDo.of(new BasicFn()));
 *
 * }</pre>
 */
public class ExtensionTransforms {

  private ExtensionTransforms() {}

  /**
   * ParDo runs sequence of interpretations for {@link MultimediaRecord} using {@link
   * ExtendedRecord} as a source and {@link MultimediaInterpreter} as interpretation steps
   */
  public static class MultimediaFn extends DoFn<ExtendedRecord, MultimediaRecord> {

    private final Counter counter = Metrics.counter(ExtensionTransforms.class, MULTIMEDIA_RECORDS_COUNT);

    @ProcessElement
    public void processElement(ProcessContext context) {
      Interpretation.from(context::element)
          .to(er -> MultimediaRecord.newBuilder().setId(er.getId()).build())
          .when(ExtensionPredicates.multimediaPr())
          .via(MultimediaInterpreter::interpretMultimedia)
          .consume(context::output);

      counter.inc();
    }
  }

}
