package org.gbif.pipelines.transforms;

import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.core.interpreters.extension.AmplificationInterpreter;
import org.gbif.pipelines.core.interpreters.extension.AudubonInterpreter;
import org.gbif.pipelines.core.interpreters.extension.ImageInterpreter;
import org.gbif.pipelines.core.interpreters.extension.MeasurementOrFactInterpreter;
import org.gbif.pipelines.core.interpreters.extension.MultimediaInterpreter;
import org.gbif.pipelines.io.avro.AmplificationRecord;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFactRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AMPLIFICATION_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AUDUBON_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.IMAGE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MEASUREMEN_OR_FACT_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MULTIMEDIA_RECORDS_COUNT;

/**
 * Contains ParDo functions for Beam, each method returns GBIF extension transformation (multimedia, image and etc.).
 * Transformation uses {@link org.gbif.pipelines.core.interpreters} to interpret and convert source data to target data
 *
 * <p>You can apply this functions to your Beam pipeline:
 *
 * <pre>{@code
 * PCollection<ExtendedRecord> records = ...
 * PCollection<MultimediaRecord> t = records.apply(ParDo.of(new MultimediaFn()));
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
          .when(er -> er.getExtensions().containsKey(Extension.MULTIMEDIA.getRowType()))
          .via(MultimediaInterpreter::interpret)
          .consume(context::output);

      counter.inc();
    }
  }

  /**
   * ParDo runs sequence of interpretations for {@link ImageRecord} using {@link
   * ExtendedRecord} as a source and {@link ImageInterpreter} as interpretation steps
   */
  public static class ImageFn extends DoFn<ExtendedRecord, ImageRecord> {

    private final Counter counter = Metrics.counter(ExtensionTransforms.class, IMAGE_RECORDS_COUNT);

    @ProcessElement
    public void processElement(ProcessContext context) {
      Interpretation.from(context::element)
          .to(er -> ImageRecord.newBuilder().setId(er.getId()).build())
          .when(er -> er.getExtensions().containsKey(Extension.IMAGE.getRowType()))
          .via(ImageInterpreter::interpret)
          .consume(context::output);

      counter.inc();
    }
  }

  /**
   * ParDo runs sequence of interpretations for {@link AudubonRecord} using {@link
   * ExtendedRecord} as a source and {@link AudubonInterpreter} as interpretation steps
   */
  public static class AudubonFn extends DoFn<ExtendedRecord, AudubonRecord> {

    private final Counter counter = Metrics.counter(ExtensionTransforms.class, AUDUBON_RECORDS_COUNT);

    @ProcessElement
    public void processElement(ProcessContext context) {
      Interpretation.from(context::element)
          .to(er -> AudubonRecord.newBuilder().setId(er.getId()).build())
          .when(er -> er.getExtensions().containsKey(Extension.AUDUBON.getRowType()))
          .via(AudubonInterpreter::interpret)
          .consume(context::output);

      counter.inc();
    }
  }

  /**
   * ParDo runs sequence of interpretations for {@link MeasurementOrFactRecord} using {@link
   * ExtendedRecord} as a source and {@link MeasurementOrFactInterpreter} as interpretation steps
   */
  public static class MeasurementOrFactFn extends DoFn<ExtendedRecord, MeasurementOrFactRecord> {

    private final Counter counter = Metrics.counter(ExtensionTransforms.class, MEASUREMEN_OR_FACT_RECORDS_COUNT);

    @ProcessElement
    public void processElement(ProcessContext context) {
      Interpretation.from(context::element)
          .to(er -> MeasurementOrFactRecord.newBuilder().setId(er.getId()).build())
          .when(er -> er.getExtensions().containsKey(Extension.MEASUREMENT_OR_FACT.getRowType()))
          .via(MeasurementOrFactInterpreter::interpret)
          .consume(context::output);

      counter.inc();
    }
  }

  /**
   * ParDo runs sequence of interpretations for {@link AmplificationRecord} using {@link
   * ExtendedRecord} as a source and {@link AmplificationInterpreter} as interpretation steps
   */
  public static class AmplificationFn extends DoFn<ExtendedRecord, AmplificationRecord> {

    private final Counter counter = Metrics.counter(ExtensionTransforms.class, AMPLIFICATION_RECORDS_COUNT);

    @ProcessElement
    public void processElement(ProcessContext context) {
      Interpretation.from(context::element)
          .to(er -> AmplificationRecord.newBuilder().setId(er.getId()).build())
          .when(er -> er.getExtensions().containsKey(AmplificationInterpreter.EXTENSION_ROW_TYPE))
          .via(AmplificationInterpreter::interpret)
          .consume(context::output);

      counter.inc();
    }
  }

}
