package org.gbif.pipelines.base;

import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.parsers.Interpretation;
import org.gbif.pipelines.parsers.interpreter.BasicInterpreter;
import org.gbif.pipelines.parsers.interpreter.LocationInterpreter;
import org.gbif.pipelines.parsers.interpreter.MetadataInterpreter;
import org.gbif.pipelines.parsers.interpreter.MultimediaInterpreter;
import org.gbif.pipelines.parsers.interpreter.TaxonomyInterpreter;
import org.gbif.pipelines.parsers.interpreter.TemporalInterpreter;
import org.gbif.pipelines.parsers.ws.config.Config;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.values.KV;

import static java.util.Optional.ofNullable;

/**
 * Contains ParDo functions for Beam, each method returns GBIF interpretation (common, temporal,
 * multimedia, location, metadata, taxonomy)
 *
 * <p>You can apply this functions to your Beam pipeline:
 *
 * <pre>{@code
 * PCollection<ExtendedRecord> records = ...
 * PCollection<KV<String, TemporalRecord>> t = records.apply(Transforms.temporal());
 *
 * }</pre>
 */
public class Transforms {

  private Transforms() {}

  /**
   * ParDo runs sequence of interpretations for {@link MultimediaRecord} using {@link
   * ExtendedRecord} as a source and {@link MultimediaInterpreter} as interpretation steps
   */
  public static SingleOutput<ExtendedRecord, KV<String, MultimediaRecord>> multimedia() {
    return ParDo.of(
        new DoFn<ExtendedRecord, KV<String, MultimediaRecord>>() {
          @ProcessElement
          public void processElement(ProcessContext context) {
            Interpretation.from(context.element())
                .to(er -> MultimediaRecord.newBuilder().setId(er.getId()).build())
                .via(MultimediaInterpreter::interpretMultimedia)
                .consume(v -> context.output(KV.of(v.getId(), v)));
          }
        });
  }

  /**
   * ParDo runs sequence of interpretations for {@link TemporalRecord} using {@link ExtendedRecord}
   * as a source and {@link TemporalInterpreter} as interpretation steps
   */
  public static SingleOutput<ExtendedRecord, KV<String, TemporalRecord>> temporal() {
    return ParDo.of(
        new DoFn<ExtendedRecord, KV<String, TemporalRecord>>() {
          @ProcessElement
          public void processElement(ProcessContext context) {
            Interpretation.from(context.element())
                .to(er -> TemporalRecord.newBuilder().setId(er.getId()).build())
                .via(TemporalInterpreter::interpretEventDate)
                .via(TemporalInterpreter::interpretDateIdentified)
                .via(TemporalInterpreter::interpretModifiedDate)
                .via(TemporalInterpreter::interpretDayOfYear)
                .consume(v -> context.output(KV.of(v.getId(), v)));
          }
        });
  }

  /**
   * ParDo runs sequence of interpretations for {@link BasicRecord} using {@link ExtendedRecord} as
   * a source and {@link BasicInterpreter} as interpretation steps
   */
  public static SingleOutput<ExtendedRecord, KV<String, BasicRecord>> common() {
    return ParDo.of(
        new DoFn<ExtendedRecord, KV<String, BasicRecord>>() {
          @ProcessElement
          public void processElement(ProcessContext context) {
            Interpretation.from(context.element())
                .to(er -> BasicRecord.newBuilder().setId(er.getId()).build())
                .via(BasicInterpreter::interpretBasisOfRecord)
                .via(BasicInterpreter::interpretSex)
                .via(BasicInterpreter::interpretEstablishmentMeans)
                .via(BasicInterpreter::interpretLifeStage)
                .via(BasicInterpreter::interpretTypeStatus)
                .via(BasicInterpreter::interpretIndividualCount)
                .via(BasicInterpreter::interpretReferences)
                .consume(v -> context.output(KV.of(v.getId(), v)));
          }
        });
  }

  /**
   * ParDo runs sequence of interpretations for {@link LocationRecord} using {@link ExtendedRecord}
   * as a source and {@link LocationInterpreter} as interpretation steps
   */
  public static SingleOutput<ExtendedRecord, KV<String, LocationRecord>> location(Config wsConfig) {
    return ParDo.of(
        new DoFn<ExtendedRecord, KV<String, LocationRecord>>() {
          @ProcessElement
          public void processElement(ProcessContext context) {
            Interpretation.from(context.element())
                .to(er -> LocationRecord.newBuilder().setId(er.getId()).build())
                .via(LocationInterpreter.interpretCountryAndCoordinates(wsConfig))
                .via(LocationInterpreter::interpretContinent)
                .via(LocationInterpreter::interpretWaterBody)
                .via(LocationInterpreter::interpretStateProvince)
                .via(LocationInterpreter::interpretMinimumElevationInMeters)
                .via(LocationInterpreter::interpretMaximumElevationInMeters)
                .via(LocationInterpreter::interpretMinimumDepthInMeters)
                .via(LocationInterpreter::interpretMaximumDepthInMeters)
                .via(LocationInterpreter::interpretMinimumDistanceAboveSurfaceInMeters)
                .via(LocationInterpreter::interpretMaximumDistanceAboveSurfaceInMeters)
                .via(LocationInterpreter::interpretCoordinatePrecision)
                .via(LocationInterpreter::interpretCoordinateUncertaintyInMeters)
                .consume(v -> context.output(KV.of(v.getId(), v)));
          }
        });
  }

  /**
   * ParDo runs sequence of interpretations for {@link MetadataRecord} using {@link ExtendedRecord}
   * as a source and {@link MetadataInterpreter} as interpretation steps
   */
  public static SingleOutput<String, KV<String, MetadataRecord>> metadata(Config wsConfig) {
    return ParDo.of(
        new DoFn<String, KV<String, MetadataRecord>>() {
          @ProcessElement
          public void processElement(ProcessContext context) {
            Interpretation.from(context.element())
                .to(id -> MetadataRecord.newBuilder().setDatasetId(id).build())
                .via(MetadataInterpreter.interpretDataset(wsConfig))
                .via(MetadataInterpreter.interpretInstallation(wsConfig))
                .via(MetadataInterpreter.interpretOrganization(wsConfig))
                .consume(v -> context.output(KV.of(v.getDatasetId(), v)));
          }
        });
  }

  /**
   * ParDo runs sequence of interpretations for {@link TaxonRecord} using {@link ExtendedRecord} as
   * a source and {@link TaxonomyInterpreter} as interpretation steps
   */
  public static SingleOutput<ExtendedRecord, KV<String, TaxonRecord>> taxonomy(Config wsConfig) {
    return ParDo.of(
        new DoFn<ExtendedRecord, KV<String, TaxonRecord>>() {
          @ProcessElement
          public void processElement(ProcessContext context) {
            Interpretation.from(context.element())
                .to(TaxonRecord.newBuilder()::build)
                .via(TaxonomyInterpreter.taxonomyInterpreter(wsConfig))
                // the id is null when there is an error in the interpretation. In these
                // cases we do not write the taxonRecord because it is totally empty.
                .consume(v -> ofNullable(v.getId()).ifPresent(id -> context.output(KV.of(id, v))));
          }
        });
  }
}
