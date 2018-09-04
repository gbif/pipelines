package org.gbif.pipelines.base.transforms;

import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.core.interpreters.BasicInterpreter;
import org.gbif.pipelines.core.interpreters.LocationInterpreter;
import org.gbif.pipelines.core.interpreters.MetadataInterpreter;
import org.gbif.pipelines.core.interpreters.MultimediaInterpreter;
import org.gbif.pipelines.core.interpreters.TaxonomyInterpreter;
import org.gbif.pipelines.core.interpreters.TemporalInterpreter;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.parsers.ws.config.ServiceType;
import org.gbif.pipelines.parsers.ws.config.WsConfig;
import org.gbif.pipelines.parsers.ws.config.WsConfigFactory;

import java.nio.file.Paths;
import java.util.Optional;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;

/**
 * Contains ParDo functions for Beam, each method returns GBIF interpretation (basic, temporal,
 * multimedia, location, metadata, taxonomy)
 *
 * <p>You can apply this functions to your Beam pipeline:
 *
 * <pre>{@code
 * PCollection<ExtendedRecord> records = ...
 * PCollection<TemporalRecord> t = records.apply(Transforms.temporal());
 *
 * }</pre>
 */
public class RecordTransforms {

  private RecordTransforms() {}

  /**
   * ParDo runs sequence of interpretations for {@link MultimediaRecord} using {@link
   * ExtendedRecord} as a source and {@link MultimediaInterpreter} as interpretation steps
   */
  public static SingleOutput<ExtendedRecord, MultimediaRecord> multimedia() {
    return ParDo.of(
        new DoFn<ExtendedRecord, MultimediaRecord>() {
          @ProcessElement
          public void processElement(ProcessContext context) {
            Interpretation.from(context.element())
                .to(er -> MultimediaRecord.newBuilder().setId(er.getId()).build())
                .via(MultimediaInterpreter::interpretMultimedia)
                .consume(context::output);
          }
        });
  }

  /**
   * ParDo runs sequence of interpretations for {@link TemporalRecord} using {@link ExtendedRecord}
   * as a source and {@link TemporalInterpreter} as interpretation steps
   */
  public static SingleOutput<ExtendedRecord, TemporalRecord> temporal() {
    return ParDo.of(
        new DoFn<ExtendedRecord, TemporalRecord>() {
          @ProcessElement
          public void processElement(ProcessContext context) {
            Interpretation.from(context.element())
                .to(er -> TemporalRecord.newBuilder().setId(er.getId()).build())
                .via(TemporalInterpreter::interpretEventDate)
                .via(TemporalInterpreter::interpretDateIdentified)
                .via(TemporalInterpreter::interpretModifiedDate)
                .via(TemporalInterpreter::interpretDayOfYear)
                .consume(context::output);
          }
        });
  }

  /**
   * ParDo runs sequence of interpretations for {@link BasicRecord} using {@link ExtendedRecord} as
   * a source and {@link BasicInterpreter} as interpretation steps
   */
  public static SingleOutput<ExtendedRecord, BasicRecord> basic() {
    return ParDo.of(
        new DoFn<ExtendedRecord, BasicRecord>() {
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
                .consume(context::output);
          }
        });
  }

  /**
   * ParDo runs sequence of interpretations for {@link LocationRecord} using {@link ExtendedRecord}
   * as a source and {@link LocationInterpreter} as interpretation steps
   */
  public static SingleOutput<ExtendedRecord, LocationRecord> location(String properties) {
    WsConfig wsConfig = WsConfigFactory.create(ServiceType.GEO_CODE, Paths.get(properties));
    return ParDo.of(
        new DoFn<ExtendedRecord, LocationRecord>() {
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
                .consume(context::output);
          }
        });
  }

  /**
   * ParDo runs sequence of interpretations for {@link MetadataRecord} using {@link ExtendedRecord}
   * as a source and {@link MetadataInterpreter} as interpretation steps
   */
  public static SingleOutput<String, MetadataRecord> metadata(String properties) {
    WsConfig wsConfig = WsConfigFactory.create(ServiceType.DATASET_META, Paths.get(properties));
    return ParDo.of(
        new DoFn<String, MetadataRecord>() {
          @ProcessElement
          public void processElement(ProcessContext context) {
            Interpretation.from(context.element())
                .to(id -> MetadataRecord.newBuilder().setDatasetId(id).build())
                .via(MetadataInterpreter.interpretDataset(wsConfig))
                .via(MetadataInterpreter.interpretInstallation(wsConfig))
                .via(MetadataInterpreter.interpretOrganization(wsConfig))
                .consume(context::output);
          }
        });
  }

  /**
   * ParDo runs sequence of interpretations for {@link TaxonRecord} using {@link ExtendedRecord} as
   * a source and {@link TaxonomyInterpreter} as interpretation steps
   */
  public static SingleOutput<ExtendedRecord, TaxonRecord> taxonomy(String properties) {
    WsConfig wsConfig = WsConfigFactory.create(ServiceType.SPECIES_MATCH2, Paths.get(properties));
    return ParDo.of(
        new DoFn<ExtendedRecord, TaxonRecord>() {
          @ProcessElement
          public void processElement(ProcessContext context) {
            Interpretation.from(context.element())
                .to(TaxonRecord.newBuilder()::build)
                .via(TaxonomyInterpreter.taxonomyInterpreter(wsConfig))
                // the id is null when there is an error in the interpretation. In these
                // cases we do not write the taxonRecord because it is totally empty.
                .consume(v -> Optional.ofNullable(v.getId()).ifPresent(id -> context.output(v)));
          }
        });
  }
}
