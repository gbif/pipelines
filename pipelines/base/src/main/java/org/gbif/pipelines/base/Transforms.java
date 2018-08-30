package org.gbif.pipelines.base;

import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.core.interpretation.ExtendedRecordInterpreter;
import org.gbif.pipelines.core.interpretation.LocationInterpreter;
import org.gbif.pipelines.core.interpretation.MetadataInterpreter;
import org.gbif.pipelines.core.interpretation.MultimediaInterpreter;
import org.gbif.pipelines.core.interpretation.TaxonomyInterpreter;
import org.gbif.pipelines.core.interpretation.TemporalRecordInterpreter;
import org.gbif.pipelines.core.ws.config.Config;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

import java.util.Optional;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.values.KV;

/** TODO */
public class Transforms {

  private Transforms() {}

  /** TODO */
  public static SingleOutput<ExtendedRecord, KV<String, MultimediaRecord>> multimedia() {
    return ParDo.of(
        new DoFn<ExtendedRecord, KV<String, MultimediaRecord>>() {
          @ProcessElement
          public void processElement(ProcessContext context) {
            Interpretation.of(context.element())
                .convert(MultimediaInterpreter::createContext)
                .using(MultimediaInterpreter::interpretMultimedia)
                .consumeValue(v -> context.output(KV.of(v.getId(), v)));
          }
        });
  }

  /** TODO */
  public static SingleOutput<ExtendedRecord, KV<String, TemporalRecord>> temporal() {
    return ParDo.of(
        new DoFn<ExtendedRecord, KV<String, TemporalRecord>>() {
          @ProcessElement
          public void processElement(ProcessContext context) {
            Interpretation.of(context.element())
                .convert(TemporalRecordInterpreter::createContext)
                .using(TemporalRecordInterpreter::interpretEventDate)
                .using(TemporalRecordInterpreter::interpretDateIdentified)
                .using(TemporalRecordInterpreter::interpretModifiedDate)
                .using(TemporalRecordInterpreter::interpretDayOfYear)
                .consumeValue(v -> context.output(KV.of(v.getId(), v)));
          }
        });
  }

  /** TODO */
  public static SingleOutput<ExtendedRecord, KV<String, InterpretedExtendedRecord>> common() {
    return ParDo.of(
        new DoFn<ExtendedRecord, KV<String, InterpretedExtendedRecord>>() {
          @ProcessElement
          public void processElement(ProcessContext context) {
            Interpretation.of(context.element())
                .convert(ExtendedRecordInterpreter::createContext)
                .using(ExtendedRecordInterpreter::interpretBasisOfRecord)
                .using(ExtendedRecordInterpreter::interpretSex)
                .using(ExtendedRecordInterpreter::interpretEstablishmentMeans)
                .using(ExtendedRecordInterpreter::interpretLifeStage)
                .using(ExtendedRecordInterpreter::interpretTypeStatus)
                .using(ExtendedRecordInterpreter::interpretIndividualCount)
                .using(ExtendedRecordInterpreter::interpretReferences)
                .consumeValue(v -> context.output(KV.of(v.getId(), v)));
          }
        });
  }

  /** TODO */
  public static SingleOutput<ExtendedRecord, KV<String, LocationRecord>> location(Config wsConfig) {
    return ParDo.of(
        new DoFn<ExtendedRecord, KV<String, LocationRecord>>() {
          @ProcessElement
          public void processElement(ProcessContext context) {
            Interpretation.of(context.element())
                .convert(LocationInterpreter::createContext)
                .using(LocationInterpreter.interpretCountryAndCoordinates(wsConfig))
                .using(LocationInterpreter::interpretContinent)
                .using(LocationInterpreter::interpretWaterBody)
                .using(LocationInterpreter::interpretStateProvince)
                .using(LocationInterpreter::interpretMinimumElevationInMeters)
                .using(LocationInterpreter::interpretMaximumElevationInMeters)
                .using(LocationInterpreter::interpretMinimumDepthInMeters)
                .using(LocationInterpreter::interpretMaximumDepthInMeters)
                .using(LocationInterpreter::interpretMinimumDistanceAboveSurfaceInMeters)
                .using(LocationInterpreter::interpretMaximumDistanceAboveSurfaceInMeters)
                .using(LocationInterpreter::interpretCoordinatePrecision)
                .using(LocationInterpreter::interpretCoordinateUncertaintyInMeters)
                .consumeValue(v -> context.output(KV.of(v.getId(), v)));
          }
        });
  }

  /** TODO */
  public static SingleOutput<String, KV<String, MetadataRecord>> metadata(Config wsConfig) {
    return ParDo.of(
        new DoFn<String, KV<String, MetadataRecord>>() {
          @ProcessElement
          public void processElement(ProcessContext context) {
            Interpretation.of(context.element())
                .convert(MetadataInterpreter::createContext)
                .using(MetadataInterpreter.interpretDataset(wsConfig))
                .using(MetadataInterpreter.interpretInstallation(wsConfig))
                .using(MetadataInterpreter.interpretOrganization(wsConfig))
                .consumeValue(v -> context.output(KV.of(v.getDatasetId(), v)));
          }
        });
  }

  /** TODO */
  public static SingleOutput<ExtendedRecord, KV<String, TaxonRecord>> taxonomy(Config wsConfig) {
    return ParDo.of(
        new DoFn<ExtendedRecord, KV<String, TaxonRecord>>() {
          @ProcessElement
          public void processElement(ProcessContext context) {
            Interpretation.of(context.element())
                .convert(TaxonomyInterpreter::createContext)
                .using(TaxonomyInterpreter.taxonomyInterpreter(wsConfig))
                // the id is null when there is an error in the interpretation. In these
                // cases we do not write the taxonRecord because it is totally empty.
                .consumeValue(v -> Optional.ofNullable(v.getId()).ifPresent(id -> context.output(KV.of(id, v))));
          }
        });
  }
}
