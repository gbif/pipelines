package org.gbif.pipelines.transform.record;

import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.core.interpretation.InterpreterHandler;
import org.gbif.pipelines.core.ws.config.Config;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.issue.OccurrenceIssue;
import org.gbif.pipelines.io.avro.location.LocationRecord;
import org.gbif.pipelines.transform.RecordTransform;

import java.util.Objects;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import static org.gbif.pipelines.core.interpretation.LocationInterpreter.interpretContinent;
import static org.gbif.pipelines.core.interpretation.LocationInterpreter.interpretCoordinatePrecision;
import static org.gbif.pipelines.core.interpretation.LocationInterpreter.interpretCoordinateUncertaintyInMeters;
import static org.gbif.pipelines.core.interpretation.LocationInterpreter.interpretCountryAndCoordinates;
import static org.gbif.pipelines.core.interpretation.LocationInterpreter.interpretId;
import static org.gbif.pipelines.core.interpretation.LocationInterpreter.interpretMaximumDepthInMeters;
import static org.gbif.pipelines.core.interpretation.LocationInterpreter.interpretMaximumDistanceAboveSurfaceInMeters;
import static org.gbif.pipelines.core.interpretation.LocationInterpreter.interpretMaximumElevationInMeters;
import static org.gbif.pipelines.core.interpretation.LocationInterpreter.interpretMinimumDepthInMeters;
import static org.gbif.pipelines.core.interpretation.LocationInterpreter.interpretMinimumDistanceAboveSurfaceInMeters;
import static org.gbif.pipelines.core.interpretation.LocationInterpreter.interpretMinimumElevationInMeters;
import static org.gbif.pipelines.core.interpretation.LocationInterpreter.interpretStateProvince;
import static org.gbif.pipelines.core.interpretation.LocationInterpreter.interpretWaterBody;

public class LocationRecordTransform extends RecordTransform<ExtendedRecord, LocationRecord> {

  private final Config wsConfig;

  private LocationRecordTransform(Config wsConfig) {
    super("Interpret location record");
    this.wsConfig = wsConfig;
  }

  public static LocationRecordTransform create(Config wsConfig) {
    Objects.requireNonNull(wsConfig);
    return new LocationRecordTransform(wsConfig);
  }

  @Override
  public DoFn<ExtendedRecord, KV<String, LocationRecord>> interpret() {
    return new DoFn<ExtendedRecord, KV<String, LocationRecord>>() {
      @ProcessElement
      public void processElement(ProcessContext context) {

        ExtendedRecord extendedRecord = context.element();
        String id = extendedRecord.getId();

        // Interpreting location
        InterpreterHandler.of(extendedRecord, new LocationRecord())
            .withId(id)
            .using(interpretId())
            .using(interpretCountryAndCoordinates(wsConfig))
            .using(interpretContinent())
            .using(interpretWaterBody())
            .using(interpretStateProvince())
            .using(interpretMinimumElevationInMeters())
            .using(interpretMaximumElevationInMeters())
            .using(interpretMinimumDepthInMeters())
            .using(interpretMaximumDepthInMeters())
            .using(interpretMinimumDistanceAboveSurfaceInMeters())
            .using(interpretMaximumDistanceAboveSurfaceInMeters())
            .using(interpretCoordinatePrecision())
            .using(interpretCoordinateUncertaintyInMeters())
            .consumeData(d -> context.output(getDataTag(), KV.of(id, d)))
            .consumeIssue(i -> context.output(getIssueTag(), KV.of(id, i)));
      }
    };
  }

  @Override
  public LocationRecordTransform withAvroCoders(Pipeline pipeline) {
    Coders.registerAvroCoders(
        pipeline, OccurrenceIssue.class, LocationRecord.class, ExtendedRecord.class);
    return this;
  }
}
