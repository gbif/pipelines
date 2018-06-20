package org.gbif.pipelines.transform.record;

import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.core.interpretation.Interpretation;
import org.gbif.pipelines.core.interpretation.LocationInterpreter;
import org.gbif.pipelines.core.ws.client.geocode.GeocodeServiceRest;
import org.gbif.pipelines.core.ws.config.Config;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.issue.OccurrenceIssue;
import org.gbif.pipelines.io.avro.issue.Validation;
import org.gbif.pipelines.io.avro.location.LocationRecord;
import org.gbif.pipelines.transform.RecordTransform;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocationRecordTransform extends RecordTransform<ExtendedRecord, LocationRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(LocationRecordTransform.class);

  private final Config wsConfig;

  private LocationRecordTransform(Config wsConfig) {
    super("Interpret location record");
    this.wsConfig = wsConfig;

    // add hook to delete ws cache at shutdown
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      LOG.info("Executing location shutdown hook");
      GeocodeServiceRest.clearCache();
    }));
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
        List<Validation> validations = new ArrayList<>();

        // Transformation main output
        LocationRecord location = LocationRecord.newBuilder().setId(id).build();

        // Interpreting Country and Country code
        Interpretation.of(extendedRecord)
          .using(LocationInterpreter.interpretCountryAndCoordinates(location, wsConfig))
          .using(LocationInterpreter.interpretContinent(location))
          .using(LocationInterpreter.interpretWaterBody(location))
          .using(LocationInterpreter.interpretStateProvince(location))
          .using(LocationInterpreter.interpretMinimumElevationInMeters(location))
          .using(LocationInterpreter.interpretMaximumElevationInMeters(location))
          .using(LocationInterpreter.interpretMinimumDepthInMeters(location))
          .using(LocationInterpreter.interpretMaximumDepthInMeters(location))
          .using(LocationInterpreter.interpretMinimumDistanceAboveSurfaceInMeters(location))
          .using(LocationInterpreter.interpretMaximumDistanceAboveSurfaceInMeters(location))
          .using(LocationInterpreter.interpretCoordinatePrecision(location))
          .using(LocationInterpreter.interpretCoordinateUncertaintyInMeters(location))
          .forEachValidation(trace -> validations.add(toValidation(trace.getContext())));

        //additional outputs
        if (!validations.isEmpty()) {
          OccurrenceIssue issue = OccurrenceIssue.newBuilder().setId(id).setIssues(validations).build();
          context.output(getIssueTag(), KV.of(id, issue));
        }

        // Main output
        context.output(getDataTag(), KV.of(id, location));
      }
    };
  }

  @Override
  public LocationRecordTransform withAvroCoders(Pipeline pipeline) {
    Coders.registerAvroCoders(pipeline, OccurrenceIssue.class, LocationRecord.class, ExtendedRecord.class);
    return this;
  }

}
