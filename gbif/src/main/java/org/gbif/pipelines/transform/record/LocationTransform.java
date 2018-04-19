package org.gbif.pipelines.transform.record;

import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.core.interpretation.Interpretation;
import org.gbif.pipelines.core.interpretation.LocationInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.Location;
import org.gbif.pipelines.io.avro.OccurrenceIssue;
import org.gbif.pipelines.io.avro.Validation;
import org.gbif.pipelines.mapper.LocationMapper;
import org.gbif.pipelines.transform.RecordTransform;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class LocationTransform extends RecordTransform<ExtendedRecord, Location> {

  private LocationTransform() {
    super("Interpret location record");
  }

  public static LocationTransform create() {
    return new LocationTransform();
  }

  @Override
  public DoFn<ExtendedRecord, KV<String, Location>> interpret() {
    return new DoFn<ExtendedRecord, KV<String, Location>>() {
      @ProcessElement
      public void processElement(ProcessContext context) {

        ExtendedRecord extendedRecord = context.element();
        Location location = LocationMapper.map(extendedRecord);
        String id = extendedRecord.getId();
        List<Validation> validations = new ArrayList<>();

        // read the ws properties path from the PipelineOptions
        String wsPropertiesPath =
          context.getPipelineOptions().as(DataProcessingPipelineOptions.class).getWsProperties();

        // Interpreting Country and Country code
        Interpretation.of(extendedRecord)
          .using(LocationInterpreter.interpretCountryAndCoordinates(location, wsPropertiesPath))
          .using(LocationInterpreter.interpretContinent(location))
          .using(LocationInterpreter.interpretWaterBody(location))
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
  public LocationTransform withAvroCoders(Pipeline pipeline) {
    Coders.registerAvroCoders(pipeline, OccurrenceIssue.class, Location.class, ExtendedRecord.class);
    return this;
  }

}
