package org.gbif.pipelines.transform.record;

import org.gbif.dwca.avro.Location;
import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.interpretation.Interpretation;
import org.gbif.pipelines.interpretation.LocationInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.OccurrenceIssue;
import org.gbif.pipelines.io.avro.Validation;
import org.gbif.pipelines.mapper.LocationMapper;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class LocationTransform extends RecordTransform<ExtendedRecord, Location> {

  public LocationTransform() {
    super("Interpret loction record");
  }

  @Override
  DoFn<ExtendedRecord, KV<String, Location>> interpret() {
    return new DoFn<ExtendedRecord, KV<String, Location>>() {
      @ProcessElement
      public void processElement(ProcessContext context) {

        ExtendedRecord extendedRecord = context.element();
        Location location = LocationMapper.map(extendedRecord);
        String id = extendedRecord.getId();
        List<Validation> validations = new ArrayList<>();

        // Interpreting Country and Country code
        Interpretation.of(extendedRecord)
          .using(LocationInterpreter.interpretCountry(location))
          .using(LocationInterpreter.interpretCountryCode(location))
          .using(LocationInterpreter.interpretContinent(location))
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
