package org.gbif.pipelines.transform;

import org.gbif.dwca.avro.Location;
import org.gbif.pipelines.core.functions.interpretation.error.IssueLineageRecord;
import org.gbif.pipelines.interpretation.Interpretation;
import org.gbif.pipelines.interpretation.LocationInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.mapper.LocationMapper;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocationTransform extends RecordTransform<ExtendedRecord, Location> {

  private static final Logger LOG = LoggerFactory.getLogger(LocationTransform.class);

  public LocationTransform() {
    super("Interpret loction record");
  }

  @Override
  DoFn<ExtendedRecord, KV<String, Location>> interpret() {
    return new DoFn<ExtendedRecord, KV<String, Location>>() {
      @ProcessElement
      public void processElement(ProcessContext context) {

        ExtendedRecord record = context.element();
        Location location = LocationMapper.map(record);

        // Interpreting Country and Country code
        final IssueLineageRecord issueLineageRecord = Interpretation.of(record)
          .using(LocationInterpreter.interpretCountry(location))
          .using(LocationInterpreter.interpretCountryCode(location))
          .using(LocationInterpreter.interpretContinent(location))
          .getIssueLineageRecord(record.getId());

        LOG.debug("Raw records converted to spatial category reporting issues and lineages");

        // Additional output
        context.output(getIssueTag(), KV.of(location.getOccurrenceID(), issueLineageRecord));

        // Main output
        context.output(getDataTag(), KV.of(location.getOccurrenceID(), location));
      }
    };
  }

}
