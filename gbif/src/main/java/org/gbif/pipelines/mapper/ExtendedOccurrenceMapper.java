package org.gbif.pipelines.mapper;

import org.gbif.dwca.avro.ExtendedOccurrence;
import org.gbif.dwca.avro.Location;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

public class ExtendedOccurrenceMapper {

  private ExtendedOccurrenceMapper() {
    //Can't have an instance
  }

  //TODO: Fill all fields
  public static ExtendedOccurrence map(InterpretedExtendedRecord record, Location location, TemporalRecord temporal) {
    return ExtendedOccurrence.newBuilder()
      .setOccurrenceID(record.getId())
      .setDay(temporal.getDay())
      .setMonth(temporal.getMonth())
      .setYear(temporal.getYear())
      .setEventDate(temporal.getEventDate())
      .setDecimalLatitude(location.getDecimalLatitude())
      .setDecimalLongitude(location.getDecimalLongitude())
      .setCountry(location.getCountry())
      .setCountryCode(location.getCountryCode())
      .setContinent(location.getContinent())
      .build();
  }

}
