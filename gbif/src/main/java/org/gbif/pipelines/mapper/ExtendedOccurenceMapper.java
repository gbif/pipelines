package org.gbif.pipelines.mapper;

import org.gbif.dwca.avro.Event;
import org.gbif.dwca.avro.ExtendedOccurence;
import org.gbif.dwca.avro.Location;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

public class ExtendedOccurenceMapper {

  private ExtendedOccurenceMapper() {
    //Can't have an instance
  }

  //TODO: Fill all fields
  public static ExtendedOccurence map(InterpretedExtendedRecord record, Location location, Event event, TemporalRecord temporal) {
    return ExtendedOccurence.newBuilder()
      .setOccurrenceID(record.getId())
      .setBasisOfRecord(event.getBasisOfRecord())
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
