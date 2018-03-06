package org.gbif.pipelines.labs.mapper;

import org.gbif.pipelines.io.avro.ExtendedOccurrence;
import org.gbif.pipelines.io.avro.Location;
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
      .setTypeStatus(record.getTypeStatus())
      .setEstablishmentMeans(record.getEstablishmentMeans())
      .setLifeStage(record.getLifeStage())
      .setSex(record.getSex())
      .setBasisOfRecord(record.getBasisOfRecord())
      .setIndividualCount(record.getIndividualCount().toString())
      .setDay(temporal.getDay())
      .setMonth(temporal.getMonth())
      .setYear(temporal.getYear())
      .setEventDate(temporal.getEventDate())
      .setDecimalLatitude(location.getDecimalLatitude())
      .setDecimalLongitude(location.getDecimalLongitude())
      .setCountry(location.getCountry())
      .setCountryCode(location.getCountryCode())
      .setContinent(location.getContinent())
      .setWaterBody(location.getWaterBody())
      .setMinimumElevationInMeters(location.getMinimumElevationInMeters())
      .setMaximumElevationInMeters(location.getMaximumElevationInMeters())
      .setMinimumDepthInMeters(location.getMinimumDepthInMeters())
      .setMaximumDepthInMeters(location.getMaximumDepthInMeters())
      .build();
  }

}
