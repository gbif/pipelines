package org.gbif.pipelines.labs.mapper;

import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.ExtendedOccurrence;
import org.gbif.pipelines.io.avro.ExtendedOccurrence.Builder;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.location.LocationRecord;
import org.gbif.pipelines.io.avro.multimedia.MultimediaRecord;
import org.gbif.pipelines.io.avro.taxon.TaxonRecord;
import org.gbif.pipelines.io.avro.temporal.TemporalRecord;

import java.util.Optional;

public class ExtendedOccurrenceMapper {

  private ExtendedOccurrenceMapper() {
    // Can't have an instance
  }

  // TODO: Fill all fields
  public static ExtendedOccurrence map(InterpretedExtendedRecord record, LocationRecord location, TemporalRecord temporal,
      TaxonRecord taxon, MultimediaRecord multimedia) {

    ExtendedOccurrence.Builder builder = ExtendedOccurrence.newBuilder();

    mapCommon(builder, record);
    mapLocation(builder, location);
    mapMultimedia(builder, multimedia);
    mapTaxon(builder, taxon);
    mapTemporal(builder, temporal);

    return builder.build();
  }

  private static void mapTemporal(Builder builder, TemporalRecord temporal){
    builder.setEventID(temporal.getEventID())
      .setParentEventID(temporal.getParentEventID())
      .setFieldNumber(temporal.getFieldNumber())
      .setVerbatimEventDate(temporal.getVerbatimEventDate())
      .setYear(temporal.getYear())
      .setMonth(temporal.getMonth())
      .setDay(temporal.getDay())
      .setStartDayOfYear(temporal.getStartDayOfYear())
      .setEndDayOfYear(temporal.getEndDayOfYear())
      .setEventDate(mapEventDate(temporal.getEventDate()))
      .setEventTime(temporal.getEventTime())
      .setFieldNotes(temporal.getFieldNotes())
      .setEventRemarks(temporal.getEventRemarks())
      .setHabitat(temporal.getHabitat())
      .setSamplingProtocol(temporal.getSamplingProtocol())
      .setSampleSizeValue(temporal.getSampleSizeValue())
      .setSampleSizeUnit(temporal.getSampleSizeUnit())
      .setSamplingEffort(temporal.getSamplingEffort())
      .setModified(temporal.getModified())
      .setDateIdentified(temporal.getDateIdentified());
  }

  private static void mapLocation(Builder builder, LocationRecord location){
    builder.setDecimalLatitude(location.getDecimalLatitude())
      .setDecimalLongitude(location.getDecimalLongitude())
      .setMinimumElevationInMeters(location.getMinimumElevationInMeters())
      .setMaximumElevationInMeters(location.getMaximumElevationInMeters())
      .setMinimumDepthInMeters(location.getMinimumDepthInMeters())
      .setMaximumDepthInMeters(location.getMaximumDepthInMeters())
      .setMinimumDistanceAboveSurfaceInMeters(location.getMinimumDistanceAboveSurfaceInMeters())
      .setMaximumDistanceAboveSurfaceInMeters(location.getMaximumDistanceAboveSurfaceInMeters())
      .setCoordinateUncertaintyInMeters(location.getCoordinateUncertaintyInMeters())
      .setCoordinatePrecision(location.getCoordinatePrecision())
      .setLocationID(location.getLocationID())
      .setHigherGeographyID(location.getHigherGeographyID())
      .setHigherGeography(location.getHigherGeography())
      .setIslandGroup(location.getIslandGroup())
      .setIsland(location.getIsland())
      .setCounty(location.getCounty())
      .setMunicipality(location.getMunicipality())
      .setLocality(location.getLocality())
      .setVerbatimLocality(location.getVerbatimLocality())
      .setVerbatimElevation(location.getVerbatimElevation())
      .setLocationAccordingTo(location.getLocationAccordingTo())
      .setLocationRemarks(location.getLocationRemarks())
      .setGeodeticDatum(location.getGeodeticDatum())
      .setVerbatimCoordinates(location.getVerbatimCoordinates())
      .setVerbatimLatitude(location.getVerbatimLatitude())
      .setVerbatimLongitude(location.getVerbatimLongitude())
      .setVerbatimCoordinateSystem(location.getVerbatimCoordinateSystem())
      .setVerbatimSRS(location.getVerbatimSRS())
      .setFootprintWKT(location.getFootprintWKT())
      .setFootprintSRS(location.getFootprintSRS())
      .setFootprintSpatialFit(location.getFootprintSpatialFit())
      .setGeoreferencedBy(location.getGeoreferencedBy())
      .setGeoreferencedDate(location.getGeoreferencedDate())
      .setGeoreferenceProtocol(location.getGeoreferenceProtocol())
      .setGeoreferenceSources(location.getGeoreferenceSources())
      .setGeoreferenceVerificationStatus(location.getGeoreferenceVerificationStatus())
      .setPointRadiusSpatialFit(location.getPointRadiusSpatialFit())
      .setContinent(location.getContinent())
      .setWaterBody(location.getWaterBody())
      .setCountry(location.getCountry())
      .setCountryCode(location.getCountryCode())
      .setStateProvince(location.getStateProvince())
      .setVerbatimDepth(location.getVerbatimDepth());
  }

  private static void mapCommon(Builder builder, InterpretedExtendedRecord record){
    builder.setOccurrenceID(record.getId())
      .setTypeStatus(record.getTypeStatus())
      .setEstablishmentMeans(record.getEstablishmentMeans())
      .setLifeStage(record.getLifeStage())
      .setSex(record.getSex())
      .setBasisOfRecord(record.getBasisOfRecord());

      Optional.ofNullable(record.getIndividualCount()).ifPresent(x->builder.setIndividualCount(x.toString()));

  }

  private static void mapTaxon(Builder builder, TaxonRecord taxon){
    // TODO: PARSE FIELDS
  }

  private static void mapMultimedia(Builder builder, MultimediaRecord multimedia){
    // TODO: NO FIELDS
  }

  private static EventDate mapEventDate(org.gbif.pipelines.io.avro.temporal.EventDate eventDate){
    return EventDate.newBuilder()
      .setGte(eventDate.getGte())
      .setLte(eventDate.getLte())
      .build();
  }

}
