package org.gbif.pipelines.indexing.converter;

import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.location.LocationRecord;
import org.gbif.pipelines.io.avro.temporal.EventDate;
import org.gbif.pipelines.io.avro.temporal.TemporalRecord;
import org.gbif.pipelines.util.JsonValidationUtils;

import org.junit.Assert;
import org.junit.Test;

public class Records2JsonConverterTest {

  @Test
  public void createSimpleJsonFromSpecificRecordBase() {

    // State
    ExtendedRecord extendedRecord =
        ExtendedRecord.newBuilder().setId("777").setCoreRowType("core").build();

    TemporalRecord temporalRecord =
        TemporalRecord.newBuilder()
            .setId("777")
            .setEventDate(EventDate.newBuilder().setLte("01-01-2018").setGte("01-01-2011").build())
            .setDay(1)
            .setYear(2000)
            .setStartDayOfYear(1)
            .build();

    LocationRecord locationRecord =
        LocationRecord.newBuilder()
            .setId("777")
            .setCountry("Country")
            .setCountryCode("Code 1'2\"")
            .setDecimalLatitude(1d)
            .setDecimalLongitude(2d)
            .build();

    String expected =
        "{\"id\":\"777\",\"coreRowType\":\"core\",\"coreTerms\":{},\"extensions\":{},\"id\":\"777\",\"year\":\"2000\","
        + "\"month\":null,\"day\":\"1\",\"eventDate\":{\"gte\": \"01-01-2011\", \"lte\": \"01-01-2018\"},\"startDayOfYear\":\"1\","
        + "\"endDayOfYear\":null,\"modified\":null,\"dateIdentified\":null,\"id\":\"777\",\"continent\":null,\"waterBody\":null,"
        + "\"country\":\"Country\",\"countryCode\":\"Code 1'2\\\",\",\"stateProvince\":null,\"minimumElevationInMeters\":null,"
        + "\"maximumElevationInMeters\":null,\"minimumDepthInMeters\":null,\"maximumDepthInMeters\":null,"
        + "\"minimumDistanceAboveSurfaceInMeters\":null,\"maximumDistanceAboveSurfaceInMeters\":null,\"decimalLatitude\":\"1.0\","
        + "\"decimalLongitude\":\"2.0\",\"coordinateUncertaintyInMeters\":null,\"coordinatePrecision\":null}";

    // When
    String result =
        Records2JsonConverter.create(extendedRecord, temporalRecord, locationRecord).buildJson();

    // Should
    Assert.assertTrue(JsonValidationUtils.isValid(result));
    Assert.assertEquals(expected, result);
  }
}
