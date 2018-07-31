package org.gbif.pipelines.indexing.converter;

import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.location.LocationRecord;
import org.gbif.pipelines.io.avro.temporal.EventDate;
import org.gbif.pipelines.io.avro.temporal.TemporalRecord;
import org.gbif.pipelines.util.JsonValidationUtils;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

public class GbifRecords2JsonConverterTest {

  @Test
  public void testJsonFromSpecificRecordBase() {

    // State
    ExtendedRecord extendedRecord =
        ExtendedRecord.newBuilder()
            .setId("777")
            .setCoreRowType("core")
            .setCoreTerms(
                Collections.singletonMap(
                    "http://rs.tdwg.org/dwc/terms/locality", "something:{something}"))
            .build();

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
            .setContinent("something{something}")
            .build();

    String expected =
        "{\"id\":\"777\",\"verbatim\":{\"locality\":\"something:{something}\"},\"year\":\"2000\",\"day\":\"1\","
            + "\"eventDate\":{\"gte\": \"01-01-2011\", \"lte\": \"01-01-2018\"},\"startDayOfYear\":\"1\",\"location\":{\"lon\":\"2.0\","
            + "\"lat\":\"1.0\"},\"continent\":\"something{something}\",\"country\":\"Country\",\"countryCode\":\"Code 1'2\\\"\"}";

    // When
    String result =
        GbifRecords2JsonConverter.create(extendedRecord, temporalRecord, locationRecord)
            .buildJson();

    // Should
    Assert.assertTrue(JsonValidationUtils.isValid(result));
    Assert.assertEquals(expected, result);
  }

  @Test
  public void testOnlyOneIdInJson() {

    // State
    ExtendedRecord extendedRecord = ExtendedRecord.newBuilder().setId("777").build();

    TemporalRecord temporalRecord = TemporalRecord.newBuilder().setId("777").build();

    // When
    String result = GbifRecords2JsonConverter.create(extendedRecord, temporalRecord).buildJson();

    // Should
    String[] ids = result.split("id");
    Assert.assertTrue(JsonValidationUtils.isValid(result));
    Assert.assertEquals(2, ids.length);
  }
}
