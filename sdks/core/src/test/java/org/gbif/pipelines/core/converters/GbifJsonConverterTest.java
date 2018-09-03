package org.gbif.pipelines.core.converters;

import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

import java.util.Collections;
import java.util.List;

import com.google.common.base.Splitter;
import org.junit.Assert;
import org.junit.Test;

public class GbifJsonConverterTest {

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
    temporalRecord.getIssues().getIssueList().add("ISSUE_1");

    LocationRecord locationRecord =
        LocationRecord.newBuilder()
            .setId("777")
            .setCountry("Country")
            .setCountryCode("Code 1'2\"")
            .setDecimalLatitude(1d)
            .setDecimalLongitude(2d)
            .setContinent("something{something}")
            .build();
    locationRecord.getIssues().getIssueList().add("ISSUE_2");

    String expected =
        "{\"id\":\"777\",\"verbatim\":{\"locality\":\"something:{something}\"},\"year\":\"2000\",\"day\":\"1\","
            + "\"eventDate\":{\"gte\": \"01-01-2011\", \"lte\": \"01-01-2018\"},\"startDayOfYear\":\"1\","
            + "\"issues\":[\"ISSUE_1\",\"ISSUE_2\"],\"location\":{\"lon\":\"2.0\",\"lat\":\"1.0\"},\"continent\":\"something{something}\","
            + "\"country\":\"Country\",\"countryCode\":\"Code 1'2\\\"\"}";

    // When
    String result =
        GbifJsonConverter.create(extendedRecord, temporalRecord, locationRecord)
            .buildJson()
            .toString();

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
    String result = GbifJsonConverter.create(extendedRecord, temporalRecord).buildJson().toString();

    // Should
    List<String> ids = Splitter.on("id").splitToList(result);
    Assert.assertTrue(JsonValidationUtils.isValid(result));
    Assert.assertEquals(2, ids.size());
  }
}
