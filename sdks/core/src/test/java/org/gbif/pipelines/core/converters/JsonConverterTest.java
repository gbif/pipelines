package org.gbif.pipelines.core.converters;

import java.util.Collections;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.junit.Assert;
import org.junit.Test;

public class JsonConverterTest {

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

    MetadataRecord metadataRecord =
        MetadataRecord.newBuilder()
            .setId("777")
            .setNetworkKeys(Collections.singletonList("NK1"))
            .build();

    String expected =
        "{\"id\":\"777\",\"coreRowType\":\"core\",\"coreTerms\":\"{}\",\"extensions\":\"{}\",\"year\":2000,"
            + "\"day\":1,\"eventDate\":{\"gte\":\"01-01-2011\",\"lte\":\"01-01-2018\"},\"startDayOfYear\":1,"
            + "\"issues\":{},\"country\":\"Country\",\"countryCode\":\"Code 1'2\\\"\",\"decimalLatitude\":1.0,"
            + "\"decimalLongitude\":2.0,\"networkKeys\":[\"NK1\"]}";

    // When
    String result =
        JsonConverter.builder()
            .record(extendedRecord)
            .record(temporalRecord)
            .record(locationRecord)
            .record(metadataRecord)
            .build()
            .toString();

    // Should
    Assert.assertTrue(JsonValidationUtils.isValid(result));
    Assert.assertEquals(expected, result);
  }
}
