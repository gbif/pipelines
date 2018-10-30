package org.gbif.pipelines.core.converters;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.Rank;
import org.gbif.pipelines.io.avro.RankedName;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
    temporalRecord.getIssues().getIssueList().add(OccurrenceIssue.ZERO_COORDINATE.name());

    LocationRecord locationRecord =
        LocationRecord.newBuilder()
            .setId("777")
            .setCountry("Country")
            .setCountryCode("Code 1'2\"")
            .setDecimalLatitude(1d)
            .setDecimalLongitude(2d)
            .setContinent("something{something}")
            .build();
    locationRecord.getIssues().getIssueList().add(OccurrenceIssue.BASIS_OF_RECORD_INVALID.name());

    List<RankedName> rankedNameList = new ArrayList<>();
    RankedName name =
        RankedName.newBuilder().setKey(1).setName("Name").setRank(Rank.CHEMOFORM).build();
    RankedName name2 =
        RankedName.newBuilder().setKey(2).setName("Name2").setRank(Rank.ABERRATION).build();
    rankedNameList.add(name);
    rankedNameList.add(name2);

    TaxonRecord taxonRecord =
        TaxonRecord.newBuilder().setId("777").setClassification(rankedNameList).build();

    String expected =
        "{\"verbatim\":{\"locality\":\"something:{something}\"},\"startDate\":\"01-01-2011\",\"year\":\"2000\",\"day\":\"1\","
            + "\"eventDate\":{\"gte\": \"01-01-2011\", \"lte\": \"01-01-2018\"},\"startDayOfYear\":\"1\",\"issues\":[\"BASIS_OF_RECORD_INVALID\","
            + "\"ZERO_COORDINATE\"],\"coordinatePoints\":{\"lon\":\"2.0\",\"lat\":\"1.0\"},\"continent\":\"something{something}\","
            + "\"countryCode\":\"Code 1'2\\\"\",\"backbone\":[{\"taxonKey\":1,\"name\":\"Name\",\"depthKey_0\":1,\"kingdomKey\":1,"
            + "\"rank\":\"CHEMOFORM\"},{\"taxonKey\":2,\"name\":\"Name2\",\"depthKey_1\":2,\"kingdomKey\":2,\"rank\":\"ABERRATION\"}],"
            + "\"notIssues\":[\"COORDINATE_PRECISION_UNCERTAINTY_MISMATCH\",\"MODIFIED_DATE_INVALID\",\"CONTINENT_COUNTRY_MISMATCH\","
            + "\"COORDINATE_INVALID\",\"COORDINATE_PRECISION_INVALID\",\"ELEVATION_NON_NUMERIC\",\"COORDINATE_OUT_OF_RANGE\",\"COUNTRY_INVALID\","
            + "\"ELEVATION_NOT_METRIC\",\"COORDINATE_REPROJECTION_SUSPICIOUS\",\"PRESUMED_NEGATED_LONGITUDE\",\"DEPTH_UNLIKELY\","
            + "\"IDENTIFIED_DATE_INVALID\",\"ELEVATION_MIN_MAX_SWAPPED\",\"TAXON_MATCH_NONE\",\"TYPE_STATUS_INVALID\",\"TAXON_MATCH_FUZZY\","
            + "\"CONTINENT_INVALID\",\"GEODETIC_DATUM_INVALID\",\"MODIFIED_DATE_UNLIKELY\",\"COORDINATE_REPROJECTED\",\"PRESUMED_SWAPPED_COORDINATE\","
            + "\"REFERENCES_URI_INVALID\",\"COORDINATE_ROUNDED\",\"IDENTIFIED_DATE_UNLIKELY\",\"COUNTRY_COORDINATE_MISMATCH\",\"DEPTH_NON_NUMERIC\","
            + "\"COUNTRY_DERIVED_FROM_COORDINATES\",\"COORDINATE_REPROJECTION_FAILED\",\"COORDINATE_UNCERTAINTY_METERS_INVALID\","
            + "\"PRESUMED_NEGATED_LATITUDE\",\"MULTIMEDIA_URI_INVALID\",\"COORDINATE_ACCURACY_INVALID\",\"GEODETIC_DATUM_ASSUMED_WGS84\","
            + "\"TAXON_MATCH_HIGHERRANK\",\"ELEVATION_UNLIKELY\",\"CONTINENT_DERIVED_FROM_COORDINATES\",\"DEPTH_MIN_MAX_SWAPPED\","
            + "\"RECORDED_DATE_INVALID\",\"INDIVIDUAL_COUNT_INVALID\",\"RECORDED_DATE_MISMATCH\",\"DEPTH_NOT_METRIC\",\"MULTIMEDIA_DATE_INVALID\","
            + "\"INTERPRETATION_ERROR\",\"RECORDED_DATE_UNLIKELY\",\"COUNTRY_MISMATCH\"]}";

    // When
    String result =
        GbifJsonConverter.create(extendedRecord, temporalRecord, locationRecord, taxonRecord)
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
    Assert.assertTrue(JsonValidationUtils.isValid(result));
  }
}
