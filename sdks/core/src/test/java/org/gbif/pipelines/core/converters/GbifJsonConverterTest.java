package org.gbif.pipelines.core.converters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.pipelines.io.avro.Amplification;
import org.gbif.pipelines.io.avro.AmplificationRecord;
import org.gbif.pipelines.io.avro.AustraliaSpatialRecord;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.Rank;
import org.gbif.pipelines.io.avro.RankedName;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

import org.junit.Assert;
import org.junit.Test;

public class GbifJsonConverterTest {

  @Test
  public void jsonFromSpecificRecordBaseTest() {

    // Expected
    String expected =
        "{\"id\":\"777\",\"verbatim\":{\"remark\":\"{\\\"something\\\":1}{\\\"something\\\":1}\",\"locality\":\"something:{something}\"},"
            + "\"startDate\":\"01-01-2011\",\"year\":\"2000\",\"day\":\"1\",\"eventDate\":{\"gte\": \"01-01-2011\", \"lte\": \"01-01-2018\"},"
            + "\"startDayOfYear\":\"1\",\"issues\":[\"BASIS_OF_RECORD_INVALID\",\"ZERO_COORDINATE\"],\"coordinates\":{\"lon\":\"2.0\","
            + "\"lat\":\"1.0\"},\"continent\":\"something{something}\",\"country\":\"Country\",\"countryCode\":\"Code 1'2\\\"\",\"backbone\":[{\"taxonKey\":1,"
            + "\"name\":\"Name\",\"depthKey_0\":1,\"kingdomKey\":1,\"rank\":\"CHEMOFORM\"},{\"taxonKey\":2,\"name\":\"Name2\","
            + "\"depthKey_1\":2,\"kingdomKey\":2,\"rank\":\"ABERRATION\"}],\"notIssues\":[\"COORDINATE_PRECISION_UNCERTAINTY_MISMATCH\","
            + "\"MODIFIED_DATE_INVALID\",\"CONTINENT_COUNTRY_MISMATCH\",\"COORDINATE_INVALID\",\"COORDINATE_PRECISION_INVALID\","
            + "\"ELEVATION_NON_NUMERIC\",\"COORDINATE_OUT_OF_RANGE\",\"COUNTRY_INVALID\",\"ELEVATION_NOT_METRIC\",\"COORDINATE_REPROJECTION_SUSPICIOUS\","
            + "\"PRESUMED_NEGATED_LONGITUDE\",\"DEPTH_UNLIKELY\",\"IDENTIFIED_DATE_INVALID\",\"ELEVATION_MIN_MAX_SWAPPED\",\"TAXON_MATCH_NONE\","
            + "\"TYPE_STATUS_INVALID\",\"TAXON_MATCH_FUZZY\",\"CONTINENT_INVALID\",\"GEODETIC_DATUM_INVALID\",\"MODIFIED_DATE_UNLIKELY\","
            + "\"COORDINATE_REPROJECTED\",\"PRESUMED_SWAPPED_COORDINATE\",\"REFERENCES_URI_INVALID\",\"COORDINATE_ROUNDED\","
            + "\"IDENTIFIED_DATE_UNLIKELY\",\"COUNTRY_COORDINATE_MISMATCH\",\"DEPTH_NON_NUMERIC\",\"COUNTRY_DERIVED_FROM_COORDINATES\","
            + "\"COORDINATE_REPROJECTION_FAILED\",\"COORDINATE_UNCERTAINTY_METERS_INVALID\",\"PRESUMED_NEGATED_LATITUDE\",\"MULTIMEDIA_URI_INVALID\","
            + "\"COORDINATE_ACCURACY_INVALID\",\"GEODETIC_DATUM_ASSUMED_WGS84\",\"TAXON_MATCH_HIGHERRANK\",\"ELEVATION_UNLIKELY\","
            + "\"CONTINENT_DERIVED_FROM_COORDINATES\",\"DEPTH_MIN_MAX_SWAPPED\",\"RECORDED_DATE_INVALID\",\"INDIVIDUAL_COUNT_INVALID\","
            + "\"RECORDED_DATE_MISMATCH\",\"DEPTH_NOT_METRIC\",\"MULTIMEDIA_DATE_INVALID\",\"INTERPRETATION_ERROR\",\"RECORDED_DATE_UNLIKELY\","
            + "\"COUNTRY_MISMATCH\"]}";

    // State
    Map<String, String> erMap = new HashMap<>(2);
    erMap.put("http://rs.tdwg.org/dwc/terms/locality", "something:{something}");
    erMap.put("http://rs.tdwg.org/dwc/terms/remark", "{\"something\":1}{\"something\":1}");

    ExtendedRecord er =
        ExtendedRecord.newBuilder()
            .setId("777")
            .setCoreRowType("core")
            .setCoreTerms(erMap)
            .build();

    TemporalRecord tmr =
        TemporalRecord.newBuilder()
            .setId("777")
            .setEventDate(EventDate.newBuilder().setLte("01-01-2018").setGte("01-01-2011").build())
            .setDay(1)
            .setYear(2000)
            .setStartDayOfYear(1)
            .build();
    tmr.getIssues().getIssueList().add(OccurrenceIssue.ZERO_COORDINATE.name());

    LocationRecord lr =
        LocationRecord.newBuilder()
            .setId("777")
            .setCountry("Country")
            .setCountryCode("Code 1'2\"")
            .setDecimalLatitude(1d)
            .setDecimalLongitude(2d)
            .setContinent("something{something}")
            .build();
    lr.getIssues().getIssueList().add(OccurrenceIssue.BASIS_OF_RECORD_INVALID.name());

    List<RankedName> rankedNameList = new ArrayList<>();
    RankedName name = RankedName.newBuilder().setKey(1).setName("Name").setRank(Rank.CHEMOFORM).build();
    RankedName name2 = RankedName.newBuilder().setKey(2).setName("Name2").setRank(Rank.ABERRATION).build();
    rankedNameList.add(name);
    rankedNameList.add(name2);

    TaxonRecord tr = TaxonRecord.newBuilder().setId("777").setClassification(rankedNameList).build();

    // When
    String result = GbifJsonConverter.toStringJson(er, tmr, lr, tr);

    // Should
    Assert.assertTrue(JsonValidationUtils.isValid(result));
    Assert.assertEquals(expected, result);
  }

  @Test
  public void jsonFromSpecificRecordBaseAustraliaTest() {

    // Expected
    String expected =
        "{\"id\":\"777\",\"verbatim\":{\"remark\":\"{\\\"something\\\":1}{\\\"something\\\":1}\",\"locality\":\"something:{something}\"},"
            + "\"startDate\":\"01-01-2011\",\"year\":\"2000\",\"day\":\"1\",\"eventDate\":{\"gte\": \"01-01-2011\", \"lte\": \"01-01-2018\"},"
            + "\"startDayOfYear\":\"1\",\"issues\":[\"BASIS_OF_RECORD_INVALID\",\"ZERO_COORDINATE\"],\"coordinates\":{\"lon\":\"2.0\",\"lat\":\"1.0\"},"
            + "\"continent\":\"something{something}\",\"country\":\"Country\",\"countryCode\":\"Code 1'2\\\"\",\"backbone\":[{\"taxonKey\":1,\"name\":\"Name\",\"depthKey_0\":1,"
            + "\"kingdomKey\":1,\"rank\":\"CHEMOFORM\"},{\"taxonKey\":2,\"name\":\"Name2\",\"depthKey_1\":2,\"kingdomKey\":2,\"rank\":\"ABERRATION\"}],"
            + "\"australiaSpatialLayers\":[{\"key\":\"data\",\"value\":\"value\"}],\"notIssues\":[\"COORDINATE_PRECISION_UNCERTAINTY_MISMATCH\",\"MODIFIED_DATE_INVALID\","
            + "\"CONTINENT_COUNTRY_MISMATCH\",\"COORDINATE_INVALID\",\"COORDINATE_PRECISION_INVALID\",\"ELEVATION_NON_NUMERIC\",\"COORDINATE_OUT_OF_RANGE\","
            + "\"COUNTRY_INVALID\",\"ELEVATION_NOT_METRIC\",\"COORDINATE_REPROJECTION_SUSPICIOUS\",\"PRESUMED_NEGATED_LONGITUDE\",\"DEPTH_UNLIKELY\","
            + "\"IDENTIFIED_DATE_INVALID\",\"ELEVATION_MIN_MAX_SWAPPED\",\"TAXON_MATCH_NONE\",\"TYPE_STATUS_INVALID\",\"TAXON_MATCH_FUZZY\","
            + "\"CONTINENT_INVALID\",\"GEODETIC_DATUM_INVALID\",\"MODIFIED_DATE_UNLIKELY\",\"COORDINATE_REPROJECTED\",\"PRESUMED_SWAPPED_COORDINATE\","
            + "\"REFERENCES_URI_INVALID\",\"COORDINATE_ROUNDED\",\"IDENTIFIED_DATE_UNLIKELY\",\"COUNTRY_COORDINATE_MISMATCH\",\"DEPTH_NON_NUMERIC\","
            + "\"COUNTRY_DERIVED_FROM_COORDINATES\",\"COORDINATE_REPROJECTION_FAILED\",\"COORDINATE_UNCERTAINTY_METERS_INVALID\",\"PRESUMED_NEGATED_LATITUDE\","
            + "\"MULTIMEDIA_URI_INVALID\",\"COORDINATE_ACCURACY_INVALID\",\"GEODETIC_DATUM_ASSUMED_WGS84\",\"TAXON_MATCH_HIGHERRANK\",\"ELEVATION_UNLIKELY\","
            + "\"CONTINENT_DERIVED_FROM_COORDINATES\",\"DEPTH_MIN_MAX_SWAPPED\",\"RECORDED_DATE_INVALID\",\"INDIVIDUAL_COUNT_INVALID\","
            + "\"RECORDED_DATE_MISMATCH\",\"DEPTH_NOT_METRIC\",\"MULTIMEDIA_DATE_INVALID\",\"INTERPRETATION_ERROR\",\"RECORDED_DATE_UNLIKELY\","
            + "\"COUNTRY_MISMATCH\"]}";

    // State
    Map<String, String> erMap = new HashMap<>(2);
    erMap.put("http://rs.tdwg.org/dwc/terms/locality", "something:{something}");
    erMap.put("http://rs.tdwg.org/dwc/terms/remark", "{\"something\":1}{\"something\":1}");

    ExtendedRecord er =
        ExtendedRecord.newBuilder()
            .setId("777")
            .setCoreRowType("core")
            .setCoreTerms(erMap)
            .build();

    TemporalRecord tmr =
        TemporalRecord.newBuilder()
            .setId("777")
            .setEventDate(EventDate.newBuilder().setLte("01-01-2018").setGte("01-01-2011").build())
            .setDay(1)
            .setYear(2000)
            .setStartDayOfYear(1)
            .build();
    tmr.getIssues().getIssueList().add(OccurrenceIssue.ZERO_COORDINATE.name());

    LocationRecord lr =
        LocationRecord.newBuilder()
            .setId("777")
            .setCountry("Country")
            .setCountryCode("Code 1'2\"")
            .setDecimalLatitude(1d)
            .setDecimalLongitude(2d)
            .setContinent("something{something}")
            .build();
    lr.getIssues().getIssueList().add(OccurrenceIssue.BASIS_OF_RECORD_INVALID.name());

    AustraliaSpatialRecord asr =
        AustraliaSpatialRecord.newBuilder()
            .setId("777")
            .setItems(Collections.singletonMap("data", "value"))
            .build();

    List<RankedName> rankedNameList = new ArrayList<>();
    RankedName name = RankedName.newBuilder().setKey(1).setName("Name").setRank(Rank.CHEMOFORM).build();
    RankedName name2 = RankedName.newBuilder().setKey(2).setName("Name2").setRank(Rank.ABERRATION).build();
    rankedNameList.add(name);
    rankedNameList.add(name2);

    TaxonRecord tr = TaxonRecord.newBuilder().setId("777").setClassification(rankedNameList).build();

    // When
    String result = GbifJsonConverter.toStringJson(er, tmr, lr, tr, asr);

    // Should
    Assert.assertTrue(JsonValidationUtils.isValid(result));
    Assert.assertEquals(expected, result);
  }

  @Test
  public void onlyOneIdInJsonTest() {

    // Expected
    String expected =
        "{\"id\":\"777\",\"verbatim\":{},\"issues\":[],\"notIssues\":[\"COORDINATE_PRECISION_UNCERTAINTY_MISMATCH\","
            + "\"MODIFIED_DATE_INVALID\",\"CONTINENT_COUNTRY_MISMATCH\",\"COORDINATE_INVALID\",\"COORDINATE_PRECISION_INVALID\","
            + "\"ELEVATION_NON_NUMERIC\",\"COORDINATE_OUT_OF_RANGE\",\"COUNTRY_INVALID\",\"ELEVATION_NOT_METRIC\","
            + "\"COORDINATE_REPROJECTION_SUSPICIOUS\",\"PRESUMED_NEGATED_LONGITUDE\",\"DEPTH_UNLIKELY\",\"IDENTIFIED_DATE_INVALID\","
            + "\"ELEVATION_MIN_MAX_SWAPPED\",\"TAXON_MATCH_NONE\",\"BASIS_OF_RECORD_INVALID\",\"TYPE_STATUS_INVALID\",\"TAXON_MATCH_FUZZY\","
            + "\"CONTINENT_INVALID\",\"GEODETIC_DATUM_INVALID\",\"MODIFIED_DATE_UNLIKELY\",\"COORDINATE_REPROJECTED\","
            + "\"PRESUMED_SWAPPED_COORDINATE\",\"REFERENCES_URI_INVALID\",\"COORDINATE_ROUNDED\",\"IDENTIFIED_DATE_UNLIKELY\","
            + "\"COUNTRY_COORDINATE_MISMATCH\",\"DEPTH_NON_NUMERIC\",\"COUNTRY_DERIVED_FROM_COORDINATES\",\"COORDINATE_REPROJECTION_FAILED\","
            + "\"COORDINATE_UNCERTAINTY_METERS_INVALID\",\"PRESUMED_NEGATED_LATITUDE\",\"MULTIMEDIA_URI_INVALID\","
            + "\"COORDINATE_ACCURACY_INVALID\",\"GEODETIC_DATUM_ASSUMED_WGS84\",\"TAXON_MATCH_HIGHERRANK\",\"ELEVATION_UNLIKELY\","
            + "\"CONTINENT_DERIVED_FROM_COORDINATES\",\"DEPTH_MIN_MAX_SWAPPED\",\"RECORDED_DATE_INVALID\",\"INDIVIDUAL_COUNT_INVALID\","
            + "\"RECORDED_DATE_MISMATCH\",\"DEPTH_NOT_METRIC\",\"MULTIMEDIA_DATE_INVALID\",\"INTERPRETATION_ERROR\",\"ZERO_COORDINATE\","
            + "\"RECORDED_DATE_UNLIKELY\",\"COUNTRY_MISMATCH\"]}";

    // State
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("777").build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("777").build();

    // When
    String result = GbifJsonConverter.toStringJson(er, tr);

    // Should
    Assert.assertTrue(JsonValidationUtils.isValid(result));
    Assert.assertEquals(expected, result);
  }

  @Test
  public void avroBaseRecordSkipIssuesWithIdTest() {

    // Expected
    String expected =
        "{\"id\":\"777\",\"amplificationItems\":[{\"amplificationDate\": null, \"amplificationStaff\": null, \"amplificationSuccess\": null, "
            + "\"amplificationSuccessDetails\": null, \"amplificationMethod\": null, \"primerSequenceForward\": null, \"primerNameForward\": null, "
            + "\"primerReferenceCitationForward\": null, \"primerReferenceLinkForward\": null, \"primerSequenceReverse\": null, "
            + "\"primerNameReverse\": null, \"primerReferenceCitationReverse\": null, \"primerReferenceLinkReverse\": null, "
            + "\"purificationMethod\": null, \"consensusSequence\": \"DNADNADNA\", \"consensusSequenceLength\": null, "
            + "\"consensusSequenceChromatogramFileUri\": null, \"barcodeSequence\": null, \"haplotype\": null, \"marker\": \"IRS\", "
            + "\"markerSubfragment\": null, \"geneticAccessionNumber\": null, \"boldProcessId\": null, \"geneticAccessionUri\": null, "
            + "\"gcContent\": null, \"chimeraCheck\": null, \"assembly\": null, \"sop\": null, \"finishingStrategy\": null, "
            + "\"annotSource\": null, \"markerAccordance\": null, \"seqQualityCheck\": null, \"adapters\": null, \"mid\": null, "
            + "\"blastResult\": {\"name\": null, \"identity\": null, \"appliedScientificName\": null, \"matchType\": null, \"bitScore\": null, "
            + "\"expectValue\": null, \"querySequence\": null, \"subjectSequence\": null, \"qstart\": null, \"qend\": null, \"sstart\": null, "
            + "\"send\": null, \"distanceToBestMatch\": null, \"sequenceLength\": null}}]}";

    // State
    AmplificationRecord record = AmplificationRecord.newBuilder()
        .setId("777")
        .setAmplificationItems(Collections.singletonList(
            Amplification.newBuilder().setMarker("IRS").setConsensusSequence("DNADNADNA").build()))
        .build();

    // When
    String result = GbifJsonConverter.toStringPartialJson(record);

    // Should
    Assert.assertTrue(JsonValidationUtils.isValid(result));
    Assert.assertEquals(expected, result);
  }

  @Test
  public void taxonRecordUsageTest() {

    // Expected
    String expected =
        "{\"id\":\"777\",\"backbone\":[{\"taxonKey\":1,\"name\":\"Name\",\"depthKey_0\":1,\"kingdomKey\":1,\"rank\":\"CHEMOFORM\"},"
            + "{\"taxonKey\":2,\"name\":\"Name2\",\"depthKey_1\":2,\"kingdomKey\":2,\"rank\":\"ABERRATION\"}],\"gbifTaxonKey\":\"1\","
            + "\"gbifScientificName\":\"n\",\"gbifTaxonRank\":\"ABERRATION\"}";

    // State
    List<RankedName> rankedNameList = new ArrayList<>();
    RankedName name = RankedName.newBuilder().setKey(1).setName("Name").setRank(Rank.CHEMOFORM).build();
    RankedName name2 = RankedName.newBuilder().setKey(2).setName("Name2").setRank(Rank.ABERRATION).build();
    rankedNameList.add(name);
    rankedNameList.add(name2);

    TaxonRecord taxonRecord = TaxonRecord.newBuilder()
        .setId("777")
        .setUsage(RankedName.newBuilder().setKey(1).setName("n").setRank(Rank.ABERRATION).build())
        .setClassification(rankedNameList)
        .build();

    // When
    String result = GbifJsonConverter.toStringPartialJson(taxonRecord);

    // Should
    Assert.assertEquals(expected, result);
    Assert.assertTrue(JsonValidationUtils.isValid(result));
  }

  @Test
  public void extendedRecordSkipIssuesWithIdTest() {

    // Expected
    String expected = "{\"id\":\"777\",\"verbatim\":{}}";

    // State
    ExtendedRecord record = ExtendedRecord.newBuilder().setId("777").build();

    // When
    String result = GbifJsonConverter.toStringPartialJson(record);

    // Should
    Assert.assertEquals(expected, result);
    Assert.assertTrue(JsonValidationUtils.isValid(result));
  }

  @Test
  public void temporalRecordSkipIssuesWithIdTest() {

    // Expected
    String expected = "{\"id\":\"777\"}";

    // State
    TemporalRecord record = TemporalRecord.newBuilder().setId("777").build();

    // When
    String result = GbifJsonConverter.toStringPartialJson(record);

    // Should
    Assert.assertEquals(expected, result);
    Assert.assertTrue(JsonValidationUtils.isValid(result));
  }

  @Test
  public void locationRecordSkipIssuesWithIdTest() {

    // Expected
    String expected = "{\"id\":\"777\"}";

    // State
    LocationRecord record = LocationRecord.newBuilder().setId("777").build();

    // When
    String result = GbifJsonConverter.toStringPartialJson(record);

    // Should
    Assert.assertEquals(expected, result);
    Assert.assertTrue(JsonValidationUtils.isValid(result));
  }

  @Test
  public void australiaSpatialRecordSkipIssuesWithIdTest() {

    // Expected
    String expected = "{\"id\":\"777\",\"australiaSpatialLayers\":[{\"key\":\"{awdawd}\","
        + "\"value\":\"\\\"{\\\"wad\\\":\\\"adw\\\"}\\\"\"}]}";

    // State
    AustraliaSpatialRecord record = AustraliaSpatialRecord.newBuilder()
        .setId("777")
        .setItems(Collections.singletonMap("{awdawd}", "\"{\"wad\":\"adw\"}\""))
        .build();

    // When
    String result = GbifJsonConverter.toStringPartialJson(record);

    // Should
    Assert.assertEquals(expected, result);
    Assert.assertTrue(JsonValidationUtils.isValid(result));
  }
}
