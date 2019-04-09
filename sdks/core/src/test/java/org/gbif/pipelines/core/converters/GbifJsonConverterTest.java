package org.gbif.pipelines.core.converters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.Amplification;
import org.gbif.pipelines.io.avro.AmplificationRecord;
import org.gbif.pipelines.io.avro.AustraliaSpatialRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.BlastResult;
import org.gbif.pipelines.io.avro.DeterminedDate;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFact;
import org.gbif.pipelines.io.avro.MeasurementOrFactRecord;
import org.gbif.pipelines.io.avro.Multimedia;
import org.gbif.pipelines.io.avro.MultimediaRecord;
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
        "{\"id\":\"777\",\"verbatim\":{\"core\":{\"http://purl.org/dc/terms/remark\":\"{\\\"something\\\":1}"
            + "{\\\"something\\\":1}\",\"http://rs.tdwg.org/dwc/terms/locality\":\"something:{something}\"},\"extensions\":{}},"
            + "\"startDate\":\"01-01-2011\",\"year\":2000,\"day\":1,\"eventDate\":{\"gte\": \"01-01-2011\", \"lte\": "
            + "\"01-01-2018\"},\"startDayOfYear\":1,\"issues\":[\"BASIS_OF_RECORD_INVALID\",\"ZERO_COORDINATE\"],\"coordinates\":"
            + "{\"lon\":2.0,\"lat\":1.0},\"continent\":\"something{something}\",\"country\":\"Country\",\"countryCode\":"
            + "\"Code 1'2\\\"\",\"gbifClassification\":{\"usage\":{\"key\": 2, \"name\": \"Name2\", \"rank\": \"ABERRATION\"},"
            + "\"classification\":[{\"key\":1,\"name\":\"Name\",\"rank\":\"CHEMOFORM\"}," + "{\"key\":2,\"name\":\"Name2\",\"rank\":\"ABERRATION\"}],"
            + "\"issues\":{\"issueList\": []},\"chemoformKey\":1,\"aberrationKey\":2,\"classificationPath\":\"_1\"},\"gbifId\":111,"
            + "\"notIssues\":[\"COORDINATE_PRECISION_UNCERTAINTY_MISMATCH\",\"MODIFIED_DATE_INVALID\",\"CONTINENT_COUNTRY_MISMATCH\","
            + "\"COORDINATE_INVALID\",\"COORDINATE_PRECISION_INVALID\",\"ELEVATION_NON_NUMERIC\",\"COORDINATE_OUT_OF_RANGE\","
            + "\"COUNTRY_INVALID\",\"ELEVATION_NOT_METRIC\",\"COORDINATE_REPROJECTION_SUSPICIOUS\",\"PRESUMED_NEGATED_LONGITUDE\","
            + "\"DEPTH_UNLIKELY\",\"IDENTIFIED_DATE_INVALID\",\"ELEVATION_MIN_MAX_SWAPPED\",\"TAXON_MATCH_NONE\","
            + "\"TYPE_STATUS_INVALID\",\"TAXON_MATCH_FUZZY\",\"CONTINENT_INVALID\",\"GEODETIC_DATUM_INVALID\","
            + "\"MODIFIED_DATE_UNLIKELY\",\"COORDINATE_REPROJECTED\",\"PRESUMED_SWAPPED_COORDINATE\",\"REFERENCES_URI_INVALID\","
            + "\"COORDINATE_ROUNDED\",\"IDENTIFIED_DATE_UNLIKELY\",\"COUNTRY_COORDINATE_MISMATCH\",\"DEPTH_NON_NUMERIC\","
            + "\"COUNTRY_DERIVED_FROM_COORDINATES\",\"COORDINATE_REPROJECTION_FAILED\",\"COORDINATE_UNCERTAINTY_METERS_INVALID\","
            + "\"PRESUMED_NEGATED_LATITUDE\",\"MULTIMEDIA_URI_INVALID\",\"COORDINATE_ACCURACY_INVALID\",\"GEODETIC_DATUM_ASSUMED_WGS84\","
            + "\"TAXON_MATCH_HIGHERRANK\",\"ELEVATION_UNLIKELY\",\"CONTINENT_DERIVED_FROM_COORDINATES\",\"DEPTH_MIN_MAX_SWAPPED\","
            + "\"RECORDED_DATE_INVALID\",\"INDIVIDUAL_COUNT_INVALID\",\"RECORDED_DATE_MISMATCH\",\"DEPTH_NOT_METRIC\","
            + "\"MULTIMEDIA_DATE_INVALID\",\"INTERPRETATION_ERROR\",\"RECORDED_DATE_UNLIKELY\",\"COUNTRY_MISMATCH\"]}";

    // State
    Map<String, String> erMap = new HashMap<>(2);
    erMap.put("http://rs.tdwg.org/dwc/terms/locality", "something:{something}");
    erMap.put("http://purl.org/dc/terms/remark", "{\"something\":1}{\"something\":1}");

    ExtendedRecord er =
        ExtendedRecord.newBuilder()
            .setId("777")
            .setCoreRowType("core")
            .setCoreTerms(erMap)
            .build();

    BasicRecord br = BasicRecord.newBuilder().setId("777").setGbifId(111).build();

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

    TaxonRecord tr = TaxonRecord.newBuilder().setId("777").setClassification(rankedNameList).setUsage(name2).build();

    // When
    String result = GbifJsonConverter.toStringJson(er, tmr, lr, tr, br);

    // Should
    Assert.assertTrue(JsonValidationUtils.isValid(result));
    Assert.assertEquals(expected, result);
  }

  @Test
  public void jsonFromSpecificRecordBaseAustraliaTest() {

    // Expected
    String expected =
        "{\"id\":\"777\",\"verbatim\":{\"core\":{\"http://rs.tdwg.org/dwc/terms/remark\":\"{\\\"something\\\":1}"
            + "{\\\"something\\\":1}\",\"http://rs.tdwg.org/dwc/terms/locality\":\"something:{something}\"},"
            + "\"extensions\":{\"http://rs.tdwg.org/ac/terms/Multimedia\":[{\"http://purl.org/dc/terms/license\":\"Lic1\","
            + "\"http://www.w3.org/2003/01/geo/wgs84_pos#latitude\":\"60.4\",\"http://purl.org/dc/terms/identifier\":"
            + "\"http://www.gbif.org/tmp.jpg\",\"http://rs.tdwg.org/dwc/terms/datasetID\":\"1\",\"http://purl.org/dc/terms/description\":"
            + "\"Desc1\",\"http://purl.org/dc/terms/publisher\":\"Pub1\",\"http://purl.org/dc/terms/audience\":\"Aud1\","
            + "\"http://purl.org/dc/terms/spatial\":\"Sp1\",\"http://purl.org/dc/terms/format\":\"jpeg\","
            + "\"http://purl.org/dc/terms/rightsHolder\":\"Rh1\",\"http://purl.org/dc/terms/creator\":\"Cr1\","
            + "\"http://purl.org/dc/terms/created\":\"2010\",\"http://purl.org/dc/terms/references\":\"http://www.gbif.org/tmp.jpg\","
            + "\"http://purl.org/dc/terms/contributor\":\"Cont1\",\"http://purl.org/dc/terms/title\":\"Tt1\","
            + "\"http://www.w3.org/2003/01/geo/wgs84_pos#longitude\":\"-131.3\"},{\"http://purl.org/dc/terms/created\":\"not a date\"}],"
            + "\"http://rs.gbif.org/terms/1.0/Image\":[{\"http://purl.org/dc/terms/license\":\"Lic1\","
            + "\"http://www.w3.org/2003/01/geo/wgs84_pos#latitude\":\"60.4\",\"http://purl.org/dc/terms/identifier\":"
            + "\"http://www.gbif.org/tmp.jpg\",\"http://rs.tdwg.org/dwc/terms/datasetID\":\"1\",\"http://purl.org/dc/terms/description\":"
            + "\"Desc1\",\"http://purl.org/dc/terms/publisher\":\"Pub1\",\"http://purl.org/dc/terms/audience\":\"Aud1\","
            + "\"http://purl.org/dc/terms/spatial\":\"Sp1\",\"http://purl.org/dc/terms/format\":\"jpeg\","
            + "\"http://purl.org/dc/terms/rightsHolder\":\"Rh1\",\"http://purl.org/dc/terms/creator\":\"Cr1\","
            + "\"http://purl.org/dc/terms/created\":\"2010\",\"http://purl.org/dc/terms/references\":\"http://www.gbif.org/tmp.jpg\","
            + "\"http://purl.org/dc/terms/contributor\":\"Cont1\",\"http://purl.org/dc/terms/title\":\"Tt1\","
            + "\"http://www.w3.org/2003/01/geo/wgs84_pos#longitude\":\"-131.3\"},{\"http://purl.org/dc/terms/created\":\"not a date\"}]}},"
            + "\"startDate\":\"01-01-2011\",\"year\":2000,\"day\":1,\"eventDate\":{\"gte\": \"01-01-2011\", \"lte\": \"01-01-2018\"},"
            + "\"startDayOfYear\":1,\"issues\":[\"BASIS_OF_RECORD_INVALID\",\"ZERO_COORDINATE\"],\"coordinates\":{\"lon\":2.0,\"lat\":1.0},"
            + "\"continent\":\"something{something}\",\"country\":\"Country\",\"countryCode\":\"Code 1'2\\\"\","
            + "\"gbifClassification\":{\"usage\":{\"key\": 2, \"name\": \"Name2\", \"rank\": \"ABERRATION\"},"
            + "\"classification\":[{\"key\":1,\"name\":\"Name\",\"rank\":\"CHEMOFORM\"}," + "{\"key\":2,\"name\":\"Name2\",\"rank\":\"ABERRATION\"}],\"issues\":{\"issueList\": []},"
            + "\"chemoformKey\":1,\"aberrationKey\":2,\"classificationPath\":\"_1\"},"
            + "\"australiaSpatialLayers\":[{\"key\":\"data\",\"value\":\"value\"}],\"measurementOrFactItems\":[{\"id\":\"123\",\"type\":\"{\\\"something\\\":1}"
            + "{\\\"something\\\":1}\",\"value\":1.1,\"determinedDate\":{\"gte\": \"2010\", \"lte\": \"2011\"}},{\"id\":\"124\",\"type\":null,"
            + "\"value\":null,\"determinedDate\":{\"gte\": \"2010\", \"lte\": \"2012\"}}],\"notIssues\":[\"COORDINATE_PRECISION_UNCERTAINTY_MISMATCH\","
            + "\"MODIFIED_DATE_INVALID\",\"CONTINENT_COUNTRY_MISMATCH\",\"COORDINATE_INVALID\",\"COORDINATE_PRECISION_INVALID\","
            + "\"ELEVATION_NON_NUMERIC\",\"COORDINATE_OUT_OF_RANGE\",\"COUNTRY_INVALID\",\"ELEVATION_NOT_METRIC\",\"COORDINATE_REPROJECTION_SUSPICIOUS\","
            + "\"PRESUMED_NEGATED_LONGITUDE\",\"DEPTH_UNLIKELY\",\"IDENTIFIED_DATE_INVALID\",\"ELEVATION_MIN_MAX_SWAPPED\","
            + "\"TAXON_MATCH_NONE\",\"TYPE_STATUS_INVALID\",\"TAXON_MATCH_FUZZY\",\"CONTINENT_INVALID\",\"GEODETIC_DATUM_INVALID\","
            + "\"MODIFIED_DATE_UNLIKELY\",\"COORDINATE_REPROJECTED\",\"PRESUMED_SWAPPED_COORDINATE\",\"REFERENCES_URI_INVALID\",\"COORDINATE_ROUNDED\","
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

    // State
    Map<String, String> ext1 = new HashMap<>();
    ext1.put(DcTerm.identifier.qualifiedName(), "http://www.gbif.org/tmp.jpg");
    ext1.put(DcTerm.references.qualifiedName(), "http://www.gbif.org/tmp.jpg");
    ext1.put(DcTerm.created.qualifiedName(), "2010");
    ext1.put(DcTerm.title.qualifiedName(), "Tt1");
    ext1.put(DcTerm.description.qualifiedName(), "Desc1");
    ext1.put(DcTerm.spatial.qualifiedName(), "Sp1");
    ext1.put(DcTerm.format.qualifiedName(), "jpeg");
    ext1.put(DcTerm.creator.qualifiedName(), "Cr1");
    ext1.put(DcTerm.contributor.qualifiedName(), "Cont1");
    ext1.put(DcTerm.publisher.qualifiedName(), "Pub1");
    ext1.put(DcTerm.audience.qualifiedName(), "Aud1");
    ext1.put(DcTerm.license.qualifiedName(), "Lic1");
    ext1.put(DcTerm.rightsHolder.qualifiedName(), "Rh1");
    ext1.put(DwcTerm.datasetID.qualifiedName(), "1");
    ext1.put("http://www.w3.org/2003/01/geo/wgs84_pos#longitude", "-131.3");
    ext1.put("http://www.w3.org/2003/01/geo/wgs84_pos#latitude", "60.4");

    Map<String, String> ext2 = new HashMap<>();
    ext2.put(DcTerm.created.qualifiedName(), "not a date");

    Map<String, List<Map<String, String>>> extMap1 = new HashMap<>();
    extMap1.put(Extension.IMAGE.getRowType(), Arrays.asList(ext1, ext2));

    Map<String, List<Map<String, String>>> extMap2 = new HashMap<>();
    extMap2.put(Extension.AUDUBON.getRowType(), Arrays.asList(ext1, ext2));

    extMap1.putAll(extMap2);

    ExtendedRecord er =
        ExtendedRecord.newBuilder()
            .setId("777")
            .setCoreRowType("core")
            .setCoreTerms(erMap)
            .setExtensions(extMap1)
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

    TaxonRecord tr = TaxonRecord.newBuilder().setId("777").setClassification(rankedNameList).setUsage(name2).build();

    MeasurementOrFactRecord mfr =
        MeasurementOrFactRecord.newBuilder().setId("777").setMeasurementOrFactItems(
            Arrays.asList(
                MeasurementOrFact.newBuilder()
                    .setType("{\"something\":1}{\"something\":1}")
                    .setId("123")
                    .setValueParsed(1.1d)
                    .setDeterminedDateParsed(DeterminedDate.newBuilder().setGte("2010").setLte("2011").build())
                    .build(),
                MeasurementOrFact.newBuilder()
                    .setId("124")
                    .setDeterminedDateParsed(DeterminedDate.newBuilder().setGte("2010").setLte("2012").build())
                    .build(),
                MeasurementOrFact.newBuilder()
                    .setId("125")
                    .build()))
            .build();

    // When
    String result = GbifJsonConverter.toStringJson(er, tmr, lr, tr, asr, mfr);

    // Should
    Assert.assertTrue(JsonValidationUtils.isValid(result));
    Assert.assertEquals(expected, result);
  }

  @Test
  public void onlyOneIdInJsonTest() {

    // Expected
    String expected =
        "{\"id\":\"777\",\"verbatim\":{\"core\":{},\"extensions\":{}},\"issues\":[],\"notIssues\":"
            + "[\"COORDINATE_PRECISION_UNCERTAINTY_MISMATCH\",\"MODIFIED_DATE_INVALID\",\"CONTINENT_COUNTRY_MISMATCH\","
            + "\"COORDINATE_INVALID\",\"COORDINATE_PRECISION_INVALID\",\"ELEVATION_NON_NUMERIC\",\"COORDINATE_OUT_OF_RANGE\","
            + "\"COUNTRY_INVALID\",\"ELEVATION_NOT_METRIC\",\"COORDINATE_REPROJECTION_SUSPICIOUS\",\"PRESUMED_NEGATED_LONGITUDE\","
            + "\"DEPTH_UNLIKELY\",\"IDENTIFIED_DATE_INVALID\",\"ELEVATION_MIN_MAX_SWAPPED\",\"TAXON_MATCH_NONE\","
            + "\"BASIS_OF_RECORD_INVALID\",\"TYPE_STATUS_INVALID\",\"TAXON_MATCH_FUZZY\",\"CONTINENT_INVALID\","
            + "\"GEODETIC_DATUM_INVALID\",\"MODIFIED_DATE_UNLIKELY\",\"COORDINATE_REPROJECTED\",\"PRESUMED_SWAPPED_COORDINATE\","
            + "\"REFERENCES_URI_INVALID\",\"COORDINATE_ROUNDED\",\"IDENTIFIED_DATE_UNLIKELY\",\"COUNTRY_COORDINATE_MISMATCH\","
            + "\"DEPTH_NON_NUMERIC\",\"COUNTRY_DERIVED_FROM_COORDINATES\",\"COORDINATE_REPROJECTION_FAILED\","
            + "\"COORDINATE_UNCERTAINTY_METERS_INVALID\",\"PRESUMED_NEGATED_LATITUDE\",\"MULTIMEDIA_URI_INVALID\","
            + "\"COORDINATE_ACCURACY_INVALID\",\"GEODETIC_DATUM_ASSUMED_WGS84\",\"TAXON_MATCH_HIGHERRANK\",\"ELEVATION_UNLIKELY\","
            + "\"CONTINENT_DERIVED_FROM_COORDINATES\",\"DEPTH_MIN_MAX_SWAPPED\",\"RECORDED_DATE_INVALID\","
            + "\"INDIVIDUAL_COUNT_INVALID\",\"RECORDED_DATE_MISMATCH\",\"DEPTH_NOT_METRIC\",\"MULTIMEDIA_DATE_INVALID\","
            + "\"INTERPRETATION_ERROR\",\"ZERO_COORDINATE\",\"RECORDED_DATE_UNLIKELY\",\"COUNTRY_MISMATCH\"]}";

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
  public void taxonRecordUsageTest() {

    // Expected
    String expected =
        "{\"id\":\"777\",\"gbifClassification\":{\"usage\":{\"key\": 1, \"name\": \"n\", \"rank\": \"ABERRATION\"},"
            + "\"classification\":[{\"key\":1,\"name\":\"Name\",\"rank\":\"CHEMOFORM\"},"
            + "{\"key\":2,\"name\":\"Name2\",\"rank\":\"ABERRATION\"}]"
            + ",\"acceptedUsage\":{\"key\": 2, \"name\": \"Name2\", \"rank\": \"ABERRATION\"},"
            + "\"chemoformKey\":1,\"aberrationKey\":2,\"classificationPath\":\"_1\"}}";

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
        .setAcceptedUsage(name2)
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
    String expected = "{\"id\":\"777\",\"verbatim\":{\"core\":{},\"extensions\":{}}}";

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

  @Test
  public void measurementOrFactRecordSkipIssuesWithIdTest() {

    // Expected
    String expected = "{\"id\":\"777\",\"measurementOrFactItems\":[]}";

    // State
    MeasurementOrFactRecord record = MeasurementOrFactRecord.newBuilder().setId("777").build();

    // When
    String result = GbifJsonConverter.toStringPartialJson(record);

    // Should
    Assert.assertEquals(expected, result);
    Assert.assertTrue(JsonValidationUtils.isValid(result));
  }

  @Test
  public void amplificationRecordSkipIssuesWithIdEmptyTest() {

    // Expected
    String expected = "{\"id\":\"777\",\"amplificationItems\":[]}";

    // State
    AmplificationRecord record = AmplificationRecord.newBuilder().setId("777").build();

    // When
    String result = GbifJsonConverter.toStringPartialJson(record);

    // Should
    Assert.assertEquals(expected, result);
    Assert.assertTrue(JsonValidationUtils.isValid(result));
  }

  @Test
  public void multimediaRecordSkipIssuesWithIdTest() {

    // Expected
    String expected = "{\"id\":\"777\"}";

    // State
    MultimediaRecord record = MultimediaRecord.newBuilder().setId("777").build();

    // When
    String result = GbifJsonConverter.toStringPartialJson(record);

    // Should
    Assert.assertEquals(expected, result);
    Assert.assertTrue(JsonValidationUtils.isValid(result));
  }

  @Test
  public void multimediaRecordSkipIssuesWithIdEmptyTest() {

    // Expected
    String expected = "{\"id\":\"777\",\"multimediaItems\":[{}]}";

    // State
    MultimediaRecord record = MultimediaRecord.newBuilder()
        .setId("777")
        .setMultimediaItems(Collections.singletonList(Multimedia.newBuilder().build()))
        .build();

    // When
    String result = GbifJsonConverter.toStringPartialJson(record);

    // Should
    Assert.assertEquals(expected, result);
    Assert.assertTrue(JsonValidationUtils.isValid(result));
  }

  @Test
  public void amplificationRecordSkipIssuesWithIdTest() {

    // Expected
    String expected =
        "{\"id\":\"777\",\"amplificationItems\":[{\"name\":\"n\",\"identity\":3,\"appliedScientificName\":"
            + "\"sn\",\"matchType\":\"mt\",\"bitScore\":1,\"expectValue\":2,\"querySequence\":\"qs\",\"subjectSequence\":"
            + "\"ss\",\"qstart\":5,\"qend\":4,\"sstart\":8,\"send\":6,\"distanceToBestMatch\":\"dm\",\"sequenceLength\":7}]}";

    // State
    AmplificationRecord record = AmplificationRecord.newBuilder()
        .setId("777")
        .setAmplificationItems(
            Arrays.asList(
                Amplification.newBuilder().setBlastResult(
                    BlastResult.newBuilder()
                        .setAppliedScientificName("sn")
                        .setBitScore(1)
                        .setDistanceToBestMatch("dm")
                        .setExpectValue(2)
                        .setIdentity(3)
                        .setMatchType("mt")
                        .setName("n")
                        .setQend(4)
                        .setQstart(5)
                        .setQuerySequence("qs")
                        .setSend(6)
                        .setSequenceLength(7)
                        .setSstart(8)
                        .setSubjectSequence("ss")
                        .build()
                ).build(),
                Amplification.newBuilder().build()
            )
        )
        .build();

    // When
    String result = GbifJsonConverter.toStringPartialJson(record);

    // Should
    Assert.assertEquals(expected, result);
    Assert.assertTrue(JsonValidationUtils.isValid(result));
  }

}
