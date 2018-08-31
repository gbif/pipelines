package org.gbif.pipelines.parsers.ws.client.match2;

import org.gbif.api.v2.NameUsageMatch2;
import org.gbif.api.v2.RankedName;
import org.gbif.api.vocabulary.Rank;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.parsers.utils.ExtendedRecordBuilder;
import org.gbif.pipelines.parsers.ws.BaseMockServerTest;
import org.gbif.pipelines.parsers.ws.HttpResponse;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Map;
import java.util.stream.Collectors;

import okhttp3.mockwebserver.MockResponse;
import org.junit.Assert;
import org.junit.Test;
import retrofit2.Call;
import retrofit2.Response;

public class SpeciesMatchv2ServiceRestTest extends BaseMockServerTest {

  private static final String TEST_RECORD_ID = "testId";

  @Test
  public void simpleCallTest() throws IOException {

    // State
    SpeciesMatchv2Service service =
        SpeciesMatchv2ServiceRest.getInstance(getWsConfig()).getService();
    enqueueResponse(PUMA_CONCOLOR_RESPONSE);

    final String name = "Puma concolor";

    // When
    Call<NameUsageMatch2> call =
        service.match(null, null, null, null, null, null, null, name, true, false);
    Response<NameUsageMatch2> response = call.execute();

    // Should
    Assert.assertNotNull(response);
  }

  @Test
  public void shouldReturn500error() {

    // State
    MOCK_SERVER.enqueue(new MockResponse().setResponseCode(HttpURLConnection.HTTP_INTERNAL_ERROR));

    ExtendedRecord record = ExtendedRecordBuilder.create().name("Puma concolor").id("1").build();

    // When
    HttpResponse<NameUsageMatch2> response =
        SpeciesMatchv2Client.create(getWsConfig()).getMatch(record);

    // Should
    Assert.assertEquals(
        HttpURLConnection.HTTP_INTERNAL_ERROR, response.getHttpResponseCode().intValue());
    Assert.assertEquals(HttpResponse.ErrorCode.CALL_FAILED, response.getErrorCode());
  }

  /**
   * Call mocked:
   * https://api.gbif-uat.org/v1/species/match2?kingdom=Animalia&genus=Puma&rank=SPECIES&name=Puma%20concolor&strict=false&verbose=false
   */
  @Test
  public void assembledAuthorTest() throws IOException {

    // State
    ExtendedRecord record =
        ExtendedRecordBuilder.create()
            .kingdom("Animalia")
            .genus("Puma")
            .name("Puma concolor")
            .authorship("")
            .rank(Rank.SPECIES.name())
            .id(TEST_RECORD_ID)
            .build();

    enqueueResponse(PUMA_CONCOLOR_RESPONSE);

    // When

    NameUsageMatch2 result = SpeciesMatchv2Client.create(getWsConfig()).getMatch(record).getBody();
    Map<Rank, RankedName> ranksResponse =
        result
            .getClassification()
            .stream()
            .collect(Collectors.toMap(RankedName::getRank, rankedName -> rankedName));

    // Should
    Assert.assertEquals(2435099, result.getUsage().getKey());
    Assert.assertEquals(1, ranksResponse.get(Rank.KINGDOM).getKey());
    Assert.assertEquals("Chordata", ranksResponse.get(Rank.PHYLUM).getName());
  }

  /**
   * Call mocked:
   * http://api.gbif-uat.org/v1/species/match2?kingdom=Animalia&genus=Puma&rank=SPECIES&name=Puma%20concolor%20(Linnaeus,%201771)&strict=false&verbose=false
   */
  @Test
  public void assembledAuthorRankTest() throws IOException {

    // State
    ExtendedRecord record =
        ExtendedRecordBuilder.create()
            .kingdom("Animalia")
            .genus("Puma")
            .name("Puma concolor (Linnaeus, 1771)")
            .rank(Rank.SPECIES.name())
            .id(TEST_RECORD_ID)
            .build();

    enqueueResponse(PUMA_CONCOLOR_2_RESPONSE);

    // When
    NameUsageMatch2 result = SpeciesMatchv2Client.create(getWsConfig()).getMatch(record).getBody();
    Map<Rank, RankedName> ranksResponse =
        result
            .getClassification()
            .stream()
            .collect(Collectors.toMap(RankedName::getRank, rankedName -> rankedName));

    // Should
    Assert.assertEquals(2435099, result.getUsage().getKey());
    Assert.assertEquals(1, ranksResponse.get(Rank.KINGDOM).getKey());
    Assert.assertEquals("Chordata", ranksResponse.get(Rank.PHYLUM).getName());
  }

  /**
   * Call mocked:
   * http://api.gbif-uat.org/v1/species/match2?kingdom=Animalia&genus=Puma&rank=SPECIES&name=Puma%20concolor%20(Linnaeus,%201771)&strict=false&verbose=false
   */
  @Test
  public void assembledAuthorRankAuthorshipTest() throws IOException {

    // State
    ExtendedRecord record =
        ExtendedRecordBuilder.create()
            .kingdom("Animalia")
            .genus("Puma")
            .name("Puma concolor")
            .authorship("(Linnaeus, 1771)")
            .rank(Rank.SPECIES.name())
            .id(TEST_RECORD_ID)
            .build();

    enqueueResponse(PUMA_CONCOLOR_3_RESPONSE);

    // When
    NameUsageMatch2 result = SpeciesMatchv2Client.create(getWsConfig()).getMatch(record).getBody();
    Map<Rank, RankedName> ranksResponse =
        result
            .getClassification()
            .stream()
            .collect(Collectors.toMap(RankedName::getRank, rankedName -> rankedName));

    // Should
    Assert.assertEquals(2435099, result.getUsage().getKey());
    Assert.assertEquals(1, ranksResponse.get(Rank.KINGDOM).getKey());
    Assert.assertEquals("Chordata", ranksResponse.get(Rank.PHYLUM).getName());
  }

  /**
   * Call mocked:
   * https://api.gbif-uat.org/v1/species/match2?kingdom=Plantae&rank=GENUS&name=Oenanthe&strict=false&verbose=false
   */
  @Test
  public void oenantheTest() throws IOException {

    // State
    ExtendedRecord record =
        ExtendedRecordBuilder.create()
            .kingdom("Plantae")
            .name("Oenanthe")
            .authorship("")
            .rank(Rank.GENUS.name())
            .id(TEST_RECORD_ID)
            .build();

    enqueueResponse(OENANTHE_RESPONSE);

    // When
    NameUsageMatch2 result = SpeciesMatchv2Client.create(getWsConfig()).getMatch(record).getBody();
    Map<Rank, RankedName> ranksResponse =
        result
            .getClassification()
            .stream()
            .collect(Collectors.toMap(RankedName::getRank, rankedName -> rankedName));

    // Should
    Assert.assertEquals(3034893, result.getUsage().getKey());
    Assert.assertEquals(6, ranksResponse.get(Rank.KINGDOM).getKey());
    Assert.assertEquals("Oenanthe L.", result.getUsage().getName());
  }

  /**
   * Call mocked:
   * https://api.gbif-uat.org/v1/species/match2?kingdom=Plantae&rank=GENUS&name=Oenanthe%20L.&strict=false&verbose=false
   */
  @Test
  public void oenantheAuthorshipTest() throws IOException {

    // State
    ExtendedRecord record =
        ExtendedRecordBuilder.create()
            .kingdom("Plantae")
            .name("Oenanthe")
            .authorship("L.")
            .rank(Rank.GENUS.name())
            .id(TEST_RECORD_ID)
            .build();

    enqueueResponse(OENANTHE_2_RESPONSE);

    // When
    NameUsageMatch2 result = SpeciesMatchv2Client.create(getWsConfig()).getMatch(record).getBody();
    Map<Rank, RankedName> ranksResponse =
        result
            .getClassification()
            .stream()
            .collect(Collectors.toMap(RankedName::getRank, rankedName -> rankedName));

    // Should
    Assert.assertEquals(3034893, result.getUsage().getKey());
    Assert.assertEquals(6, ranksResponse.get(Rank.KINGDOM).getKey());
    Assert.assertEquals("Oenanthe L.", result.getUsage().getName());
  }

  /**
   * Call mocked:
   * https://api.gbif-uat.org/v1/species/match2?kingdom=Animalia&rank=GENUS&name=Oenanthe%20Vieillot,%201816&strict=false&verbose=false
   */
  @Test
  public void oenantheRankAuthorshipTest() throws IOException {

    // State
    ExtendedRecord record =
        ExtendedRecordBuilder.create()
            .kingdom("Animalia")
            .name("Oenanthe")
            .authorship("Vieillot, 1816")
            .rank(Rank.GENUS.name())
            .id(TEST_RECORD_ID)
            .build();

    enqueueResponse(OENANTHE_3_RESPONSE);

    // When
    NameUsageMatch2 result = SpeciesMatchv2Client.create(getWsConfig()).getMatch(record).getBody();
    Map<Rank, RankedName> ranksResponse =
        result
            .getClassification()
            .stream()
            .collect(Collectors.toMap(RankedName::getRank, rankedName -> rankedName));

    // Should
    Assert.assertEquals(2492483, result.getUsage().getKey());
    Assert.assertEquals(1, ranksResponse.get(Rank.KINGDOM).getKey());
    Assert.assertEquals("Oenanthe Vieillot, 1816", result.getUsage().getName());
  }

  /**
   * Call mocked:
   * https://api.gbif-uat.org/v1/species/match2?kingdom=Animalia&phylum=Annelida&family=Lumbricidae&rank=SPECIES&name=Bold:acv7160&strict=false&verbose=false
   */
  @Test
  public void otuTest() throws IOException {

    // State
    ExtendedRecord record =
        ExtendedRecordBuilder.create()
            .kingdom("Animalia")
            .phylum("Annelida")
            .family("Lumbricidae")
            .name("BOLD:ACV7160")
            .rank(Rank.SPECIES.name())
            .id(TEST_RECORD_ID)
            .build();

    enqueueResponse(ANNELIDA_RESPONSE);

    // When
    NameUsageMatch2 result = SpeciesMatchv2Client.create(getWsConfig()).getMatch(record).getBody();

    // Should
    Assert.assertEquals("Lumbricidae", result.getUsage().getName());
  }

  /**
   * Call mocked:
   * https://api.gbif-uat.org/v1/species/match2?kingdom=Chromista&phylum=Dinophyta&class=Dinophyceae&order=Peridiniales&family=Ceratiaceae&genus=Ceratium&rank=SPECIES&name=Ceratium%20hirundinella&strict=false&verbose=false
   */
  @Test
  public void ceratiaceaeTest() throws IOException {

    // State
    ExtendedRecord record =
        ExtendedRecordBuilder.create()
            .kingdom("Chromista")
            .phylum("Dinophyta")
            .clazz("Dinophyceae")
            .order("Peridiniales")
            .family("Ceratiaceae")
            .genus("Ceratium")
            .name("Ceratium hirundinella")
            .rank(Rank.SPECIES.name())
            .id(TEST_RECORD_ID)
            .build();

    enqueueResponse(CERATIACEAE_RESPONSE);

    // When
    NameUsageMatch2 result = SpeciesMatchv2Client.create(getWsConfig()).getMatch(record).getBody();
    Map<Rank, RankedName> ranksResponse =
        result
            .getClassification()
            .stream()
            .collect(Collectors.toMap(RankedName::getRank, rankedName -> rankedName));

    // Should
    Assert.assertEquals(7598904, result.getUsage().getKey());
    Assert.assertEquals(7479242, ranksResponse.get(Rank.FAMILY).getKey());
    Assert.assertEquals("Ceratium hirundinella (O.F.Müll.) Dujard.", result.getUsage().getName());
  }

  /**
   * Call mocked:
   * https://api.gbif-uat.org/v1/species/match2?kingdom=Animalia&genus=Puma&rank=SPECIES&name=Puma%20concolor&strict=false&verbose=false
   */
  @Test
  public void nubLookupGoodTest() throws IOException {

    // State
    ExtendedRecord record =
        ExtendedRecordBuilder.create()
            .kingdom("Animalia")
            .genus("Puma")
            .name("Puma concolor")
            .rank(Rank.SPECIES.name())
            .id(TEST_RECORD_ID)
            .build();

    enqueueResponse(PUMA_CONCOLOR_RESPONSE);

    // When
    NameUsageMatch2 result = SpeciesMatchv2Client.create(getWsConfig()).getMatch(record).getBody();
    Map<Rank, RankedName> ranksResponse =
        result
            .getClassification()
            .stream()
            .collect(Collectors.toMap(RankedName::getRank, rankedName -> rankedName));

    // Should
    Assert.assertEquals(2435099, result.getUsage().getKey());
    Assert.assertEquals(1, ranksResponse.get(Rank.KINGDOM).getKey());
    Assert.assertEquals("Chordata", ranksResponse.get(Rank.PHYLUM).getName());
  }

  /**
   * Calls
   * https://api.gbif-uat.org/v1/species/match2?rank=GENUS&name=Agallisus%20lepturoides&strict=false&verbose=false
   */
  @Test
  public void acceptedUsageTest() throws IOException {

    // State
    ExtendedRecord record =
        ExtendedRecordBuilder.create().name("Agallisus lepturoides").id(TEST_RECORD_ID).build();

    enqueueResponse(AGALLISUS_LEPTUROIDES_RESPONSE);

    // When
    NameUsageMatch2 result = SpeciesMatchv2Client.create(getWsConfig()).getMatch(record).getBody();

    // Should
    Assert.assertEquals(1118030, result.getUsage().getKey());
    Assert.assertEquals(1118026, result.getAcceptedUsage().getKey());
    Assert.assertEquals(
        "Agallisus lepturoides Hovore, Penrose & Neck, 1987", result.getUsage().getName());
    Assert.assertEquals(
        "Agallissus lepturoides (Chevrolat, 1849)", result.getAcceptedUsage().getName());
  }

  @Test(expected = NullPointerException.class)
  public void matchNullTaxonRecordTest() {

    // When
    SpeciesMatchv2Client.create(getWsConfig()).getMatch(null);
  }

  @Test(expected = NullPointerException.class)
  public void matchNullArgsTest() {

    // When
    SpeciesMatchv2Client.create(getWsConfig()).getMatch(null);

    // Should
    Assert.fail("This line should not be reached ");
  }
}
