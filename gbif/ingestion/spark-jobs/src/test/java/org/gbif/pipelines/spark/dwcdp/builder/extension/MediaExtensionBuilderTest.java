package org.gbif.pipelines.spark.dwcdp.builder.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.pipelines.spark.dwcdp.builder.TermResolver;
import org.gbif.pipelines.spark.util.MapperUtil;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TestTableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Tests for {@link MediaExtensionBuilder}, in particular:
 *
 * <ul>
 *   <li>{@code ROW_TYPE_MULTIMEDIA} holding the real Simple Multimedia row type, not Audubon Core's
 *       (which it held by mistake before this fix — see class javadoc on {@link
 *       MediaExtensionBuilder} for how that happened).
 *   <li>{@code license}/{@code rightsHolder} arriving via the new {@link UsagePolicyJoinBuilder}
 *       enrichment, for both the event-media and occurrence-media paths.
 * </ul>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MediaExtensionBuilderTest {

  SparkSession spark;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("MediaExtensionBuilderTest").getOrCreate();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  // ---- fixtures ----

  private Dataset<Row> eventPkDf(List<Row> rows) {
    StructType schema =
        new StructType().add("event_pk", DataTypes.StringType).add("eventID", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> occurrencePkDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("occurrence_pk", DataTypes.StringType)
            .add("occurrenceID", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> mediaDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("media_pk", DataTypes.StringType)
            .add("usagePolicy_fk", DataTypes.StringType)
            .add("accessURI", DataTypes.StringType)
            .add("mediaType", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> mediaNoUsagePolicyDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("media_pk", DataTypes.StringType)
            .add("accessURI", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> usagePolicyDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("usagePolicy_pk", DataTypes.StringType)
            .add("license", DataTypes.StringType)
            .add("rightsHolder", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> eventMediaDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("event_fk", DataTypes.StringType)
            .add("media_fk", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> occurrenceMediaDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("occurrence_fk", DataTypes.StringType)
            .add("media_fk", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  // ---- row type ----

  @Test
  void rowType_isSimpleMultimediaNotAudubon() {
    // Extension.MULTIMEDIA.getRowType() in org.gbif.api.vocabulary.Extension.
    // Extension.AUDUBON.getRowType() is "http://rs.tdwg.org/ac/terms/Multimedia" — the value
    // this constant held by mistake before this fix.
    assertEquals(
        "http://rs.gbif.org/terms/1.0/Multimedia", MediaExtensionBuilder.ROW_TYPE_MULTIMEDIA);
  }

  // ---- term resolution (accessURI / mediaType renames) ----

  @Test
  void accessUri_resolvesToDcIdentifierNotAcAccessUri() {
    assertEquals(DcTerm.identifier.qualifiedName(), TermResolver.resolve("accessURI"));
  }

  @Test
  void mediaType_resolvesToDcType() {
    assertEquals(DcTerm.type.qualifiedName(), TermResolver.resolve("mediaType"));
  }

  // ---- usage-policy enrichment: event-media path ----

  @Test
  void eventMedia_licenseAndRightsHolderJoinedViaUsagePolicy() throws Exception {
    Dataset<Row> eventDf = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> mediaDf =
        mediaDf(
            List.of(
                RowFactory.create("MPK-001", "UP-1", "https://example.com/img.jpg", "StillImage")));
    Dataset<Row> upDf = usagePolicyDf(List.of(RowFactory.create("UP-1", "CC_BY_4_0", "Museum X")));
    Dataset<Row> eventMediaDf = eventMediaDf(List.of(RowFactory.create("EPK-001", "MPK-001")));

    Optional<Dataset<Row>> resultOpt =
        MediaExtensionBuilder.buildEventMediaExtension(
            spark,
            TestTableLoader.of(
                "event",
                eventDf,
                MediaExtensionBuilder.TABLE_MEDIA,
                mediaDf,
                UsagePolicyJoinBuilder.TABLE_USAGE_POLICY,
                upDf,
                MediaExtensionBuilder.TABLE_EVENT_MEDIA,
                eventMediaDf));

    assertTrue(resultOpt.isPresent());
    Row row = resultOpt.get().collectAsList().get(0);
    List<Map<String, String>> mediaExt = parseMediaJson(row);
    assertEquals(1, mediaExt.size());
    assertEquals("CC_BY_4_0", mediaExt.get(0).get(DcTerm.license.qualifiedName()));
    assertEquals("Museum X", mediaExt.get(0).get(DcTerm.rightsHolder.qualifiedName()));
    assertEquals(
        "https://example.com/img.jpg", mediaExt.get(0).get(DcTerm.identifier.qualifiedName()));
  }

  // ---- usage-policy enrichment: occurrence-media path ----

  @Test
  void occurrenceMedia_licenseAndRightsHolderJoinedViaUsagePolicy() throws Exception {
    Dataset<Row> occDf = occurrencePkDf(List.of(RowFactory.create("OPK-001", "OCC001")));
    Dataset<Row> mediaDf =
        mediaDf(
            List.of(
                RowFactory.create("MPK-001", "UP-1", "https://example.com/img.jpg", "StillImage")));
    Dataset<Row> upDf = usagePolicyDf(List.of(RowFactory.create("UP-1", "CC0_1_0", "Herbarium Y")));
    Dataset<Row> occMediaDf = occurrenceMediaDf(List.of(RowFactory.create("OPK-001", "MPK-001")));

    Optional<Dataset<Row>> resultOpt =
        MediaExtensionBuilder.buildOccurrenceMediaExtension(
            spark,
            TestTableLoader.of(
                "occurrence",
                occDf,
                MediaExtensionBuilder.TABLE_MEDIA,
                mediaDf,
                UsagePolicyJoinBuilder.TABLE_USAGE_POLICY,
                upDf,
                MediaExtensionBuilder.TABLE_OCCURRENCE_MEDIA,
                occMediaDf));

    assertTrue(resultOpt.isPresent());
    Row row = resultOpt.get().collectAsList().get(0);
    List<Map<String, String>> mediaExt = parseMediaJson(row);
    assertEquals(1, mediaExt.size());
    assertEquals("CC0_1_0", mediaExt.get(0).get(DcTerm.license.qualifiedName()));
    assertEquals("Herbarium Y", mediaExt.get(0).get(DcTerm.rightsHolder.qualifiedName()));
  }

  // ---- usage-policy absent: media extension still builds ----

  @Test
  void usagePolicyTableAbsent_mediaExtensionStillBuildsWithoutLicenseFields() throws Exception {
    Dataset<Row> eventDf = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> mediaDf =
        mediaNoUsagePolicyDf(List.of(RowFactory.create("MPK-001", "https://example.com/img.jpg")));
    Dataset<Row> eventMediaDf = eventMediaDf(List.of(RowFactory.create("EPK-001", "MPK-001")));

    Optional<Dataset<Row>> resultOpt =
        MediaExtensionBuilder.buildEventMediaExtension(
            spark,
            TestTableLoader.of(
                "event",
                eventDf,
                MediaExtensionBuilder.TABLE_MEDIA,
                mediaDf,
                MediaExtensionBuilder.TABLE_EVENT_MEDIA,
                eventMediaDf));

    assertTrue(
        resultOpt.isPresent(),
        "usage-policy being absent must not prevent the media extension from building at all");
    List<Map<String, String>> mediaExt = parseMediaJson(resultOpt.get().collectAsList().get(0));
    assertEquals(1, mediaExt.size());
    assertEquals(
        "https://example.com/img.jpg", mediaExt.get(0).get(DcTerm.identifier.qualifiedName()));
    assertTrue(mediaExt.get(0).get(DcTerm.license.qualifiedName()) == null);
  }

  // ---- helper ----

  @SuppressWarnings("unchecked")
  private List<Map<String, String>> parseMediaJson(Row row) throws Exception {
    String json = row.getAs(MediaExtensionBuilder.COL_MEDIA_EXT_JSON);
    return MapperUtil.MAPPER.readValue(json, new TypeReference<List<Map<String, String>>>() {});
  }
}
