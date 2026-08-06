package org.gbif.pipelines.spark.dwcdp.builder;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.gbif.pipelines.spark.util.MapperUtil;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Support for turning an extension builder's aggregated JSON column into the {@code Map<rowType,
 * List<Map<term, value>>>} shape {@link org.gbif.pipelines.io.avro.ExtendedRecord} expects, and for
 * synthesising a record identifier when a DwC-DP table's natural {@code weakPk} id column (e.g.
 * {@code eventID}, {@code occurrenceID}) is absent or null.
 *
 * <p>Every DwC-DP table requires+guarantees a unique surrogate {@code pk}, but the corresponding
 * natural id is optional per the profile — a legitimate, if unusual, package shape. The DwC-A/
 * {@link org.gbif.pipelines.io.avro.ExtendedRecord} contract downstream has no notion of pk/fk at
 * all, only "id" (record identity, join/group keys) — so a missing natural id has to be backfilled
 * from the surrogate {@code pk} for that table's rows to survive/enrich correctly downstream,
 * rather than silently dropping or failing to join.
 */
@Slf4j
final class CoreBuilderSupport {

  private static final ObjectMapper MAPPER = MapperUtil.MAPPER;

  // "urn:gbif:dwcdp:<table>:<pk>" convention — same one MaterialJoinBuilder's
  // virtualMaterialOccurrences already established for material-derived virtual occurrence ids.
  // Keeps a synthesised id distinguishable from a genuine publisher-supplied one, rather than
  // looking like an ordinary value once it lands in coreTerms/HBase/downstream debugging.
  static final String EVENT_URN_PREFIX = "urn:gbif:dwcdp:event:";
  static final String OCCURRENCE_URN_PREFIX = "urn:gbif:dwcdp:occurrence:";

  private CoreBuilderSupport() {}

  /**
   * Wraps {@code loader} so that, for {@code tableName} only, {@code idColumn} is guaranteed
   * non-null: existing values are preserved, and rows with a null/absent {@code idColumn} get
   * {@code urnPrefix + pkColumn} instead. Every other table (and every other consumer that later
   * calls {@code loader.load(tableName)} on the returned loader) sees the fix transparently — this
   * needs to be applied only once per builder entry point, not per join site.
   *
   * <p>{@code pkColumn} is required+unique per the DwC-DP profile, so it's always safe to fall back
   * to; it's also expected to be a real, stored, stable column from the source package (not
   * recomputed per Spark stage), so the synthesised id stays consistent across re-attempts — the
   * same assumption the HBase-keyed GBIF id lookup downstream already depends on.
   *
   * <p>The {@code urnPrefix} keeps synthesised ids recognisable as synthesised (as opposed to a
   * bare pk value, which is indistinguishable from a genuine publisher-supplied id) — e.g. {@code
   * "urn:gbif:dwcdp:event:"}, {@code "urn:gbif:dwcdp:occurrence:"}. Same convention already used by
   * {@code MaterialJoinBuilder#virtualMaterialOccurrences} for the material-derived virtual
   * occurrence case.
   *
   * @param loader the loader to wrap
   * @param tableName the table this fallback applies to; every other table passes through unchanged
   * @param pkColumn the table's required+unique surrogate key column (fallback source)
   * @param idColumn the table's optional natural id column (fallback target)
   * @param urnPrefix prefix applied to {@code pkColumn} when synthesising a fallback id
   */
  static TableLoader withIdFallback(
      TableLoader loader, String tableName, String pkColumn, String idColumn, String urnPrefix) {
    return name -> {
      Optional<Dataset<Row>> dfOpt = loader.load(name);
      if (!tableName.equals(name) || dfOpt.isEmpty()) {
        return dfOpt;
      }

      Dataset<Row> df = dfOpt.get();
      List<String> columns = Arrays.asList(df.columns());
      if (!columns.contains(pkColumn)) {
        // pk itself absent — nothing to fall back to; leave the table as-is (id absence, if any,
        // surfaces further downstream the same way it always has).
        return dfOpt;
      }

      Column fallback = functions.concat(functions.lit(urnPrefix), df.col(pkColumn));

      if (columns.contains(idColumn)) {
        log.warn(
            "{} table contains null {} values; filling those from {} (existing {} values are "
                + "preserved)",
            tableName,
            idColumn,
            pkColumn,
            idColumn);
        return Optional.of(df.withColumn(idColumn, functions.coalesce(df.col(idColumn), fallback)));
      }

      log.warn(
          "{} table has no {} column; falling back to {} as the record identifier ({} is "
              + "required+unique per the DwC-DP profile, {} is not — a legitimate, if unusual, "
              + "package shape)",
          tableName,
          idColumn,
          pkColumn,
          pkColumn,
          idColumn);
      return Optional.of(df.withColumn(idColumn, fallback));
    };
  }

  static void addExtensionIfPresent(
      Row row,
      Map<String, List<Map<String, String>>> extensions,
      boolean hasExtension,
      String jsonColumn,
      String rowType)
      throws IOException {
    if (!hasExtension) {
      return;
    }

    String json = RowTermMapper.safeGet(row, jsonColumn);
    if (json != null) {
      extensions.put(rowType, fromJson(json));
    }
  }

  @SuppressWarnings("unchecked")
  private static List<Map<String, String>> fromJson(String json) throws IOException {
    return MAPPER.readValue(json, List.class);
  }
}
