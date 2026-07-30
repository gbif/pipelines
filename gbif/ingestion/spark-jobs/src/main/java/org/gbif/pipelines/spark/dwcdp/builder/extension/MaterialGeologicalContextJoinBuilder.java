package org.gbif.pipelines.spark.dwcdp.builder.extension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Adds Darwin Core geological-context terms to an occurrence from its single material evidence.
 *
 * <p>The target fields ({@code formation}, {@code bed}, chronostratigraphic ranges, etc.) are
 * single-valued in a DwC-A record, whereas DwC-DP permits a material to have multiple rows in
 * {@code material-geological-context}. This builder therefore applies the fields only when both
 * relationships are unambiguous: the occurrence has exactly one evidence material, and that
 * material has exactly one distinct geological-context link. Existing occurrence values always take
 * precedence.
 */
@Slf4j
public class MaterialGeologicalContextJoinBuilder {

  static final String TABLE_MATERIAL_GEOLOGICAL_CONTEXT = "material-geological-context";
  private static final String MATERIAL_ENTITY_PK_COLUMN = "materialEntity_pk";
  private static final String MATERIAL_ENTITY_FK_COLUMN = "materialEntity_fk";
  private static final String GEOLOGICAL_CONTEXT_PK_COLUMN = "geologicalContext_pk";
  private static final String GEOLOGICAL_CONTEXT_FK_COLUMN = "geologicalContext_fk";
  private static final String OCCURRENCE_ID_COLUMN = "occurrenceID";

  private MaterialGeologicalContextJoinBuilder() {}

  /**
   * Returns {@code occurrenceDf} with material-linked geological context fields where the link is
   * unambiguous, otherwise unchanged.
   */
  public static Dataset<Row> enrichOccurrences(TableLoader loader, Dataset<Row> occurrenceDf) {
    if (!Arrays.asList(occurrenceDf.columns()).contains(OCCURRENCE_ID_COLUMN)) {
      log.debug("occurrence table has no occurrenceID; skipping material geological context join");
      return occurrenceDf;
    }

    Optional<Dataset<Row>> materialGeoDfOpt = loader.load(TABLE_MATERIAL_GEOLOGICAL_CONTEXT);
    Optional<Dataset<Row>> geoDfOpt =
        loader.load(GeologicalContextJoinBuilder.TABLE_GEOLOGICAL_CONTEXT);
    Optional<Dataset<Row>> materialLinksOpt =
        MaterialJoinBuilder.singleMaterialOccurrenceLinks(loader);
    if (materialGeoDfOpt.isEmpty() || geoDfOpt.isEmpty() || materialLinksOpt.isEmpty()) {
      log.debug(
          "Skipping material geological context join: material-geological-context present={}, "
              + "geological-context present={}, unambiguous material links present={}",
          materialGeoDfOpt.isPresent(),
          geoDfOpt.isPresent(),
          materialLinksOpt.isPresent());
      return occurrenceDf;
    }

    Dataset<Row> materialGeoDf = materialGeoDfOpt.get();
    Dataset<Row> geoDf = geoDfOpt.get();
    if (!hasColumns(materialGeoDf, MATERIAL_ENTITY_FK_COLUMN, GEOLOGICAL_CONTEXT_FK_COLUMN)
        || !hasColumns(geoDf, GEOLOGICAL_CONTEXT_PK_COLUMN)) {
      log.warn("Cannot resolve material geological context: required FK or PK columns are absent");
      return occurrenceDf;
    }

    Dataset<Row> linksByOccurrence =
        materialGeoDf
            .select(
                functions.col(MATERIAL_ENTITY_FK_COLUMN),
                functions.col(GEOLOGICAL_CONTEXT_FK_COLUMN))
            .filter(
                functions
                    .col(MATERIAL_ENTITY_FK_COLUMN)
                    .isNotNull()
                    .and(functions.col(GEOLOGICAL_CONTEXT_FK_COLUMN).isNotNull()))
            .distinct()
            .join(
                materialLinksOpt.get(),
                functions
                    .col(MATERIAL_ENTITY_FK_COLUMN)
                    .equalTo(materialLinksOpt.get().col(MATERIAL_ENTITY_PK_COLUMN)),
                "inner")
            .select(
                materialLinksOpt.get().col(OCCURRENCE_ID_COLUMN),
                functions.col(GEOLOGICAL_CONTEXT_FK_COLUMN));

    Dataset<Row> singleContextOccurrences =
        linksByOccurrence
            .groupBy(functions.col(OCCURRENCE_ID_COLUMN))
            .count()
            .filter(functions.col("count").equalTo(1))
            .select(functions.col(OCCURRENCE_ID_COLUMN));

    Dataset<Row> oneContextPerOccurrence =
        linksByOccurrence
            .as("material_geo_link")
            .join(
                singleContextOccurrences.as("single_context_occurrence"),
                functions
                    .col("material_geo_link." + OCCURRENCE_ID_COLUMN)
                    .equalTo(functions.col("single_context_occurrence." + OCCURRENCE_ID_COLUMN)),
                "inner")
            .select(
                functions.col("material_geo_link." + OCCURRENCE_ID_COLUMN),
                functions.col("material_geo_link." + GEOLOGICAL_CONTEXT_FK_COLUMN));

    Dataset<Row> contextByOccurrence =
        oneContextPerOccurrence
            .join(
                geoDf,
                oneContextPerOccurrence
                    .col(GEOLOGICAL_CONTEXT_FK_COLUMN)
                    .equalTo(geoDf.col(GEOLOGICAL_CONTEXT_PK_COLUMN)),
                "inner")
            .select(contextColumns(occurrenceDf, oneContextPerOccurrence, geoDf));

    // A left join against an empty relation would add null-only geological columns to every
    // occurrence. Existing material builders treat that as no enrichment at all, preserving the
    // original schema as well as the original rows.
    if (contextByOccurrence.isEmpty()) {
      return occurrenceDf;
    }

    return occurrenceDf
        .join(
            contextByOccurrence,
            occurrenceDf
                .col(OCCURRENCE_ID_COLUMN)
                .equalTo(contextByOccurrence.col(OCCURRENCE_ID_COLUMN)),
            "left_outer")
        .select(mergedColumns(occurrenceDf, contextByOccurrence));
  }

  private static Column[] contextColumns(
      Dataset<Row> occurrenceDf, Dataset<Row> linksDf, Dataset<Row> geoDf) {
    Set<String> occurrenceColumns = new HashSet<>(Arrays.asList(occurrenceDf.columns()));
    List<Column> columns = new ArrayList<>();
    columns.add(linksDf.col(OCCURRENCE_ID_COLUMN));
    for (String column : geoDf.columns()) {
      if (!occurrenceColumns.contains(column)
          && !GEOLOGICAL_CONTEXT_PK_COLUMN.equals(column)
          && !GeologicalContextJoinBuilder.JOIN_KEY.equals(column)) {
        columns.add(geoDf.col(column));
      }
    }
    return columns.toArray(new Column[0]);
  }

  private static Column[] mergedColumns(Dataset<Row> occurrenceDf, Dataset<Row> contextDf) {
    Set<String> occurrenceColumns = new HashSet<>(Arrays.asList(occurrenceDf.columns()));
    List<Column> columns = new ArrayList<>();
    for (String column : occurrenceDf.columns()) {
      columns.add(occurrenceDf.col(column));
    }
    for (String column : contextDf.columns()) {
      if (!occurrenceColumns.contains(column)) {
        columns.add(contextDf.col(column));
      }
    }
    return columns.toArray(new Column[0]);
  }

  private static boolean hasColumns(Dataset<Row> df, String... columns) {
    return Arrays.asList(df.columns()).containsAll(Arrays.asList(columns));
  }
}
