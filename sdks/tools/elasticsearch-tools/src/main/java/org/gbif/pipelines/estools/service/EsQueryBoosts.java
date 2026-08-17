package org.gbif.pipelines.estools.service;

import java.util.List;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Query-time replacements for mapping {@code boost} values removed in Elasticsearch 8+.
 *
 * <p>Use {@link #queryFields(String)} as {@code multi_match}/{@code query_string} fields, or {@link
 * #defaultFieldSetting(String)} as {@code index.query.default_field} when creating an index.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class EsQueryBoosts {

  public static final int TAXON_BOOST = 100;

  public static List<String> occurrenceQueryFields() {
    return List.of(
        "all",
        "taxonID^" + TAXON_BOOST,
        "taxonConceptID^" + TAXON_BOOST,
        "verbatimScientificName^" + TAXON_BOOST);
  }

  public static List<String> eventQueryFields() {
    return List.of("all", "taxonIDs^" + TAXON_BOOST);
  }

  public static List<String> queryFields(String schemaPath) {
    return isEventSchema(schemaPath) ? eventQueryFields() : occurrenceQueryFields();
  }

  public static String defaultFieldSetting(String schemaPath) {
    return queryFields(schemaPath).stream()
        .map(field -> "\"" + field + "\"")
        .collect(Collectors.joining(",", "[", "]"));
  }

  static boolean isEventSchema(String schemaPath) {
    return schemaPath != null && schemaPath.contains("es-event");
  }
}
