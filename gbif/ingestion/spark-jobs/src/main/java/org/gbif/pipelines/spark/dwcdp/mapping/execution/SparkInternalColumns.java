package org.gbif.pipelines.spark.dwcdp.mapping.execution;

/** Reserved physical columns used only while executing a mapping. */
final class SparkInternalColumns {
  private static final String NESTED_CONTEXT_LINK_PREFIX = "__dwcdp_nested_context_link__";

  private SparkInternalColumns() {}

  static boolean isNestedContextLink(String column) {
    return column.startsWith(NESTED_CONTEXT_LINK_PREFIX);
  }

  static String nestedContextLink(String leftColumn, String rightColumn) {
    return leftColumn.compareTo(rightColumn) <= 0
        ? NESTED_CONTEXT_LINK_PREFIX + leftColumn + "__" + rightColumn
        : NESTED_CONTEXT_LINK_PREFIX + rightColumn + "__" + leftColumn;
  }
}
