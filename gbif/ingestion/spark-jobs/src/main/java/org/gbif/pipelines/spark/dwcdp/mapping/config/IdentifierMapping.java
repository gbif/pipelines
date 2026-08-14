package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.gbif.pipelines.spark.dwcdp.mapping.ExtensionFragmentBuilder.extensionFragment;

import org.gbif.pipelines.spark.dwcdp.mapping.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.ExtensionFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaPath;

/** Declarative mappings for the GBIF Identifier extension. */
public final class IdentifierMapping {

  /** Extension.IDENTIFIER.getRowType(). */
  public static final String ROW_TYPE_IDENTIFIER = "http://rs.gbif.org/terms/1.0/Identifier";

  private IdentifierMapping() {}

  /**
   * Direct {@code event-identifier} rows attached to Event core records.
   *
   * <p>The identifier table is itself the extension row source. Its structural PK/FK fields are
   * omitted by {@link DirectFieldMappings}; all ordinary fields are inferred through the same
   * target-term resolver used by the legacy builder.
   */
  public static ExtensionFragment eventIdentifiers(SchemaGraph graph) {
    SchemaPath identifiers = SchemaPath.root("event-identifier");
    ExtensionFragmentBuilder builder =
        extensionFragment(
                "event-identifiers", ROW_TYPE_IDENTIFIER, "event-identifier")
            .scopeKey("event_fk");

    DirectFieldMappings.from(graph, "event-identifier", identifiers).addTo(builder);
    return builder.build();
  }
}
