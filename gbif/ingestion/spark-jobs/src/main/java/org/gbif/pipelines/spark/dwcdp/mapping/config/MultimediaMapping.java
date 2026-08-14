package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.gbif.pipelines.spark.dwcdp.mapping.ExtensionFragmentBuilder.extensionFragment;

import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.spark.dwcdp.mapping.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.ExtensionFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaPath;

/** Declarative mappings for the Simple Multimedia extension. */
public final class MultimediaMapping {

  public static final String ROW_TYPE_MULTIMEDIA = Extension.MULTIMEDIA.getRowType();

  private MultimediaMapping() {}

  /** Direct event-media rows. */
  public static ExtensionFragment eventMedia(SchemaGraph graph) {
    SchemaPath link = SchemaPath.root("event-media");
    SchemaPath media = link.append(graph.resolve("event-media", "media", "media_fk", null));
    SchemaPath usagePolicy =
        media.append(graph.resolve("media", "usage-policy", "usagePolicy_fk", null));

    ExtensionFragmentBuilder builder =
        extensionFragment("event-media", ROW_TYPE_MULTIMEDIA, "event-media")
            .scopeKey("event_fk")
            .join("media")
            .via("media_fk")
            .optional()
            .exactlyOne()
            .join("usage-policy")
            .via("usagePolicy_fk")
            .optional()
            .exactlyOne()
            .endJoin();

    DirectFieldMappings.from(graph, "media", media).addTo(builder);
    DirectFieldMappings.from(graph, "usage-policy", usagePolicy).addTo(builder);
    return builder.build();
  }


  /** Direct occurrence-media rows attached to Occurrence core. */
  public static ExtensionFragment occurrenceMedia(SchemaGraph graph) {
    SchemaPath link = SchemaPath.root("occurrence-media");
    SchemaPath media =
        link.append(graph.resolve("occurrence-media", "media", "media_fk", null));
    SchemaPath usagePolicy =
        media.append(graph.resolve("media", "usage-policy", "usagePolicy_fk", null));

    ExtensionFragmentBuilder builder =
        extensionFragment("occurrence-media", ROW_TYPE_MULTIMEDIA, "occurrence-media")
            .scopeKey("occurrence_fk")
            .join("media")
            .via("media_fk")
            .optional()
            .exactlyOne()
            .join("usage-policy")
            .via("usagePolicy_fk")
            .optional()
            .exactlyOne()
            .endJoin();

    DirectFieldMappings.from(graph, "media", media).addTo(builder);
    DirectFieldMappings.from(graph, "usage-policy", usagePolicy).addTo(builder);
    return builder.build();
  }

  /**
   * Material media attached to Occurrence core only when the occurrence has exactly one evidence
   * material.
   */
  public static ExtensionFragment materialMediaForOccurrence(SchemaGraph graph) {
    SchemaPath occurrence = SchemaPath.root("occurrence");
    SchemaPath material =
        occurrence.append(graph.resolve("occurrence", "material", "evidenceForOccurrenceID", null));
    SchemaPath link =
        material.append(graph.resolve("material", "material-media", "materialEntity_fk", null));
    SchemaPath media =
        link.append(graph.resolve("material-media", "media", "media_fk", null));
    SchemaPath usagePolicy =
        media.append(graph.resolve("media", "usage-policy", "usagePolicy_fk", null));

    ExtensionFragmentBuilder builder =
        extensionFragment("material-media-for-occurrence", ROW_TYPE_MULTIMEDIA, "occurrence")
            .scopeKey("occurrence_pk")
            .join("material")
            .via("evidenceForOccurrenceID")
            .optional()
            .exactlyOne()
            .join("material-media")
            .via("materialEntity_fk")
            .optional()
            .fanOut()
            .join("media")
            .via("media_fk")
            .optional()
            .exactlyOne()
            .join("usage-policy")
            .via("usagePolicy_fk")
            .optional()
            .exactlyOne()
            .endJoin();

    DirectFieldMappings.from(graph, "media", media).addTo(builder);
    DirectFieldMappings.from(graph, "usage-policy", usagePolicy).addTo(builder);
    return builder.build();
  }

  /** Occurrence media promoted to the owning event's top-level Multimedia extension. */
  public static ExtensionFragment occurrenceMediaForEvent(SchemaGraph graph) {
    SchemaPath occurrence = SchemaPath.root("occurrence");
    SchemaPath link =
        occurrence.append(
            graph.resolve("occurrence", "occurrence-media", "occurrence_fk", null));
    SchemaPath media =
        link.append(graph.resolve("occurrence-media", "media", "media_fk", null));
    SchemaPath usagePolicy =
        media.append(graph.resolve("media", "usage-policy", "usagePolicy_fk", null));

    ExtensionFragmentBuilder builder =
        extensionFragment("occurrence-media-promoted-to-event", ROW_TYPE_MULTIMEDIA, "occurrence")
            .scopeKey("event_fk")
            .join("occurrence-media")
            .via("occurrence_fk")
            .optional()
            .fanOut()
            .join("media")
            .via("media_fk")
            .optional()
            .exactlyOne()
            .join("usage-policy")
            .via("usagePolicy_fk")
            .optional()
            .exactlyOne()
            .endJoin();

    DirectFieldMappings.from(graph, "media", media).addTo(builder);
    DirectFieldMappings.from(graph, "usage-policy", usagePolicy).addTo(builder);
    return builder.build();
  }

  /**
   * Material media promoted through an occurrence only when that occurrence has exactly one
   * evidence material. The occurrence->material relation is the reverse traversal of the schema-
   * declared weak FK material.evidenceForOccurrenceID -> occurrence.occurrenceID.
   */
  public static ExtensionFragment materialMediaForEvent(SchemaGraph graph) {
    SchemaPath occurrence = SchemaPath.root("occurrence");
    SchemaPath material =
        occurrence.append(graph.resolve("occurrence", "material", "evidenceForOccurrenceID", null));
    SchemaPath link =
        material.append(graph.resolve("material", "material-media", "materialEntity_fk", null));
    SchemaPath media =
        link.append(graph.resolve("material-media", "media", "media_fk", null));
    SchemaPath usagePolicy =
        media.append(graph.resolve("media", "usage-policy", "usagePolicy_fk", null));

    ExtensionFragmentBuilder builder =
        extensionFragment("material-media-promoted-to-event", ROW_TYPE_MULTIMEDIA, "occurrence")
            .scopeKey("event_fk")
            .join("material")
            .via("evidenceForOccurrenceID")
            .optional()
            .exactlyOne()
            .join("material-media")
            .via("materialEntity_fk")
            .optional()
            .fanOut()
            .join("media")
            .via("media_fk")
            .optional()
            .exactlyOne()
            .join("usage-policy")
            .via("usagePolicy_fk")
            .optional()
            .exactlyOne()
            .endJoin();

    DirectFieldMappings.from(graph, "media", media).addTo(builder);
    DirectFieldMappings.from(graph, "usage-policy", usagePolicy).addTo(builder);
    return builder.build();
  }
}
