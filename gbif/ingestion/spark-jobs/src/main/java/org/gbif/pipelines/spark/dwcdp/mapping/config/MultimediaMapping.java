package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder.extensionFragment;

import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPath;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;

/** Declarative mappings for the Simple Multimedia extension. */
public final class MultimediaMapping {

  public static final String ROW_TYPE_MULTIMEDIA = Extension.MULTIMEDIA.getRowType();

  private MultimediaMapping() {}

  /** Direct event-media rows. */
  public static ExtensionFragment eventMedia(SchemaGraph graph) {
    MappingPath link = MappingPath.root(graph, "event-media");
    MappingPath media = link.join("media").via("media_fk").optional().exactlyOne();
    MappingPath usagePolicy =
        media.join("usage-policy").via("usagePolicy_fk").optional().exactlyOne();

    ExtensionFragmentBuilder builder =
        extensionFragment("event-media", ROW_TYPE_MULTIMEDIA, usagePolicy).scopeKey("event_fk");

    DirectFieldMappings.from(graph, "media", media).addTo(builder);
    DirectFieldMappings.from(graph, "usage-policy", usagePolicy).addTo(builder);
    return builder.build();
  }

  /** Direct occurrence-media rows attached to Occurrence core. */
  public static ExtensionFragment occurrenceMedia(SchemaGraph graph) {
    MappingPath link = MappingPath.root(graph, "occurrence-media");
    MappingPath media = link.join("media").via("media_fk").optional().exactlyOne();
    MappingPath usagePolicy =
        media.join("usage-policy").via("usagePolicy_fk").optional().exactlyOne();

    ExtensionFragmentBuilder builder =
        extensionFragment("occurrence-media", ROW_TYPE_MULTIMEDIA, usagePolicy)
            .scopeKey("occurrence_fk");

    DirectFieldMappings.from(graph, "media", media).addTo(builder);
    DirectFieldMappings.from(graph, "usage-policy", usagePolicy).addTo(builder);
    return builder.build();
  }

  /**
   * Material media attached to Occurrence core only when the occurrence has exactly one evidence
   * material.
   */
  public static ExtensionFragment materialMediaForOccurrence(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath material =
        occurrence.join("material").via("evidenceForOccurrenceID").optional().exactlyOne();
    MappingPath link = material.join("material-media").via("materialEntity_fk").optional().fanOut();
    MappingPath media = link.join("media").via("media_fk").optional().exactlyOne();
    MappingPath usagePolicy =
        media.join("usage-policy").via("usagePolicy_fk").optional().exactlyOne();

    ExtensionFragmentBuilder builder =
        extensionFragment("material-media-for-occurrence", ROW_TYPE_MULTIMEDIA, usagePolicy)
            .scopeKey("occurrence_pk");

    DirectFieldMappings.from(graph, "media", media).addTo(builder);
    DirectFieldMappings.from(graph, "usage-policy", usagePolicy).addTo(builder);
    return builder.build();
  }

  /** Occurrence media promoted to the owning event's top-level Multimedia extension. */
  public static ExtensionFragment occurrenceMediaForEvent(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath link = occurrence.join("occurrence-media").via("occurrence_fk").optional().fanOut();
    MappingPath media = link.join("media").via("media_fk").optional().exactlyOne();
    MappingPath usagePolicy =
        media.join("usage-policy").via("usagePolicy_fk").optional().exactlyOne();

    ExtensionFragmentBuilder builder =
        extensionFragment("occurrence-media-promoted-to-event", ROW_TYPE_MULTIMEDIA, usagePolicy)
            .scopeKey("event_fk")
            .rowIdentity(media.field("media_pk"));

    DirectFieldMappings.from(graph, "media", media).addTo(builder);
    DirectFieldMappings.from(graph, "usage-policy", usagePolicy).addTo(builder);
    addOccurrenceIdentity(builder, occurrence, media);
    return builder.build();
  }

  /**
   * Material media promoted through an occurrence only when that occurrence has exactly one
   * evidence material. The occurrence->material relation is the reverse traversal of the schema-
   * declared weak FK material.evidenceForOccurrenceID -> occurrence.occurrenceID.
   */
  public static ExtensionFragment materialMediaForEvent(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath material =
        occurrence.join("material").via("evidenceForOccurrenceID").optional().exactlyOne();
    MappingPath link = material.join("material-media").via("materialEntity_fk").optional().fanOut();
    MappingPath media = link.join("media").via("media_fk").optional().exactlyOne();
    MappingPath usagePolicy =
        media.join("usage-policy").via("usagePolicy_fk").optional().exactlyOne();

    ExtensionFragmentBuilder builder =
        extensionFragment("material-media-promoted-to-event", ROW_TYPE_MULTIMEDIA, usagePolicy)
            .scopeKey("event_fk")
            .rowIdentity(media.field("media_pk"));

    DirectFieldMappings.from(graph, "media", media).addTo(builder);
    DirectFieldMappings.from(graph, "usage-policy", usagePolicy).addTo(builder);
    addOccurrenceIdentity(builder, occurrence, media);
    return builder.build();
  }

  /** Media explicitly attached to an Event-owned Chronometric Age row. */
  public static ExtensionFragment chronometricAgeMediaForEvent(SchemaGraph graph) {
    MappingPath age = MappingPath.root(graph, "chronometric-age");
    MappingPath link =
        age.join("chronometric-age-media").via("chronometricAge_fk").optional().fanOut();
    MappingPath media = link.join("media").via("media_fk").optional().exactlyOne();
    MappingPath usagePolicy =
        media.join("usage-policy").via("usagePolicy_fk").optional().exactlyOne();

    ExtensionFragmentBuilder builder =
        extensionFragment("chronometric-age-media-for-event", ROW_TYPE_MULTIMEDIA, usagePolicy)
            .scopeKey("event_fk");

    DirectFieldMappings.from(graph, "media", media).addTo(builder);
    DirectFieldMappings.from(graph, "usage-policy", usagePolicy).addTo(builder);
    return builder.build();
  }

  /** Chronometric Age media promoted from the Occurrence's owning Event. */
  public static ExtensionFragment chronometricAgeMediaForOccurrence(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath event = occurrence.join("event").via("event_fk").optional().exactlyOne();
    MappingPath age = event.join("chronometric-age").via("event_fk").optional().fanOut();
    MappingPath link =
        age.join("chronometric-age-media").via("chronometricAge_fk").optional().fanOut();
    MappingPath media = link.join("media").via("media_fk").optional().exactlyOne();
    MappingPath usagePolicy =
        media.join("usage-policy").via("usagePolicy_fk").optional().exactlyOne();

    ExtensionFragmentBuilder builder =
        extensionFragment("chronometric-age-media-for-occurrence", ROW_TYPE_MULTIMEDIA, usagePolicy)
            .scopeKey("occurrence_pk");

    DirectFieldMappings.from(graph, "media", media).addTo(builder);
    DirectFieldMappings.from(graph, "usage-policy", usagePolicy).addTo(builder);
    return builder.build();
  }

  /**
   * Marks an Event-level extension row as belonging to a specific Occurrence. The existing
   * Event-core downstream escape hatch uses dwc:occurrenceID to route such extension rows to the
   * extracted occurrence.
   */
  private static void addOccurrenceIdentity(
      ExtensionFragmentBuilder builder, MappingPath occurrence, MappingPath media) {
    builder.field(
        TargetFieldMapping.oneOf(
            DwcTerm.occurrenceID.qualifiedName(),
            ValueAggregation.firstOrUrnFallback("urn:gbif:dwcdp:occurrence:"),
            occurrence.field("occurrenceID"),
            occurrence.field("occurrence_pk")));
  }
}
