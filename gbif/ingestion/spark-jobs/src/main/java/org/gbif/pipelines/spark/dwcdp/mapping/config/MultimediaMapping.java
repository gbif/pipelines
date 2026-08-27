package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder.extensionFragment;

import java.util.List;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FilterExpression;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPath;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;

/** Declarative mappings for the Simple Multimedia extension. */
public final class MultimediaMapping {

  public static final String ROW_TYPE_MULTIMEDIA = Extension.MULTIMEDIA.getRowType();

  private MultimediaMapping() {}

  /** All Event-core Multimedia row producers plus media-owned metadata enrichments. */
  public static List<ExtensionFragment> eventFragments(SchemaGraph graph) {
    return List.of(
        eventMedia(graph),
        occurrenceMediaForEvent(graph),
        materialMediaForEvent(graph),
        chronometricAgeMediaForEvent(graph),
        usagePolicy(graph),
        creators(graph));
  }

  /** All Occurrence-core Multimedia row producers plus media-owned metadata enrichments. */
  public static List<ExtensionFragment> occurrenceFragments(SchemaGraph graph) {
    return List.of(
        occurrenceMedia(graph),
        materialMediaForOccurrence(graph),
        chronometricAgeMediaForOccurrence(graph),
        usagePolicy(graph),
        creators(graph));
  }

  /** Direct event-media rows. */
  public static ExtensionFragment eventMedia(SchemaGraph graph) {
    MappingPath link = MappingPath.root(graph, "event-media");
    MappingPath media = link.join("media").via("media_fk").optional().exactlyOne();

    ExtensionFragmentBuilder builder =
        extensionFragment("event-media", ROW_TYPE_MULTIMEDIA, media)
            .scopeKey("event_fk")
            .rowIdentity(media.field("media_pk"));

    DirectFieldMappings.from(graph, "media", media).addTo(builder);
    return builder.build();
  }

  /** Direct occurrence-media rows attached to Occurrence core. */
  public static ExtensionFragment occurrenceMedia(SchemaGraph graph) {
    MappingPath link = MappingPath.root(graph, "occurrence-media");
    MappingPath media = link.join("media").via("media_fk").optional().exactlyOne();

    ExtensionFragmentBuilder builder =
        extensionFragment("occurrence-media", ROW_TYPE_MULTIMEDIA, media)
            .scopeKey("occurrence_fk")
            .rowIdentity(media.field("media_pk"));

    DirectFieldMappings.from(graph, "media", media).addTo(builder);
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

    ExtensionFragmentBuilder builder =
        extensionFragment("material-media-for-occurrence", ROW_TYPE_MULTIMEDIA, media)
            .scopeKey("occurrence_pk")
            .rowIdentity(media.field("media_pk"));

    DirectFieldMappings.from(graph, "media", media).addTo(builder);
    return builder.build();
  }

  /** Occurrence media promoted to the owning event's top-level Multimedia extension. */
  public static ExtensionFragment occurrenceMediaForEvent(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath link = occurrence.join("occurrence-media").via("occurrence_fk").optional().fanOut();
    MappingPath media = link.join("media").via("media_fk").optional().exactlyOne();

    ExtensionFragmentBuilder builder =
        extensionFragment("occurrence-media-promoted-to-event", ROW_TYPE_MULTIMEDIA, media)
            .scopeKey("event_fk")
            .rowIdentity(media.field("media_pk"));

    DirectFieldMappings.from(graph, "media", media).addTo(builder);
    OccurrenceExtensionRouting.addOccurrenceId(builder, occurrence);
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

    ExtensionFragmentBuilder builder =
        extensionFragment("material-media-promoted-to-event", ROW_TYPE_MULTIMEDIA, media)
            .scopeKey("event_fk")
            .rowIdentity(media.field("media_pk"));

    DirectFieldMappings.from(graph, "media", media).addTo(builder);
    OccurrenceExtensionRouting.addOccurrenceId(builder, occurrence);
    return builder.build();
  }

  /** Media explicitly attached to an Event-owned Chronometric Age row. */
  public static ExtensionFragment chronometricAgeMediaForEvent(SchemaGraph graph) {
    MappingPath age = MappingPath.root(graph, "chronometric-age");
    MappingPath link =
        age.join("chronometric-age-media").via("chronometricAge_fk").optional().fanOut();
    MappingPath media = link.join("media").via("media_fk").optional().exactlyOne();

    ExtensionFragmentBuilder builder =
        extensionFragment("chronometric-age-media-for-event", ROW_TYPE_MULTIMEDIA, media)
            .scopeKey("event_fk")
            .rowIdentity(media.field("media_pk"));

    DirectFieldMappings.from(graph, "media", media).addTo(builder);
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

    ExtensionFragmentBuilder builder =
        extensionFragment("chronometric-age-media-for-occurrence", ROW_TYPE_MULTIMEDIA, media)
            .scopeKey("occurrence_pk")
            .rowIdentity(media.field("media_pk"));

    DirectFieldMappings.from(graph, "media", media).addTo(builder);
    return builder.build();
  }

  /** License/rights metadata owned by a media row through media-usage-policy. */
  public static ExtensionFragment usagePolicy(SchemaGraph graph) {
    MappingPath media = MappingPath.root(graph, "media");
    MappingPath link =
        media.join("media-usage-policy").via("media_fk").optional().fanOut();
    MappingPath usagePolicy =
        link.join("usage-policy").via("usagePolicy_fk").optional().exactlyOne();

    ExtensionFragmentBuilder builder =
        extensionFragment("media-usage-policy", ROW_TYPE_MULTIMEDIA, usagePolicy)
            .rowMatch(media.field("media_pk"));
    DirectFieldMappings.from(graph, "usage-policy", usagePolicy).addTo(builder);
    builder.field(
        TargetFieldMapping.oneOf(
            DcTerm.license.qualifiedName(),
            ValueAggregation.firstNonNull(),
            usagePolicy.field("license")));
    return builder.build();
  }

  /** Ordered creator agents owned by a media row through media-agent-role. */
  public static ExtensionFragment creators(SchemaGraph graph) {
    MappingPath media = MappingPath.root(graph, "media");
    MappingPath role =
        media
            .join("media-agent-role")
            .via("media_fk")
            .filter(FilterExpression.eq("agentRole", "creator"))
            .optional()
            .fanOut();
    MappingPath agent = role.join("agent").via("agent_fk").optional().exactlyOne();

    return extensionFragment("media-creators", ROW_TYPE_MULTIMEDIA, agent)
        .rowMatch(media.field("media_pk"))
        .field(
            TargetFieldMapping.allOf(
                    DcTerm.creator.qualifiedName(),
                    ValueAggregation.pipeDelimitedDistinct(),
                    agent.field("preferredAgentName"))
                .contributionIdentity(role.field("agent_fk"))
                .orderBy(role.field("agentRoleOrder")))
        .build();
  }
}
