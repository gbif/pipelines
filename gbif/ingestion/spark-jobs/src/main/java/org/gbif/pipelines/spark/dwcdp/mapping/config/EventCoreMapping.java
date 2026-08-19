package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.gbif.pipelines.spark.dwcdp.mapping.definition.CoreFragmentBuilder.coreFragment;

import java.util.List;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.CoreFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.CoreFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FilterExpression;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPath;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;

/** Reusable Event-core enrichments, including explicit protocol contribution paths. */
public final class EventCoreMapping {

  private EventCoreMapping() {}

  /** Resolves event.parentEvent_fk to the parent event's natural eventID. */
  public static CoreFragment parentEvent(SchemaGraph graph) {
    MappingPath event = MappingPath.root(graph, "event");
    MappingPath parent = event.join("event").via("parentEvent_fk").optional().exactlyOne();
    return coreFragment("event-parent", parent)
        .field(
            TargetFieldMapping.oneOf(
                DwcTerm.parentEventID.qualifiedName(),
                ValueAggregation.firstNonNull(),
                parent.field("eventID")))
        .build();
  }

  /** Weak/natural-key event.geologicalContextID -> geological-context.geologicalContextID. */
  public static CoreFragment geologicalContext(SchemaGraph graph) {
    MappingPath event = MappingPath.root(graph, "event");
    MappingPath geologicalContext =
        event
            .join("geological-context")
            .on("geologicalContextID", "geologicalContextID")
            .optional()
            .fanOut();

    CoreFragmentBuilder builder = coreFragment("event-geological-context", geologicalContext);
    DirectFieldMappings.from(graph, "geological-context", geologicalContext).addTo(builder);
    return builder.build();
  }

  /**
   * Resolves eventConductedByID through agent.agentID while preserving an explicit publisher value.
   */
  public static CoreFragment eventConductedBy(SchemaGraph graph) {
    return AgentMapping.core(
        graph,
        new AgentMapping.Spec(
            "event-conducted-by-agent",
            "event",
            "eventConductedByID",
            "eventConductedBy",
            TargetTerms.resolve("eventConductedBy")));
  }

  /**
   * Resolves georeferencedByID through agent.agentID while preserving an explicit publisher value.
   */
  public static CoreFragment georeferencedBy(SchemaGraph graph) {
    return AgentMapping.core(
        graph,
        new AgentMapping.Spec(
            "event-georeferenced-by-agent",
            "event",
            "georeferencedByID",
            "georeferencedBy",
            DwcTerm.georeferencedBy.qualifiedName()));
  }

  /** Direct eventProtocol_fk -> resolved samplingProtocol when the referenced protocol exists. */
  public static CoreFragment directSamplingProtocol(SchemaGraph graph) {
    MappingPath event = MappingPath.root(graph, "event");
    MappingPath protocol = event.join("protocol").via("eventProtocol_fk").optional().exactlyOne();
    return coreFragment("event-direct-sampling-protocol", protocol)
        .field(
            TargetFieldMapping.oneOf(
                DwcTerm.samplingProtocol.qualifiedName(),
                ValueAggregation.labeledOrFallback(": "),
                protocol.field("protocolType"),
                protocol.field("protocolName"),
                protocol.field("protocolDescription")))
        .build();
  }

  /** Publisher georeferenceProtocol wins; otherwise use the resolved protocol value. */
  public static CoreFragment directGeoreferenceProtocol(SchemaGraph graph) {
    MappingPath event = MappingPath.root(graph, "event");
    MappingPath protocol =
        event.join("protocol").via("georeferenceProtocol_fk").optional().exactlyOne();
    return coreFragment("event-direct-georeference-protocol", protocol)
        .field(
            TargetFieldMapping.oneOf(
                DwcTerm.georeferenceProtocol.qualifiedName(),
                ValueAggregation.preferredLabeledOrFallback(": "),
                event.field("georeferenceProtocol"),
                protocol.field("protocolType"),
                protocol.field("protocolName"),
                protocol.field("protocolDescription")))
        .build();
  }

  /** All protocols linked directly through event-protocol contribute to samplingProtocol. */
  public static CoreFragment eventProtocols(SchemaGraph graph) {
    return protocolJunction(
        graph, "event-protocols", false, false, DwcTerm.samplingProtocol.qualifiedName());
  }

  /**
   * Protocols linked through event -> survey -> survey-protocol also contribute to
   * samplingProtocol.
   */
  public static CoreFragment surveyProtocols(SchemaGraph graph) {
    return protocolJunction(
        graph, "survey-protocols", true, false, DwcTerm.samplingProtocol.qualifiedName());
  }

  /** Georeferencing-typed event protocols additionally contribute to georeferenceProtocol. */
  public static CoreFragment eventGeoreferenceProtocols(SchemaGraph graph) {
    return protocolJunction(
        graph,
        "event-georeference-protocols",
        false,
        true,
        DwcTerm.georeferenceProtocol.qualifiedName());
  }

  /** Georeferencing-typed survey protocols additionally contribute to georeferenceProtocol. */
  public static CoreFragment surveyGeoreferenceProtocols(SchemaGraph graph) {
    return protocolJunction(
        graph,
        "survey-georeference-protocols",
        true,
        true,
        DwcTerm.georeferenceProtocol.qualifiedName());
  }

  private static CoreFragment protocolJunction(
      SchemaGraph graph,
      String name,
      boolean throughSurvey,
      boolean georeferenceOnly,
      String targetTerm) {
    MappingPath event = MappingPath.root(graph, "event");
    MappingPath link;
    if (throughSurvey) {
      MappingPath survey = event.join("survey").via("event_fk").optional().fanOut();
      link = survey.join("survey-protocol").via("survey_fk").optional().fanOut();
    } else {
      link = event.join("event-protocol").via("event_fk").optional().fanOut();
    }

    MappingPath.JoinBuilder protocolJoin = link.join("protocol").via("protocol_fk").optional();
    if (georeferenceOnly) {
      protocolJoin.filter(
          FilterExpression.optionalIn("protocolType", "georeferencing", "georeference"));
    }
    MappingPath protocol = protocolJoin.exactlyOne();

    return coreFragment(name, protocol)
        .field(
            TargetFieldMapping.oneOf(
                targetTerm,
                ValueAggregation.labeledOrFallback(": "),
                protocol.field("protocolType"),
                protocol.field("protocolName"),
                protocol.field("protocolDescription")))
        .build();
  }

  /** Direct event.provenance_fk -> provenance contribution rows. */
  public static CoreFragment directProvenance(SchemaGraph graph) {
    MappingPath event = MappingPath.root(graph, "event");
    MappingPath provenance = event.join("provenance").via("provenance_fk").optional().exactlyOne();
    CoreFragmentBuilder builder = coreFragment("event-direct-provenance", provenance);
    addProvenanceTargets(builder, provenance);
    return builder.build();
  }

  /** event -> event-provenance -> provenance contribution rows. */
  public static CoreFragment eventProvenance(SchemaGraph graph) {
    MappingPath event = MappingPath.root(graph, "event");
    MappingPath link = event.join("event-provenance").via("event_fk").optional().fanOut();
    MappingPath provenance = link.join("provenance").via("provenance_fk").optional().exactlyOne();
    CoreFragmentBuilder builder = coreFragment("event-provenance", provenance);
    addProvenanceTargets(builder, provenance);
    return builder.build();
  }

  private static void addProvenanceTargets(CoreFragmentBuilder builder, MappingPath provenance) {
    for (String field :
        List.of("fundingAttribution", "fundingAttributionID", "projectID", "projectTitle")) {
      builder.field(
          TargetFieldMapping.oneOf(
                  TargetTerms.resolve(field),
                  ValueAggregation.firstNonNull(),
                  provenance.field(field))
              .contributionIdentity(provenance.field("provenance_pk"))
              .orderBy(provenance.field("provenanceID")));
    }
  }
}
