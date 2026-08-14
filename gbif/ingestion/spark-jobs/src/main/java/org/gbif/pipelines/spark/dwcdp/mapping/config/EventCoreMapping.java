package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.gbif.pipelines.spark.dwcdp.mapping.CoreFragmentBuilder.coreFragment;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.CoreFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.CoreFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.RelationCardinality;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaPath;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaRelation;
import org.gbif.pipelines.spark.dwcdp.mapping.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.ValueAggregation;

/** Reusable Event-core enrichments, including explicit protocol contribution paths. */
public final class EventCoreMapping {

  private EventCoreMapping() {}

  /** Resolves event.parentEvent_fk to the parent event's natural eventID. */
  public static CoreFragment parentEvent(SchemaGraph graph) {
    SchemaPath event = SchemaPath.root("event");
    SchemaPath parent = event.append(graph.resolve("event", "event", "parentEvent_fk", null));
    return coreFragment("event-parent", "event")
        .join("event")
        .via("parentEvent_fk")
        .optional()
        .exactlyOne()
        .field(
            TargetFieldMapping.oneOf(
                DwcTerm.parentEventID.qualifiedName(),
                ValueAggregation.firstNonNull(),
                parent.field("eventID")))
        .build();
  }

  /** Weak/natural-key event.geologicalContextID -> geological-context.geologicalContextID. */
  public static CoreFragment geologicalContext(SchemaGraph graph) {
    SchemaPath event = SchemaPath.root("event");
    SchemaPath geologicalContext =
        event.append(
            SchemaRelation.relation(
                "event",
                "geologicalContextID",
                "geological-context",
                "geologicalContextID",
                null,
                RelationCardinality.UNKNOWN));

    CoreFragmentBuilder builder =
        coreFragment("event-geological-context", "event")
            .join("geological-context")
            .on("geologicalContextID", "geologicalContextID")
            .optional()
            .fanOut()
            .endJoin();
    DirectFieldMappings.from(graph, "geological-context", geologicalContext).addTo(builder);
    return builder.build();
  }

  /** Resolves eventConductedByID through agent.agentID while preserving an explicit publisher value. */
  public static CoreFragment eventConductedBy(SchemaGraph graph) {
    return agentName(
        "event-conducted-by-agent",
        "eventConductedByID",
        "eventConductedBy",
        TargetTerms.resolve("eventConductedBy"));
  }

  /** Resolves georeferencedByID through agent.agentID while preserving an explicit publisher value. */
  public static CoreFragment georeferencedBy(SchemaGraph graph) {
    return agentName(
        "event-georeferenced-by-agent",
        "georeferencedByID",
        "georeferencedBy",
        DwcTerm.georeferencedBy.qualifiedName());
  }


  /** Direct eventProtocol_fk -> protocolDescription, falling back to the raw FK when absent. */
  public static CoreFragment directSamplingProtocol(SchemaGraph graph) {
    SchemaPath event = SchemaPath.root("event");
    SchemaPath protocol =
        event.append(graph.resolve("event", "protocol", "eventProtocol_fk", null));
    return coreFragment("event-direct-sampling-protocol", "event")
        .join("protocol")
        .via("eventProtocol_fk")
        .optional()
        .exactlyOne()
        .field(
            TargetFieldMapping.oneOf(
                DwcTerm.samplingProtocol.qualifiedName(),
                ValueAggregation.labeledOrFallback(": "),
                protocol.field("protocolType"),
                protocol.field("protocolName"),
                protocol.field("protocolDescription"),
                event.field("eventProtocol_fk")))
        .build();
  }

  /** Publisher georeferenceProtocol wins; resolved description/raw FK only fills a gap. */
  public static CoreFragment directGeoreferenceProtocol(SchemaGraph graph) {
    SchemaPath event = SchemaPath.root("event");
    SchemaPath protocol =
        event.append(graph.resolve("event", "protocol", "georeferenceProtocol_fk", null));
    return coreFragment("event-direct-georeference-protocol", "event")
        .join("protocol")
        .via("georeferenceProtocol_fk")
        .optional()
        .exactlyOne()
        .field(
            TargetFieldMapping.oneOf(
                DwcTerm.georeferenceProtocol.qualifiedName(),
                ValueAggregation.preferredLabeledOrFallback(": "),
                event.field("georeferenceProtocol"),
                protocol.field("protocolType"),
                protocol.field("protocolName"),
                protocol.field("protocolDescription"),
                event.field("georeferenceProtocol_fk")))
        .build();
  }

  /** All protocols linked directly through event-protocol contribute to samplingProtocol. */
  public static CoreFragment eventProtocols(SchemaGraph graph) {
    return protocolJunction(
        graph,
        "event-protocols",
        false,
        false,
        DwcTerm.samplingProtocol.qualifiedName());
  }

  /** Protocols linked through event -> survey -> survey-protocol also contribute to samplingProtocol. */
  public static CoreFragment surveyProtocols(SchemaGraph graph) {
    return protocolJunction(
        graph,
        "survey-protocols",
        true,
        false,
        DwcTerm.samplingProtocol.qualifiedName());
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
    SchemaPath event = SchemaPath.root("event");
    SchemaPath link;
    CoreFragmentBuilder.RelationBuilder relation;
    if (throughSurvey) {
      SchemaPath survey = event.append(graph.resolve("event", "survey", "event_fk", null));
      link = survey.append(graph.resolve("survey", "survey-protocol", "survey_fk", null));
      relation =
          coreFragment(name, "event")
              .join("survey")
              .via("event_fk")
              .optional()
              .fanOut()
              .join("survey-protocol")
              .via("survey_fk")
              .optional()
              .fanOut();
    } else {
      link = event.append(graph.resolve("event", "event-protocol", "event_fk", null));
      relation =
          coreFragment(name, "event")
              .join("event-protocol")
              .via("event_fk")
              .optional()
              .fanOut();
    }

    SchemaPath protocol =
        link.append(
            graph.resolve(link.currentResource(), "protocol", "protocol_fk", null));
    CoreFragmentBuilder.RelationBuilder protocolJoin =
        relation.join("protocol").via("protocol_fk").optional().exactlyOne();
    if (georeferenceOnly) {
      protocolJoin =
          protocolJoin.filter(
              cols ->
                  cols.col("protocolType")
                      .isin("georeferencing", "georeference"));
    }

    return protocolJoin
        .field(
            TargetFieldMapping.oneOf(
                targetTerm,
                ValueAggregation.labeledOrFallback(": "),
                protocol.field("protocolType"),
                protocol.field("protocolName"),
                protocol.field("protocolDescription")))
        .build();
  }

  private static CoreFragment agentName(
      String name, String idColumn, String valueColumn, String targetTerm) {
    SchemaPath event = SchemaPath.root("event");
    SchemaPath agent =
        event.append(
            SchemaRelation.relation(
                "event", idColumn, "agent", "agentID", null, RelationCardinality.UNKNOWN));

    return coreFragment(name, "event")
        .join("agent")
        .on(idColumn, "agentID")
        .optional()
        .fanOut()
        .field(
            TargetFieldMapping.oneOf(
                targetTerm,
                ValueAggregation.firstNonNull(),
                event.field(valueColumn),
                agent.field("preferredAgentName")))
        .build();
  }
}
