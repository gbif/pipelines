package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.gbif.pipelines.spark.dwcdp.mapping.MappingPlanBuilder.mappingPlan;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.CoreType;
import org.gbif.pipelines.spark.dwcdp.mapping.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.MappingPlanBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaPath;
import org.gbif.pipelines.spark.dwcdp.mapping.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.ValueAggregation;

/** Event-core mapping configurations assembled from reusable domain fragments. */
public final class EventDwcaMapping {

  private EventDwcaMapping() {}

  /**
   * Current legacy Humboldt behaviour. The survey-target path is optional, so one governing plan
   * covers datasets with targets, without targets, and with either optional target table absent.
   */
  public static MappingPlan withHumboldt(SchemaGraph graph) {
    MappingPlanBuilder.ExtensionBuilder humboldt =
        eventBase("event-core:humboldt")
            .extension(HumboldtMapping.ROW_TYPE_HUMBOLDT)
            .importFragment(HumboldtMapping.surveyTargets(graph))
            .importFragment(HumboldtMapping.surveyFields(graph))
            .importFragment(HumboldtMapping.samplingProtocol(graph))
            .importFragment(HumboldtMapping.samplingEffortProtocol(graph));
    return humboldt.build();
  }

  /**
   * Event-level Multimedia behaviour: direct event media plus occurrence and unambiguous
   * material media promoted to the event.
   */
  public static MappingPlan withMultimedia(SchemaGraph graph) {
    return eventBase("event-core:multimedia")
        .extension(MultimediaMapping.ROW_TYPE_MULTIMEDIA)
        .unionRows()
        .limitRowsPerParent(50)
        .importFragment(MultimediaMapping.eventMedia(graph))
        .importFragment(MultimediaMapping.occurrenceMediaForEvent(graph))
        .importFragment(MultimediaMapping.materialMediaForEvent(graph))
        .build();
  }

  /** Current legacy direct event-identifier behaviour. */
  public static MappingPlan withIdentifier(SchemaGraph graph) {
    return eventBase("event-core:identifier")
        .extension(IdentifierMapping.ROW_TYPE_IDENTIFIER)
        .importFragment(IdentifierMapping.eventIdentifiers(graph))
        .build();
  }

  /** Compatibility alias retained while the parity migration is in progress. */
  public static MappingPlan withHumboldtSurveyTargets(SchemaGraph graph) {
    return withHumboldt(graph);
  }

  /** Compatibility alias retained while the parity migration is in progress. */
  public static MappingPlan withHumboldtSurveyOnly(SchemaGraph graph) {
    return withHumboldt(graph);
  }

  private static MappingPlanBuilder eventBase(String name) {
    SchemaPath event = SchemaPath.root("event");
    return mappingPlan(name, CoreType.EVENT, "event")
        .coreField(
            TargetFieldMapping.oneOf(
                DwcTerm.eventID.qualifiedName(),
                ValueAggregation.firstNonNull(),
                event.field("eventID")));
  }
}
