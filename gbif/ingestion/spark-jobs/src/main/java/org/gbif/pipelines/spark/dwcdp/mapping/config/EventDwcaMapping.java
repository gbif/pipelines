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

  /** Current legacy Humboldt behaviour for packages using survey-target fan-out. */
  public static MappingPlan withHumboldtSurveyTargets(SchemaGraph graph) {
    MappingPlanBuilder.ExtensionBuilder humboldt = eventBase("event-core:humboldt-targets")
        .extension(HumboldtMapping.ROW_TYPE_HUMBOLDT)
        .importFragment(HumboldtMapping.surveyTargets(graph))
        .importFragment(HumboldtMapping.surveyFields(graph))
        .importFragment(HumboldtMapping.samplingProtocol(graph))
        .importFragment(HumboldtMapping.samplingEffortProtocol(graph));
    return humboldt.build();
  }

  /** Current legacy Humboldt behaviour for survey packages without survey-target rows. */
  public static MappingPlan withHumboldtSurveyOnly(SchemaGraph graph) {
    MappingPlanBuilder.ExtensionBuilder humboldt = eventBase("event-core:humboldt-survey-only")
        .extension(HumboldtMapping.ROW_TYPE_HUMBOLDT)
        .importFragment(HumboldtMapping.surveyRows())
        .importFragment(HumboldtMapping.surveyFields(graph))
        .importFragment(HumboldtMapping.samplingProtocol(graph))
        .importFragment(HumboldtMapping.samplingEffortProtocol(graph));
    return humboldt.build();
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
