package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.gbif.pipelines.spark.dwcdp.mapping.ExtensionFragmentBuilder.extensionFragment;

import org.gbif.dwc.terms.EcoTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.ExtensionFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaPath;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaResource;
import org.gbif.pipelines.spark.dwcdp.mapping.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.ValueAggregation;

/** Declarative mapping of the behaviour currently implemented by {@code HumboldtExtensionBuilder}. */
public final class HumboldtMapping {

  /** DwC-A Humboldt Ecological Inventory extension row type. */
  public static final String ROW_TYPE_HUMBOLDT = "http://rs.gbif.org/terms/1.0/Humboldt";

  private HumboldtMapping() {}

  /**
   * Defines one Humboldt row per survey target. Target fields themselves are copied through using
   * the same term resolution as the legacy mapper.
   */
  public static ExtensionFragment surveyTargets(SchemaGraph graph) {
    SchemaPath survey = SchemaPath.root("survey");
    SchemaPath link =
        survey.append(graph.resolve("survey", "survey-survey-target", "survey_fk", null));
    SchemaPath target =
        link.append(
            graph.resolve(
                "survey-survey-target", "survey-target", "surveyTarget_fk", null));

    ExtensionFragmentBuilder builder =
        extensionFragment("humboldt-survey-targets", ROW_TYPE_HUMBOLDT, "survey")
            .join("survey-survey-target")
            .via("survey_fk")
            .fanOut()
            .join("survey-target")
            .via("surveyTarget_fk")
            .exactlyOne()
            .rowIdentity("surveyTarget_pk");

    SchemaResource targetResource = requiredResource(graph, "survey-target");
    for (String column : targetResource.fields().keySet()) {
      if (isStructural(column)) {
        continue;
      }
      builder.field(
          TargetFieldMapping.oneOf(
              TargetTerms.resolve(column), ValueAggregation.firstNonNull(), target.field(column)));
    }
    return builder.build();
  }

  /** Defines a single Humboldt row per survey when there is no survey-target fan-out. */
  public static ExtensionFragment surveyRows() {
    return extensionFragment("humboldt-survey-row", ROW_TYPE_HUMBOLDT, "survey")
        .rowIdentity("survey_pk")
        .build();
  }

  /**
   * Survey-scoped fields copied onto every Humboldt row. Structural columns are excluded; target
   * ownership conflicts are deliberately detected later from compiled producer lineage rather than
   * hidden here with source-name exclusions.
   */
  public static ExtensionFragment surveyFields(SchemaGraph graph) {
    SchemaPath survey = SchemaPath.root("survey");
    ExtensionFragmentBuilder builder =
        extensionFragment("humboldt-survey-fields", ROW_TYPE_HUMBOLDT, "survey");

    DirectFieldMappings.from(graph, "survey", survey).addTo(builder);
    return builder.build();
  }

  /**
   * {@code survey.samplingProtocol} is represented as {@code eco:protocolDescriptions}. If the
   * publisher already supplied a textual value it wins; otherwise the protocol FK is resolved to
   * {@code protocol.protocolDescription}.
   */
  public static ExtensionFragment samplingProtocol(SchemaGraph graph) {
    SchemaPath survey = SchemaPath.root("survey");
    SchemaPath protocol =
        survey.append(graph.resolve("survey", "protocol", "samplingProtocol_fk", null));

    return extensionFragment("humboldt-sampling-protocol", ROW_TYPE_HUMBOLDT, "survey")
        .join("protocol")
        .via("samplingProtocol_fk")
        .optional()
        .exactlyOne()
        .field(
            TargetFieldMapping.oneOf(
                EcoTerm.protocolDescriptions.qualifiedName(),
                ValueAggregation.firstNonNull(),
                survey.field("samplingProtocol"),
                protocol.field("protocolDescription")))
        .build();
  }

  /** Publisher value wins; otherwise resolve {@code samplingEffortProtocol_fk} through protocol. */
  public static ExtensionFragment samplingEffortProtocol(SchemaGraph graph) {
    SchemaPath survey = SchemaPath.root("survey");
    SchemaPath protocol =
        survey.append(graph.resolve("survey", "protocol", "samplingEffortProtocol_fk", null));

    return extensionFragment("humboldt-sampling-effort-protocol", ROW_TYPE_HUMBOLDT, "survey")
        .join("protocol")
        .via("samplingEffortProtocol_fk")
        .optional()
        .exactlyOne()
        .field(
            TargetFieldMapping.oneOf(
                EcoTerm.samplingEffortProtocol.qualifiedName(),
                ValueAggregation.firstNonNull(),
                survey.field("samplingEffortProtocol"),
                protocol.field("protocolDescription")))
        .build();
  }

  private static boolean isStructural(String column) {
    return column.endsWith("_pk") || column.endsWith("_fk");
  }

  private static SchemaResource requiredResource(SchemaGraph graph, String name) {
    return graph.resource(name)
        .orElseThrow(() -> new IllegalArgumentException("DwC-DP schema has no resource " + name));
  }
}
