package org.gbif.pipelines.spark.dwcdp.mapping.compilation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.gbif.pipelines.spark.dwcdp.mapping.definition.CoreType;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPlanBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.RelationCardinality;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.InMemorySchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaPath;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaRelation;
import org.junit.jupiter.api.Test;

class MappingCompilerPrecedenceTest {

  private static final String ROW_TYPE = "urn:test:extension";
  private static final String TARGET = "urn:test:target";

  @Test
  void explicitProducerOutranksInferredProducer() {
    InMemorySchemaGraph graph =
        new InMemorySchemaGraph().resource("survey", "survey_pk", "direct", "explicit");
    SchemaPath survey = SchemaPath.root("survey");

    ExtensionFragment inferred =
        ExtensionFragmentBuilder.extensionFragment("inferred", ROW_TYPE, "survey")
            .scopeKey("survey_pk")
            .field(
                TargetFieldMapping.inferredOneOf(
                    TARGET, ValueAggregation.firstNonNull(), survey.field("direct")))
            .build();
    ExtensionFragment explicit =
        ExtensionFragmentBuilder.extensionFragment("explicit", ROW_TYPE, "survey")
            .scopeKey("survey_pk")
            .field(
                TargetFieldMapping.oneOf(
                    TARGET, ValueAggregation.firstNonNull(), survey.field("explicit")))
            .build();

    CompiledMapping compiled = new MappingCompiler(graph).compile(plan(inferred, explicit));
    CompiledExtension extension = compiled.extensions().get(0);

    MappingDecision decision =
        extension.decisions().stream()
            .filter(candidate -> TARGET.equals(candidate.targetTerm()))
            .findFirst()
            .orElseThrow();
    assertEquals(MappingDecisionType.EXPLICIT_OVERRIDE, decision.type());
    assertEquals("explicit", decision.selected().orElseThrow().owner());
    assertEquals(1, extension.fragments().stream().flatMap(f -> f.targets().stream()).count());
  }

  @Test
  void closestInferredProducerWins() {
    InMemorySchemaGraph graph = graphWithChild();
    SchemaPath root = SchemaPath.root("root");
    SchemaPath child = root.append(graph.resolve("root", "child", "child_fk", null));

    ExtensionFragment direct =
        ExtensionFragmentBuilder.extensionFragment("direct", ROW_TYPE, "root")
            .scopeKey("root_pk")
            .field(
                TargetFieldMapping.inferredOneOf(
                    TARGET, ValueAggregation.firstNonNull(), root.field("value")))
            .build();
    ExtensionFragment related =
        ExtensionFragmentBuilder.extensionFragment("related", ROW_TYPE, "root")
            .scopeKey("root_pk")
            .join("child")
            .via("child_fk")
            .optional()
            .exactlyOne()
            .field(
                TargetFieldMapping.inferredOneOf(
                    TARGET, ValueAggregation.firstNonNull(), child.field("value")))
            .build();

    CompiledExtension extension =
        new MappingCompiler(graph).compile(plan(direct, related)).extensions().get(0);
    MappingDecision decision =
        extension.decisions().stream()
            .filter(candidate -> TARGET.equals(candidate.targetTerm()))
            .findFirst()
            .orElseThrow();

    assertEquals(MappingDecisionType.INFERRED_CLOSEST, decision.type());
    assertEquals("direct", decision.selected().orElseThrow().owner());
  }

  @Test
  void equalDepthInferredProducersFailBeforeExecution() {
    InMemorySchemaGraph graph = new InMemorySchemaGraph().resource("root", "root_pk", "a", "b");
    SchemaPath root = SchemaPath.root("root");

    ExtensionFragment first =
        ExtensionFragmentBuilder.extensionFragment("first", ROW_TYPE, "root")
            .scopeKey("root_pk")
            .field(
                TargetFieldMapping.inferredOneOf(
                    TARGET, ValueAggregation.firstNonNull(), root.field("a")))
            .build();
    ExtensionFragment second =
        ExtensionFragmentBuilder.extensionFragment("second", ROW_TYPE, "root")
            .scopeKey("root_pk")
            .field(
                TargetFieldMapping.inferredOneOf(
                    TARGET, ValueAggregation.firstNonNull(), root.field("b")))
            .build();

    MappingCompilationException error =
        assertThrows(
            MappingCompilationException.class,
            () -> new MappingCompiler(graph).compile(plan(first, second)));
    assertTrue(error.getMessage().contains("AMBIGUOUS_EQUAL_DEPTH"));
    assertTrue(error.getMessage().contains("first"));
    assertTrue(error.getMessage().contains("second"));
  }

  @Test
  void multipleExplicitProducersStillFail() {
    InMemorySchemaGraph graph = new InMemorySchemaGraph().resource("root", "root_pk", "a", "b");
    SchemaPath root = SchemaPath.root("root");

    ExtensionFragment first =
        ExtensionFragmentBuilder.extensionFragment("first", ROW_TYPE, "root")
            .scopeKey("root_pk")
            .field(
                TargetFieldMapping.oneOf(TARGET, ValueAggregation.firstNonNull(), root.field("a")))
            .build();
    ExtensionFragment second =
        ExtensionFragmentBuilder.extensionFragment("second", ROW_TYPE, "root")
            .scopeKey("root_pk")
            .field(
                TargetFieldMapping.oneOf(TARGET, ValueAggregation.firstNonNull(), root.field("b")))
            .build();

    MappingCompilationException error =
        assertThrows(
            MappingCompilationException.class,
            () -> new MappingCompiler(graph).compile(plan(first, second)));
    assertTrue(error.getMessage().contains("AMBIGUOUS_MULTIPLE_EXPLICIT"));
  }

  @Test
  void missingFragmentScopeIsReportedAsCompilationProblem() {
    InMemorySchemaGraph graph = new InMemorySchemaGraph().resource("root", "value");
    SchemaPath root = SchemaPath.root("root");

    ExtensionFragment fragment =
        ExtensionFragmentBuilder.extensionFragment("keyless", ROW_TYPE, "root")
            .field(
                TargetFieldMapping.oneOf(
                    TARGET, ValueAggregation.firstNonNull(), root.field("value")))
            .build();

    MappingCompilationException error =
        assertThrows(
            MappingCompilationException.class,
            () -> new MappingCompiler(graph).compile(plan(fragment)));

    assertEquals(1, error.problems().size());
    assertEquals(MappingDecisionType.MISSING_FRAGMENT_SCOPE, error.problems().get(0).type());
  }

  private static MappingPlan plan(ExtensionFragment... fragments) {
    MappingPlanBuilder.ExtensionBuilder extension =
        MappingPlanBuilder.mappingPlan("test", CoreType.EVENT, "root").extension(ROW_TYPE);
    for (ExtensionFragment fragment : fragments) {
      extension.importFragment(fragment);
    }
    return extension.build();
  }

  private static InMemorySchemaGraph graphWithChild() {
    return new InMemorySchemaGraph()
        .resource("root", "root_pk", "child_fk", "value")
        .resource("child", "child_pk", "value")
        .relation(
            SchemaRelation.relation(
                "root", "child_fk", "child", "child_pk", "child", RelationCardinality.MANY_TO_ONE));
  }
}
