package org.gbif.pipelines.spark.dwcdp.mapping.compilation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
import org.gbif.pipelines.spark.dwcdp.mapping.config.EventDwcaMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.CoreFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.CoreFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.CoreType;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionRowComposition;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldRef;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPlanBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.RelationCardinality;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetMerge;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.InMemorySchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaPath;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaRelation;
import org.junit.jupiter.api.Test;

class MappingCompilerTest {

  @Test
  void traceRetainsFragmentOwnershipAndPathQualifiedSources() {
    SchemaGraph graph = new DwcDpSchemaLoader().current();
    MappingPlan plan = EventDwcaMapping.withHumboldtSurveyTargets(graph);

    CompiledMapping compiled = new MappingCompiler(graph).compile(plan);
    String trace = MappingTraceRenderer.render(compiled);

    assertTrue(trace.contains("Fragment: humboldt-sampling-protocol"));
    assertTrue(trace.contains("survey.samplingProtocol"));
    assertTrue(trace.contains("protocolDescription"));
    assertTrue(trace.contains("surveyTargetDescription"));
    assertTrue(trace.contains("samplingProtocol_fk"));
  }

  @Test
  void invalidSchemaRelationIsAContextualMappingProblemWithBoundedPathHints() {
    InMemorySchemaGraph graph =
        new InMemorySchemaGraph()
            .resource("occurrence", "occurrence_pk", "organismID", "material_fk")
            .resource("material", "material_pk", "organism_fk")
            .resource("organism", "organismID", "organism_pk")
            .relation(
                SchemaRelation.relation(
                    "occurrence",
                    "material_fk",
                    "material",
                    "material_pk",
                    null,
                    RelationCardinality.MANY_TO_ONE))
            .relation(
                SchemaRelation.relation(
                    "material",
                    "organism_fk",
                    "organism",
                    "organism_pk",
                    null,
                    RelationCardinality.MANY_TO_ONE));

    CoreFragment badFragment =
        CoreFragmentBuilder.coreFragment("bad-organism", "occurrence")
            .join("organism")
            .via("organismID")
            .endJoin()
            .build();
    MappingPlan plan =
        MappingPlanBuilder.mappingPlan("bad-relation", CoreType.OCCURRENCE, "occurrence")
            .importCoreFragment(badFragment)
            .build();

    MappingCompilationException error =
        assertThrows(
            MappingCompilationException.class, () -> new MappingCompiler(graph).compile(plan));

    assertEquals(1, error.problems().size());
    assertEquals(MappingDecisionType.INVALID_RELATION, error.problems().get(0).type());
    assertEquals("core-fragment:bad-organism", error.problems().get(0).scope());
    assertTrue(error.getMessage().contains("occurrence.material_fk -> material.material_pk"));
    assertTrue(error.getMessage().contains("material.organism_fk -> organism.organism_pk"));
  }

  @Test
  void missingCoreMergeProducerFailsDuringCompilationWithContext() {
    InMemorySchemaGraph graph = new InMemorySchemaGraph().resource("event", "event_pk");
    MappingPlan plan =
        MappingPlanBuilder.mappingPlan("missing-core-merge", CoreType.EVENT, "event")
            .mergeCoreTarget("urn:test:missing", ValueAggregation.firstNonNull())
            .build();

    MappingCompilationException error =
        assertThrows(
            MappingCompilationException.class, () -> new MappingCompiler(graph).compile(plan));

    assertEquals(1, error.problems().size());
    MappingDecision problem = error.problems().get(0);
    assertEquals(MappingDecisionType.MISSING_MERGE_PRODUCERS, problem.type());
    assertEquals("core:event", problem.scope());
    assertEquals("urn:test:missing", problem.targetTerm());
    assertTrue(error.getMessage().contains("FirstNonNull"));
    assertTrue(error.getMessage().contains("Available producers in this scope: <none>"));
  }

  @Test
  void missingExtensionMergeProducerFailsDuringCompilationWithAvailableProducerContext() {
    InMemorySchemaGraph graph = new InMemorySchemaGraph().resource("event", "event_pk", "eventID");
    ExtensionFragment fragment =
        ExtensionFragmentBuilder.extensionFragment("event-row", "urn:test:extension", "event")
            .scopeKey("event_pk")
            .field(
                TargetFieldMapping.oneOf(
                    "urn:test:existing",
                    ValueAggregation.firstNonNull(),
                    new FieldRef(SchemaPath.root("event"), "eventID")))
            .build();
    ExtensionMapping extension =
        new ExtensionMapping(
            "urn:test:extension",
            ExtensionRowComposition.ENRICH,
            Optional.empty(),
            List.of(new TargetMerge("urn:test:missing", ValueAggregation.firstNonNull())),
            List.of(fragment));

    MappingCompilationException error =
        assertThrows(
            MappingCompilationException.class, () -> new MappingCompiler(graph).compile(extension));

    assertEquals(1, error.problems().size());
    MappingDecision problem = error.problems().get(0);
    assertEquals(MappingDecisionType.MISSING_MERGE_PRODUCERS, problem.type());
    assertEquals("extension:urn:test:extension", problem.scope());
    assertEquals("urn:test:missing", problem.targetTerm());
    assertTrue(error.getMessage().contains("event-row -> urn:test:existing [EXPLICIT]"));
  }
}
