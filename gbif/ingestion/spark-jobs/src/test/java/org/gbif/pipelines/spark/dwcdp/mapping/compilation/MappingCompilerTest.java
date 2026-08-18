package org.gbif.pipelines.spark.dwcdp.mapping.compilation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.gbif.pipelines.spark.dwcdp.mapping.config.EventDwcaMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.CoreFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.CoreFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.CoreType;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPlanBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.RelationCardinality;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.InMemorySchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
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
}
