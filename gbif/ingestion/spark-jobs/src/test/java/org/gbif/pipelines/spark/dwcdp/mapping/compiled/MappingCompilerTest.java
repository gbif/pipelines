package org.gbif.pipelines.spark.dwcdp.mapping.compiled;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.gbif.pipelines.spark.dwcdp.mapping.CoreFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.CoreFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.CoreType;
import org.gbif.pipelines.spark.dwcdp.mapping.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.InMemorySchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.MappingPlanBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.RelationCardinality;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaRelation;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.config.EventDwcaMapping;
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
        assertThrows(MappingCompilationException.class, () -> new MappingCompiler(graph).compile(plan));

    assertEquals(1, error.problems().size());
    assertEquals(MappingDecisionType.INVALID_RELATION, error.problems().get(0).type());
    assertEquals("core-fragment:bad-organism", error.problems().get(0).scope());
    assertTrue(error.getMessage().contains("occurrence.material_fk -> material.material_pk"));
    assertTrue(error.getMessage().contains("material.organism_fk -> organism.organism_pk"));
  }

}
