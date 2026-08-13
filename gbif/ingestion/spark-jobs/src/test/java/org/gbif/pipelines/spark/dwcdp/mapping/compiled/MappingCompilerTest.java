package org.gbif.pipelines.spark.dwcdp.mapping.compiled;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.gbif.pipelines.spark.dwcdp.mapping.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.MappingPlan;
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
}
