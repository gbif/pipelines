package org.gbif.pipelines.spark.dwcdp.mapping.compilation;

import org.gbif.pipelines.spark.dwcdp.mapping.compilation.MappingValidator;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.ValidationResult;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldSource;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.Mapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.RelationCardinality;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.InMemorySchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaRelation;
import static org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldSource.field;
import static org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingBuilder.mapping;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class MappingValidatorTest {

  private final InMemorySchemaGraph graph =
      new InMemorySchemaGraph()
          .resource("survey", "survey_pk")
          .resource("survey-agent-role", "survey_fk", "agent_fk", "role")
          .resource("agent", "agent_pk", "preferredAgentName")
          .relation(
              new SchemaRelation(
                  "survey",
                  "survey_pk",
                  "survey-agent-role",
                  "survey_fk",
                  java.util.Optional.of("survey role"),
                  RelationCardinality.ONE_TO_MANY))
          .relation(
              new SchemaRelation(
                  "survey-agent-role",
                  "agent_fk",
                  "agent",
                  "agent_pk",
                  java.util.Optional.of("role holder"),
                  RelationCardinality.MANY_TO_ONE));

  @Test
  void validatesSurveyAgentPath() {
    Mapping mapping =
        mapping("survey-agents", "survey")
            .join("survey-agent-role")
            .fanOut()
            .join("agent")
            .target(
                TargetMapping.allOf(
                    "agents",
                    ValueAggregation.pipeDelimitedDistinct(),
                    field("agent", "preferredAgentName"),
                    field("survey-agent-role", "role")))
            .build();

    assertTrue(MappingValidator.validate(mapping, graph).isValid());
  }

  @Test
  void rejectsToManyJoinWithoutStrategy() {
    Mapping mapping = mapping("unsafe-survey-agents", "survey").join("survey-agent-role").build();

    ValidationResult result = MappingValidator.validate(mapping, graph);

    assertFalse(result.isValid());
    assertTrue(
        result.issues().stream()
            .anyMatch(issue -> issue.message().contains("no explicit cardinality strategy")));
  }

  @Test
  void rejectsUnknownTargetColumn() {
    Mapping mapping =
        mapping("bad-field", "survey")
            .target(
                TargetMapping.oneOf(
                    "something", ValueAggregation.firstNonNull(), field("survey", "missing")))
            .build();

    assertFalse(MappingValidator.validate(mapping, graph).isValid());
  }
}
