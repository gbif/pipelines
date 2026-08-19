package org.gbif.pipelines.spark.dwcdp.mapping.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.RelationCardinality;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.RelationStep;
import org.junit.jupiter.api.Test;

class SchemaRelationResolverTest {

  private final InMemorySchemaGraph graph =
      new InMemorySchemaGraph()
          .resource("event", "event_pk", "eventID")
          .resource("occurrence", "event_fk", "occurrence_pk")
          .relation(
              new SchemaRelation(
                  "event",
                  "event_pk",
                  "occurrence",
                  "event_fk",
                  Optional.empty(),
                  RelationCardinality.ONE_TO_MANY));

  @Test
  void resolvesSchemaBackedRelation() {
    SchemaRelation relation =
        SchemaRelationResolver.resolve(graph, "event", RelationStep.inferred("occurrence"));

    assertEquals("event_pk", relation.sourceColumn());
    assertEquals("event_fk", relation.targetColumn());
    assertEquals(RelationCardinality.ONE_TO_MANY, relation.cardinality());
  }

  @Test
  void resolvesExplicitRelationAfterValidatingColumns() {
    RelationStep step = RelationStep.inferred("occurrence").on("eventID", "event_fk");

    SchemaRelation relation = SchemaRelationResolver.resolve(graph, "event", step);

    assertEquals("eventID", relation.sourceColumn());
    assertEquals("event_fk", relation.targetColumn());
    assertEquals(RelationCardinality.UNKNOWN, relation.cardinality());
  }

  @Test
  void rejectsUnknownExplicitSourceColumn() {
    RelationStep step = RelationStep.inferred("occurrence").on("missing", "event_fk");

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () -> SchemaRelationResolver.resolve(graph, "event", step));

    assertTrue(error.getMessage().contains("event.missing"));
  }

  @Test
  void rejectsUnknownExplicitTargetColumn() {
    RelationStep step = RelationStep.inferred("occurrence").on("eventID", "missing");

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () -> SchemaRelationResolver.resolve(graph, "event", step));

    assertTrue(error.getMessage().contains("occurrence.missing"));
  }
}
