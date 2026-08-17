package org.gbif.pipelines.spark.dwcdp.mapping.compiled;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.gbif.pipelines.spark.dwcdp.mapping.CoreType;
import org.gbif.pipelines.spark.dwcdp.mapping.RelationCardinality;
import org.gbif.pipelines.spark.dwcdp.mapping.RelationRequirement;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaPath;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaRelation;
import org.gbif.pipelines.spark.dwcdp.mapping.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.ValueAggregation;
import org.junit.jupiter.api.Test;

class CompiledMappingDatasetPrunerReachabilityTest {

  @Test
  void requiredRelationIsRemovedWhenNoSurvivingDependencyUsesItsPath() {
    SchemaPath event = SchemaPath.root("event");
    SchemaRelation unusedProtocol =
        SchemaRelation.relation(
            "event",
            "eventProtocol_fk",
            "protocol",
            "protocol_pk",
            null,
            RelationCardinality.MANY_TO_ONE);

    CompiledTargetProducer eventDate =
        new CompiledTargetProducer(
            "http://rs.tdwg.org/dwc/terms/eventDate",
            "direct-event",
            TargetFieldMapping.SourceMode.ONE_OF,
            new ValueAggregation.FirstNonNull(),
            List.of(new CompiledSourceField(event.field("eventDate"))),
            TargetFieldMapping.Origin.EXPLICIT,
            Optional.empty(),
            Optional.empty());

    CompiledCoreFragment fragment =
        new CompiledCoreFragment(
            "direct-event",
            "event",
            event,
            List.of(
                new CompiledRelationStep(
                    unusedProtocol,
                    false,
                    RelationRequirement.REQUIRED,
                    Optional.empty(),
                    null)),
            List.of(eventDate));

    CompiledMapping mapping =
        new CompiledMapping(
            "test",
            CoreType.EVENT,
            "event",
            List.of(),
            List.of(fragment),
            List.of(),
            List.of(),
            List.of());

    MappingDatasetScope scope =
        new MappingDatasetScope(
            Map.of(
                "event", Set.of("event_pk", "eventID", "eventDate", "eventProtocol_fk"),
                "protocol", Set.of("protocol_pk")));

    CompiledMapping pruned = new CompiledMappingDatasetPruner().prune(mapping, scope);

    assertTrue(pruned.coreFragments().get(0).relations().isEmpty());
  }
}
