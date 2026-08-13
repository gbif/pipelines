package org.gbif.pipelines.spark.dwcdp.mapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;

class MappingPlanCompositionTest {

  private static final String HUMBOLDT = "http://rs.gbif.org/terms/1.0/Humboldt";
  private static final String MULTIMEDIA = "http://rs.gbif.org/terms/1.0/Multimedia";

  @Test
  void sameExtensionFragmentCanBeImportedByEventAndOccurrencePlans() {
    ExtensionFragment media =
        ExtensionFragmentBuilder.extensionFragment("media", MULTIMEDIA, "media")
            .rowIdentity("media_pk")
            .build();

    MappingPlan event =
        MappingPlanBuilder.mappingPlan("event-core", CoreType.EVENT, "event")
            .extension(MULTIMEDIA)
            .importFragment(media)
            .build();

    MappingPlan occurrence =
        MappingPlanBuilder.mappingPlan("occurrence-core", CoreType.OCCURRENCE, "occurrence")
            .extension(MULTIMEDIA)
            .importFragment(media)
            .build();

    assertSame(media, event.extensions().get(0).fragments().get(0));
    assertSame(media, occurrence.extensions().get(0).fragments().get(0));
  }

  @Test
  void multipleFragmentsCanContributeToOneExtension() {
    ExtensionFragment survey =
        ExtensionFragmentBuilder.extensionFragment("survey", HUMBOLDT, "survey").build();
    ExtensionFragment surveyAgents =
        ExtensionFragmentBuilder.extensionFragment("survey-agents", HUMBOLDT, "survey")
            .join("survey-agent-role")
            .fanOut()
            .join("agent")
            .exactlyOne()
            .build();

    MappingPlan plan =
        MappingPlanBuilder.mappingPlan("event-core", CoreType.EVENT, "event")
            .extension(HUMBOLDT)
            .importFragment(survey)
            .importFragment(surveyAgents)
            .build();

    assertEquals(1, plan.extensions().size());
    assertEquals(2, plan.extensions().get(0).fragments().size());
  }
}
