package org.gbif.pipelines.core.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.core.interpreters.model.VocabularyConcept;
import org.gbif.pipelines.core.interpreters.model.VocabularyTag;
import org.gbif.vocabulary.lookup.LookupConcept;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class VocabularyConceptFactory {

  /**
   * Extracts the value of vocabulary concept and set
   *
   * @param c to extract the value from
   */
  public static VocabularyConcept createConcept(LookupConcept c) {
    return createConcept(c.getConcept().getName(), c.getParents(), null);
  }

  public static VocabularyConcept createConcept(LookupConcept c, Map<String, String> tagsMap) {
    return createConcept(c.getConcept().getName(), c.getParents(), tagsMap);
  }

  public static VocabularyConcept createConcept(
      String conceptName, List<LookupConcept.Parent> parents, Map<String, String> tagsMap) {
    // we sort the parents starting from the top as in taxonomy
    List<String> sortedParents =
        parents.stream().map(LookupConcept.Parent::getName).collect(Collectors.toList());
    Collections.reverse(sortedParents);

    // add the concept itself
    sortedParents.add(conceptName);

    VocabularyConcept.Builder builder =
        VocabularyConcept.newBuilder()
            .setConcept(conceptName)
            .setLineage(new ArrayList<>(sortedParents));

    if (tagsMap != null) {
      builder.setTags(tagsMapToVocabularyTags(tagsMap));
    }

    return builder.build();
  }

  private static List<VocabularyTag> tagsMapToVocabularyTags(Map<String, String> tagsMap) {
    return tagsMap.entrySet().stream()
        .map(v -> VocabularyTag.newBuilder().setName(v.getKey()).setValue(v.getValue()).build())
        .collect(Collectors.toList());
  }
}
