package org.gbif.pipelines.core.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.io.avro.VocabularyConcept;
import org.gbif.pipelines.io.avro.VocabularyTag;
import org.gbif.vocabulary.lookup.LookupConcept;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class VocabularyUtils {

  /**
   * Extracts the value of vocabulary concept and set
   *
   * @param c to extract the value from
   */
  public static VocabularyConcept getConcept(LookupConcept c) {
    return getConcept(c.getConcept().getName(), c.getParents(), null);
  }

  public static VocabularyConcept getConcept(LookupConcept c, Map<String, String> tagsMap) {
    return getConcept(c.getConcept().getName(), c.getParents(), tagsMap);
  }

  public static VocabularyConcept getConcept(
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

  public static List<VocabularyTag> tagsMapToVocabularyTags(Map<String, String> tagsMap) {
    return tagsMap.entrySet().stream()
        .map(v -> VocabularyTag.newBuilder().setName(v.getKey()).setValue(v.getValue()).build())
        .collect(Collectors.toList());
  }
}
