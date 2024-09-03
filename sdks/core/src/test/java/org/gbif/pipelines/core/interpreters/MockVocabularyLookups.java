package org.gbif.pipelines.core.interpreters;

import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.gbif.vocabulary.lookup.LookupConcept;
import org.gbif.vocabulary.lookup.VocabularyLookup;
import org.gbif.vocabulary.model.Concept;
import org.gbif.vocabulary.model.LanguageRegion;

public class MockVocabularyLookups {

  public static class LifeStageMockVocabularyLookup implements VocabularyLookup {

    @Override
    public Optional<LookupConcept> lookup(String s) {
      return lookup(s, null);
    }

    @Override
    public Optional<LookupConcept> lookup(String s, LanguageRegion languageRegion) {
      if (s.equalsIgnoreCase("adult")) {
        Concept concept = new Concept();
        concept.setName("Adult");
        LookupConcept lookupConcept = LookupConcept.of(concept, new ArrayList<>(1), null);

        return Optional.of(lookupConcept);
      }
      return Optional.empty();
    }

    @Override
    public void close() {}
  }

  public static class TypeStatusMockVocabularyLookup implements VocabularyLookup {

    @Override
    public Optional<LookupConcept> lookup(String s) {
      return lookup(s, null);
    }

    @Override
    public Optional<LookupConcept> lookup(String s, LanguageRegion languageRegion) {
      if ("invalid".equalsIgnoreCase(s)) {
        return Optional.empty();
      }

      if ("possible".equalsIgnoreCase(s)) {
        return Optional.empty();
      }

      if (!Strings.isNullOrEmpty(s)) {
        Concept concept = new Concept();
        concept.setName(s);
        LookupConcept lookupConcept = LookupConcept.of(concept, new ArrayList<>(1), null);

        return Optional.of(lookupConcept);
      }

      return Optional.empty();
    }

    @Override
    public void close() {}
  }

  public static class GeoTimeMockVocabularyLookup implements VocabularyLookup {

    private static final List<String> CENOZOIC_TAGS =
        List.of("rank: Era", "startAge: 66.0", "endAge: 0");
    private static final List<String> MESOZOIC_TAGS =
        List.of("rank: Era", "startAge: 251.902", "endAge: 66.0");
    private static final List<String> PALEOZOIC_TAGS =
        List.of("rank: Era", "startAge: 538.8", "endAge: 251.902");
    private static final List<String> NEOGENE_TAGS =
        List.of("rank: Period", "startAge: 23.03", "endAge: 2.58");
    private static final List<String> QUATERNARY_TAGS =
        List.of("rank: Period", "startAge: 2.58", "endAge: 0");
    private static final List<String> MIOCENE_TAGS =
        List.of("rank: Epoch", "startAge: 23.03", "endAge: 5.333");
    private static final List<String> PLIOCENE_TAGS =
        List.of("rank: Epoch", "startAge: 5.333", "endAge: 2.58");
    private static final List<String> BURDIGALIAN_TAGS =
        List.of("rank: Age", "startAge: 20.44", "endAge: 15.98");
    private static final List<String> ZANCLEAN_TAGS =
        List.of("rank: Age", "startAge: 5.333", "endAge: 3.600");

    @Override
    public Optional<LookupConcept> lookup(String s) {
      return lookup(s, null);
    }

    @Override
    public Optional<LookupConcept> lookup(String s, LanguageRegion languageRegion) {
      if (s.equalsIgnoreCase("cenozoic")) {
        return Optional.of(LookupConcept.of(concept("Cenozoic"), new ArrayList<>(), CENOZOIC_TAGS));
      }
      if (s.equalsIgnoreCase("mesozoic")) {
        return Optional.of(LookupConcept.of(concept("Mesozoic"), new ArrayList<>(), MESOZOIC_TAGS));
      }
      if (s.equalsIgnoreCase("paleozoic")) {
        return Optional.of(
            LookupConcept.of(concept("Paleozoic"), new ArrayList<>(), PALEOZOIC_TAGS));
      }
      if (s.equalsIgnoreCase("neogene")) {
        return Optional.of(
            LookupConcept.of(
                concept("Neogene"),
                Collections.singletonList(parent("Cenozoic", CENOZOIC_TAGS)),
                NEOGENE_TAGS));
      }
      if (s.equalsIgnoreCase("quaternary")) {
        return Optional.of(
            LookupConcept.of(
                concept("Quaternary"),
                Collections.singletonList(parent("Cenozoic", CENOZOIC_TAGS)),
                QUATERNARY_TAGS));
      }

      if (s.equalsIgnoreCase("miocene")) {
        return Optional.of(
            LookupConcept.of(
                concept("Miocene"),
                List.of(parent("Neogene", NEOGENE_TAGS), parent("Cenozoic", CENOZOIC_TAGS)),
                MIOCENE_TAGS));
      }

      if (s.equalsIgnoreCase("pliocene")) {
        return Optional.of(
            LookupConcept.of(
                concept("Pliocene"),
                List.of(parent("Neogene", NEOGENE_TAGS), parent("Cenozoic", CENOZOIC_TAGS)),
                PLIOCENE_TAGS));
      }

      if (s.equalsIgnoreCase("burdigalian")) {
        return Optional.of(
            LookupConcept.of(
                concept("Burdigalian"),
                List.of(
                    parent("Miocene", MIOCENE_TAGS),
                    parent("Neogene", NEOGENE_TAGS),
                    parent("Cenozoic", CENOZOIC_TAGS)),
                BURDIGALIAN_TAGS));
      }

      if (s.equalsIgnoreCase("zanclean")) {
        return Optional.of(
            LookupConcept.of(
                concept("Zanclean"),
                List.of(
                    parent("Pliocene", PLIOCENE_TAGS),
                    parent("Neogene", NEOGENE_TAGS),
                    parent("Cenozoic", CENOZOIC_TAGS)),
                ZANCLEAN_TAGS));
      }

      return Optional.empty();
    }

    private static Concept concept(String name) {
      Concept concept = new Concept();
      concept.setName(name);
      return concept;
    }

    private static LookupConcept.Parent parent(String name, List<String> tags) {
      return LookupConcept.Parent.of(null, null, name, tags);
    }

    @Override
    public void close() {}
  }
}
