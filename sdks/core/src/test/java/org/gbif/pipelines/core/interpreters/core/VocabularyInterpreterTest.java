package org.gbif.pipelines.core.interpreters.core;

import java.util.HashMap;
import java.util.Map;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.interpreters.MockVocabularyLookups;
import org.gbif.pipelines.core.parsers.vocabulary.VocabularyService;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Assert;
import org.junit.Test;

public class VocabularyInterpreterTest {

  private static final String ID = "777";

  private final VocabularyService vocabularyService =
      VocabularyService.builder()
          .vocabularyLookup(
              DwcTerm.lifeStage.qualifiedName(),
              new MockVocabularyLookups.LifeStageMockVocabularyLookup())
          .build();

  @Test
  public void lifeStageEmptyValueTest() {
    // State
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(DwcTerm.lifeStage.qualifiedName(), "");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    VocabularyInterpreter.interpretLifeStage(vocabularyService).accept(er, br);

    // Should
    Assert.assertNull(br.getLifeStage());
  }

  @Test
  public void lifeStageRandomValueTest() {
    // State
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(DwcTerm.lifeStage.qualifiedName(), "adwadaw");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    VocabularyInterpreter.interpretLifeStage(vocabularyService).accept(er, br);

    // Should
    Assert.assertNull(br.getLifeStage());
  }

  @Test
  public void lifeStageAdultValueTest() {
    // State
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(DwcTerm.lifeStage.qualifiedName(), "adult");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    VocabularyInterpreter.interpretLifeStage(vocabularyService).accept(er, br);

    // Should
    Assert.assertEquals("Adult", br.getLifeStage().getConcept());
    Assert.assertEquals("Adult", br.getLifeStage().getLineage().get(0));
  }

  @Test
  public void lifeStageNotNullTest() {
    // State
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(DwcTerm.lifeStage.qualifiedName(), "adwadaw");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    VocabularyInterpreter.interpretLifeStage(vocabularyService).accept(er, br);

    // Should
    Assert.assertNull(br.getLifeStage());
  }
}
