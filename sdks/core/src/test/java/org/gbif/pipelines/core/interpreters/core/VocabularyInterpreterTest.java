package org.gbif.pipelines.core.interpreters.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.OccurrenceIssue;
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
          .vocabularyLookup(
              DwcTerm.typeStatus.qualifiedName(),
              new MockVocabularyLookups.TypeStatusMockVocabularyLookup())
          .vocabularyLookup(
              DwcTerm.sex.qualifiedName(), new MockVocabularyLookups.SexMockVocabularyLookup())
          .build();

  @Test
  public void sexTest() {
    // State
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(DwcTerm.sex.qualifiedName(), "male");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    VocabularyInterpreter.interpretSex(vocabularyService).accept(er, br);

    // Should
    Assert.assertEquals("male", br.getSex().getConcept());
    Assert.assertEquals("male", br.getSex().getLineage().get(0));
  }

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

  @Test
  public void typeStatusTest() {
    final String tp1 = "Type";
    final String tp2 = "Allotype";

    // State
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(DwcTerm.typeStatus.qualifiedName(), tp1 + " | " + tp2 + " | ");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    VocabularyInterpreter.interpretTypeStatus(vocabularyService).accept(er, br);

    // Should
    Assert.assertEquals(2, br.getTypeStatus().size());
    Assert.assertTrue(br.getTypeStatus().stream().anyMatch(v -> v.getConcept().equals(tp1)));
    Assert.assertTrue(br.getTypeStatus().stream().anyMatch(v -> v.getConcept().equals(tp1)));
    assertIssueSize(br, 0);
  }

  @Test
  public void interpretTypeStatusFromIdentificationExtensionTest() {
    final String tp1 = "Type";
    final String tp2 = "Allotype";

    // State
    Map<String, List<Map<String, String>>> ext = new HashMap<>(1);
    Map<String, String> identification = new HashMap<>(1);
    identification.put(DwcTerm.typeStatus.qualifiedName(), tp1 + " | " + tp2 + " | ");
    ext.put(Extension.IDENTIFICATION.getRowType(), Collections.singletonList(identification));
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setExtensions(ext).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    VocabularyInterpreter.interpretTypeStatus(vocabularyService).accept(er, br);

    // Should
    Assert.assertEquals(2, br.getTypeStatus().size());
    Assert.assertTrue(br.getTypeStatus().stream().anyMatch(v -> v.getConcept().equals(tp1)));
    Assert.assertTrue(br.getTypeStatus().stream().anyMatch(v -> v.getConcept().equals(tp1)));
    assertIssueSize(br, 0);
  }

  @Test
  public void interpretTypeStatusCorePreferenceOverExtensionTest() {
    final String tp1 = "Type";
    final String tp2 = "Allotype";

    // State
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(DwcTerm.typeStatus.qualifiedName(), tp1);
    Map<String, List<Map<String, String>>> ext = new HashMap<>(1);
    Map<String, String> identification = new HashMap<>(1);
    identification.put(DwcTerm.typeStatus.qualifiedName(), tp2);
    ext.put(Extension.IDENTIFICATION.getRowType(), Collections.singletonList(identification));
    ExtendedRecord er =
        ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).setExtensions(ext).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    VocabularyInterpreter.interpretTypeStatus(vocabularyService).accept(er, br);

    // Should
    Assert.assertEquals(1, br.getTypeStatus().size());
    Assert.assertEquals(tp1, br.getTypeStatus().get(0).getConcept());
    assertIssueSize(br, 0);
  }

  @Test
  public void interpretTypeStatusIgnoreIdentificationExtensionTest() {
    final String tp1 = "Type";

    // State
    Map<String, String> coreMap = new HashMap<>(1);
    // we set another identification term and the extension should be ignored
    coreMap.put(DwcTerm.kingdom.qualifiedName(), "Animalia");
    Map<String, List<Map<String, String>>> ext = new HashMap<>(1);
    Map<String, String> identification = new HashMap<>(1);
    identification.put(DwcTerm.typeStatus.qualifiedName(), tp1);
    ext.put(Extension.IDENTIFICATION.getRowType(), Collections.singletonList(identification));
    ExtendedRecord er =
        ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).setExtensions(ext).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    VocabularyInterpreter.interpretTypeStatus(vocabularyService).accept(er, br);

    // Should
    Assert.assertEquals(0, br.getTypeStatus().size());
    assertIssueSize(br, 0);
  }

  @Test
  public void interpretTypeStatusPartiallyInvalidTest() {
    final String tp1 = "Type";
    final String tp2 = "invalid";

    // State
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(DwcTerm.typeStatus.qualifiedName(), tp1 + " | " + tp2 + " | ");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    VocabularyInterpreter.interpretTypeStatus(vocabularyService).accept(er, br);

    // Should
    Assert.assertEquals(1, br.getTypeStatus().size());
    Assert.assertEquals(tp1, br.getTypeStatus().get(0).getConcept());
    assertIssueSize(br, 1);
    assertIssue(OccurrenceIssue.TYPE_STATUS_INVALID, br);
  }

  @Test
  public void interpretTypeStatusWithSciNameTest() {
    final String tp1 = "Type of sci name";
    final String tp2 = "invalid of test";

    // State
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(DwcTerm.typeStatus.qualifiedName(), tp1 + " | " + tp2 + " | ");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    VocabularyInterpreter.interpretTypeStatus(vocabularyService).accept(er, br);

    // Should
    Assert.assertEquals(1, br.getTypeStatus().size());
    Assert.assertEquals("Type", br.getTypeStatus().get(0).getConcept());
    assertIssueSize(br, 1);
    assertIssue(OccurrenceIssue.TYPE_STATUS_INVALID, br);
  }

  @Test
  public void interpretTypeStatusSuspectedTypeTest() {
    final String tp1 = "possible";

    // State
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(DwcTerm.typeStatus.qualifiedName(), tp1);
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    VocabularyInterpreter.interpretTypeStatus(vocabularyService).accept(er, br);

    // Should
    Assert.assertEquals(0, br.getTypeStatus().size());
    assertIssueSize(br, 1);
    assertIssue(OccurrenceIssue.SUSPECTED_TYPE, br);
  }

  private void assertIssueSize(BasicRecord br, int expectedSize) {
    assertEquals(expectedSize, br.getIssues().getIssueList().size());
  }

  private void assertIssue(OccurrenceIssue expectedIssue, BasicRecord br) {
    assertTrue(br.getIssues().getIssueList().contains(expectedIssue.name()));
  }
}
