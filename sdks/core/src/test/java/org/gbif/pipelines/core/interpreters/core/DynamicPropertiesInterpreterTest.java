package org.gbif.pipelines.core.interpreters.core;

import java.util.Collections;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.interpreters.MockVocabularyLookups;
import org.gbif.pipelines.core.parsers.vocabulary.VocabularyService;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Assert;
import org.junit.Test;

public class DynamicPropertiesInterpreterTest {

  private static final String ID = "777";

  private final BiFunction<DwcTerm, String, ExtendedRecord> erFn =
      (term, value) ->
          ExtendedRecord.newBuilder()
              .setId(ID)
              .setCoreTerms(Collections.singletonMap(term.qualifiedName(), value))
              .build();

  private final Function<String, ExtendedRecord> erDynamicPropertiesFn =
      value -> erFn.apply(DwcTerm.dynamicProperties, value);

  private final Supplier<BasicRecord> brFn = () -> BasicRecord.newBuilder().setId(ID).build();

  private final VocabularyService vocabularyService =
      VocabularyService.builder()
          .vocabularyLookup(
              DwcTerm.lifeStage.qualifiedName(),
              new MockVocabularyLookups.LifeStageMockVocabularyLookup())
          .build();

  @Test
  public void sexFemaleValueTest() {
    // State
    ExtendedRecord er = erDynamicPropertiesFn.apply("weight=81.00 g; sex=female; age=u ad.");
    BasicRecord br = brFn.get();

    // When
    DynamicPropertiesInterpreter.interpretSex(er, br);

    // Should
    Assert.assertEquals("FEMALE", br.getSex());
  }

  @Test
  public void sexRandomValueTest() {
    // State
    ExtendedRecord er = erDynamicPropertiesFn.apply("sex=unknown ; crown-rump length=8 mm");
    BasicRecord br = brFn.get();

    // When
    DynamicPropertiesInterpreter.interpretSex(er, br);

    // Should
    Assert.assertNull(br.getSex());
  }

  @Test
  public void sexEmptyValueTest() {
    // State
    ExtendedRecord er = erDynamicPropertiesFn.apply("");
    BasicRecord br = brFn.get();

    // When
    DynamicPropertiesInterpreter.interpretSex(er, br);

    // Should
    Assert.assertNull(br.getSex());
  }

  @Test
  public void sexTermNotEmptyTest() {
    // State
    ExtendedRecord er = erDynamicPropertiesFn.apply("");
    BasicRecord br = brFn.get();
    br.setSex("something");

    // When
    DynamicPropertiesInterpreter.interpretSex(er, br);

    // Should
    Assert.assertEquals("something", br.getSex());
  }

  @Test
  public void lifeStageEmptyValueTest() {
    // State
    ExtendedRecord er = erDynamicPropertiesFn.apply("");
    BasicRecord br = brFn.get();

    // When
    DynamicPropertiesInterpreter.interpretLifeStage(vocabularyService).accept(er, br);

    // Should
    Assert.assertNull(br.getLifeStage());
  }

  @Test
  public void lifeStageRandomValueTest() {
    // State
    ExtendedRecord er = erDynamicPropertiesFn.apply("lifeStage=unknown ; crown-rump length=8 mm");
    BasicRecord br = brFn.get();

    // When
    DynamicPropertiesInterpreter.interpretLifeStage(vocabularyService).accept(er, br);

    // Should
    Assert.assertNull(br.getLifeStage());
  }

  @Test
  public void lifeStageAdultValueTest() {
    // State
    ExtendedRecord er =
        erDynamicPropertiesFn.apply(
            "sex=female;age class=adult;total length=495 mm;tail length=210 mm;");
    BasicRecord br = brFn.get();

    // When
    DynamicPropertiesInterpreter.interpretLifeStage(vocabularyService).accept(er, br);

    // Should
    Assert.assertEquals("Adult", br.getLifeStage().getConcept());
    Assert.assertEquals("Adult", br.getLifeStage().getLineage().get(0));
  }

  @Test
  public void lifeStageNotNullTest() {
    // State
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).build();
    er.getCoreTerms().put(DwcTerm.lifeStage.qualifiedName(), "something");
    BasicRecord br = brFn.get();

    // When
    DynamicPropertiesInterpreter.interpretLifeStage(vocabularyService).accept(er, br);

    // Should
    Assert.assertNull(br.getLifeStage());
  }
}
