package org.gbif.pipelines.core.interpreters.core;

import static org.gbif.pipelines.common.PipelinesVariables.DynamicProperties.Key;
import static org.gbif.pipelines.common.PipelinesVariables.DynamicProperties.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.DynamicProperty;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.vocabulary.lookup.LookupConcept;
import org.gbif.vocabulary.model.Concept;
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

  private final Function<String, Optional<LookupConcept>> vocabularyLookupFn =
      v -> {
        if (v.equalsIgnoreCase("adult")) {
          Concept concept = new Concept();
          concept.setName("Adult");
          LookupConcept lookupConcept = LookupConcept.of(concept, new ArrayList<>(1));

          return Optional.of(lookupConcept);
        }
        return Optional.empty();
      };

  @Test
  public void tissueEmptyTest() {
    // State
    ExtendedRecord er =
        ExtendedRecord.newBuilder().setId(ID).setCoreTerms(Collections.emptyMap()).build();
    BasicRecord br = brFn.get();

    // When
    DynamicPropertiesInterpreter.interpretHasTissue(er, br);

    // Should
    DynamicProperty property = br.getDynamicProperties().get(Key.HAS_TISSUE);
    Assert.assertNull(property);
  }

  @Test
  public void tissueNullTest() {
    // State
    ExtendedRecord er = erFn.apply(DwcTerm.preparations, null);
    BasicRecord br = brFn.get();

    // When
    DynamicPropertiesInterpreter.interpretHasTissue(er, br);

    // Should
    DynamicProperty property = br.getDynamicProperties().get(Key.HAS_TISSUE);
    Assert.assertNull(property);
  }

  @Test
  public void tissueTest() {
    // State
    ExtendedRecord er = erFn.apply(DwcTerm.preparations, "frozen carcass");
    BasicRecord br = brFn.get();

    // When
    DynamicPropertiesInterpreter.interpretHasTissue(er, br);

    // Should
    DynamicProperty property = br.getDynamicProperties().get(Key.HAS_TISSUE);
    Assert.assertEquals(Type.BOOLEAN, property.getClazz());
    Assert.assertEquals("true", property.getValue());
  }

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
    DynamicPropertiesInterpreter.interpretLifeStage(vocabularyLookupFn).accept(er, br);

    // Should
    Assert.assertNull(br.getLifeStage());
    Assert.assertTrue(br.getLifeStageLineage().isEmpty());
  }

  @Test
  public void lifeStageRandomValueTest() {
    // State
    ExtendedRecord er = erDynamicPropertiesFn.apply("lifeStage=unknown ; crown-rump length=8 mm");
    BasicRecord br = brFn.get();

    // When
    DynamicPropertiesInterpreter.interpretLifeStage(vocabularyLookupFn).accept(er, br);

    // Should
    Assert.assertNull(br.getLifeStage());
    Assert.assertTrue(br.getLifeStageLineage().isEmpty());
  }

  @Test
  public void lifeStageAdultValueTest() {
    // State
    ExtendedRecord er =
        erDynamicPropertiesFn.apply(
            "sex=female;age class=adult;total length=495 mm;tail length=210 mm;");
    BasicRecord br = brFn.get();

    // When
    DynamicPropertiesInterpreter.interpretLifeStage(vocabularyLookupFn).accept(er, br);

    // Should
    Assert.assertEquals("Adult", br.getLifeStage());
    Assert.assertEquals("Adult", br.getLifeStageLineage().get(0));
  }

  @Test
  public void lifeStageNullFnTest() {
    // State
    ExtendedRecord er = erDynamicPropertiesFn.apply("");
    BasicRecord br = brFn.get();

    // When
    DynamicPropertiesInterpreter.interpretLifeStage(null).accept(er, br);

    // Should
    Assert.assertNull(br.getLifeStage());
    Assert.assertTrue(br.getLifeStageLineage().isEmpty());
  }

  @Test
  public void lifeStageNotNullTest() {
    // State
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).build();
    er.getCoreTerms().put(DwcTerm.lifeStage.qualifiedName(), "something");
    BasicRecord br = brFn.get();

    // When
    DynamicPropertiesInterpreter.interpretLifeStage(vocabularyLookupFn).accept(er, br);

    // Should
    Assert.assertTrue(br.getLifeStageLineage().isEmpty());
  }

  @Test
  public void massValueTest() {
    // State
    ExtendedRecord er =
        erDynamicPropertiesFn.apply(
            "sex=female;age class=adult;total length=495 mm;tail length=210 mm;weight=100g");
    BasicRecord br = brFn.get();

    // When
    DynamicPropertiesInterpreter.interpretMass(er, br);

    // Should
    Assert.assertFalse(br.getDynamicProperties().isEmpty());
    Assert.assertEquals("String", br.getDynamicProperties().get(Key.MASS).getClazz());
    Assert.assertEquals("total weight", br.getDynamicProperties().get(Key.MASS).getKey());
    Assert.assertEquals("g", br.getDynamicProperties().get(Key.MASS).getType());
    Assert.assertEquals("100", br.getDynamicProperties().get(Key.MASS).getValue());
  }

  @Test
  public void lengthValueTest() {
    // State
    ExtendedRecord er =
        erDynamicPropertiesFn.apply(
            "sex=female;age class=adult;total length=495 mm;tail length=210 mm;");
    BasicRecord br = brFn.get();

    // When
    DynamicPropertiesInterpreter.interpretLength(er, br);

    // Should
    Assert.assertFalse(br.getDynamicProperties().isEmpty());
    Assert.assertEquals("String", br.getDynamicProperties().get(Key.LENGTH).getClazz());
    Assert.assertEquals("total length", br.getDynamicProperties().get(Key.LENGTH).getKey());
    Assert.assertEquals("mm", br.getDynamicProperties().get(Key.LENGTH).getType());
    Assert.assertEquals("495", br.getDynamicProperties().get(Key.LENGTH).getValue());
  }
}
