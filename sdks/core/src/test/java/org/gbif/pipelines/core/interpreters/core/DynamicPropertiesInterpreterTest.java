package org.gbif.pipelines.core.interpreters.core;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.PipelinesVariables.DynamicProperties.Key;
import org.gbif.pipelines.common.PipelinesVariables.DynamicProperties.Type;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.DynamicProperty;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

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
    Assert.assertEquals(Type.BOOLEAN, property.getType());
    Assert.assertEquals("false", property.getValue());
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
    Assert.assertEquals(Type.BOOLEAN, property.getType());
    Assert.assertEquals("false", property.getValue());
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
    Assert.assertEquals(Type.BOOLEAN, property.getType());
    Assert.assertEquals("true", property.getValue());
  }

  @Test
  public void sexKeyValueDelimited1Test() {
    // State
    ExtendedRecord er = erDynamicPropertiesFn.apply("weight=81.00 g; sex=female ? ; age=u ad.");
    BasicRecord br = brFn.get();

    // When
    DynamicPropertiesInterpreter.interpretSex(er, br);

    // Should
    Assert.assertEquals("female ?", er.getCoreTerms().get(DwcTerm.sex.qualifiedName()));
  }

  @Test
  public void sexKeyValueDelimited2Test() {
    // State
    ExtendedRecord er = erDynamicPropertiesFn.apply("sex=unknown ; crown-rump length=8 mm");
    BasicRecord br = brFn.get();

    // When
    DynamicPropertiesInterpreter.interpretSex(er, br);

    // Should
    Assert.assertEquals("unknown", er.getCoreTerms().get(DwcTerm.sex.qualifiedName()));
  }

  @Test
  public void sexKeyValueUndelimited1Test() {
    // State
    ExtendedRecord er = erDynamicPropertiesFn.apply("sex=F crown rump length=8 mm");
    BasicRecord br = brFn.get();

    // When
    DynamicPropertiesInterpreter.interpretSex(er, br);

    // Should
    Assert.assertEquals("F", er.getCoreTerms().get(DwcTerm.sex.qualifiedName()));
  }

  @Test
  public void sexUnkeyed1Test() {
    // State
    ExtendedRecord er = erDynamicPropertiesFn.apply("words male female unknown more words");
    BasicRecord br = brFn.get();

    // When
    DynamicPropertiesInterpreter.interpretSex(er, br);

    // Should
    Assert.assertEquals("male/female", er.getCoreTerms().get(DwcTerm.sex.qualifiedName()));
  }

  @Test
  public void sexUnkeyed2Test() {
    // State
    ExtendedRecord er = erDynamicPropertiesFn.apply("words male female male more words");
    BasicRecord br = brFn.get();

    // When
    DynamicPropertiesInterpreter.interpretSex(er, br);

    // Should
    Assert.assertNull(er.getCoreTerms().get(DwcTerm.sex.qualifiedName()));
  }

  @Test
  public void sexUnkeyed3Test() {
    // State
    ExtendedRecord er = erDynamicPropertiesFn.apply("");
    BasicRecord br = brFn.get();

    // When
    DynamicPropertiesInterpreter.interpretSex(er, br);

    // Should
    Assert.assertNull(er.getCoreTerms().get(DwcTerm.sex.qualifiedName()));
  }

  @Test
  public void excluded1Test() {
    // State
    ExtendedRecord er = erDynamicPropertiesFn.apply("Respective sex and msmt. in mm");
    BasicRecord br = brFn.get();

    // When
    DynamicPropertiesInterpreter.interpretSex(er, br);

    // Should
    Assert.assertNull(er.getCoreTerms().get(DwcTerm.sex.qualifiedName()));
  }

  @Test
  public void preferredOrSearch1Test() {
    // State
    ExtendedRecord er = erDynamicPropertiesFn.apply("mention male in a phrase");
    BasicRecord br = brFn.get();

    // When
    DynamicPropertiesInterpreter.interpretSex(er, br);

    // Should
    Assert.assertEquals("male", er.getCoreTerms().get(DwcTerm.sex.qualifiedName()));
  }

  @Test
  public void preferredOrSearch2Test() {
    // State
    ExtendedRecord er = erDynamicPropertiesFn.apply("male in a phrase");
    BasicRecord br = brFn.get();

    // When
    DynamicPropertiesInterpreter.interpretSex(er, br);

    // Should
    Assert.assertEquals("male", er.getCoreTerms().get(DwcTerm.sex.qualifiedName()));
  }

  @Test
  public void preferredOrSearch3Test() {
    // State
    ExtendedRecord er = erDynamicPropertiesFn.apply("male or female");
    BasicRecord br = brFn.get();

    // When
    DynamicPropertiesInterpreter.interpretSex(er, br);

    // Should
    Assert.assertEquals("male,female", er.getCoreTerms().get(DwcTerm.sex.qualifiedName()));
  }
}
