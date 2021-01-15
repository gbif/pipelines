package org.gbif.pipelines.core.interpreters.core;

import java.util.Collections;
import java.util.HashMap;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.PipelinesVariables.DynamicProperties.Key;
import org.gbif.pipelines.common.PipelinesVariables.DynamicProperties.Type;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.DynamicProperty;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Assert;
import org.junit.Test;

public class DynamicPropertiesInterpreterTest {

  private static final String ID = "777";

  @Test
  public void tissueEmptyTest() {
    // State
    ExtendedRecord er =
        ExtendedRecord.newBuilder().setId(ID).setCoreTerms(new HashMap<>(0)).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

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
    ExtendedRecord er =
        ExtendedRecord.newBuilder()
            .setId(ID)
            .setCoreTerms(Collections.singletonMap(DwcTerm.preparations.qualifiedName(), null))
            .build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

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
    ExtendedRecord er =
        ExtendedRecord.newBuilder()
            .setId(ID)
            .setCoreTerms(
                Collections.singletonMap(DwcTerm.preparations.qualifiedName(), "frozen carcass"))
            .build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    DynamicPropertiesInterpreter.interpretHasTissue(er, br);

    // Should
    DynamicProperty property = br.getDynamicProperties().get(Key.HAS_TISSUE);
    Assert.assertEquals(Type.BOOLEAN, property.getType());
    Assert.assertEquals("true", property.getValue());
  }
}
