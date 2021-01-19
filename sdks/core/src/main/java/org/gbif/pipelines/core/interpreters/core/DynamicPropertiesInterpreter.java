package org.gbif.pipelines.core.interpreters.core;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.PipelinesVariables.DynamicProperties.Key;
import org.gbif.pipelines.common.PipelinesVariables.DynamicProperties.Type;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.DynamicProperty;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.gbif.pipelines.core.utils.ModelUtils.extractNullAwareValue;

public class DynamicPropertiesInterpreter {

  private static final Set<String> TISSUE_TOKENS =
      new HashSet<>(
          Arrays.asList(
              "+t",
              "tiss",
              "blood",
              "dmso",
              "dna",
              "extract",
              "froze",
              "forzen",
              "freez",
              "heart",
              "muscle",
              "higado",
              "kidney",
              "liver",
              "lung",
              "nitrogen",
              "pectoral",
              "rinon",
              "riñon",
              "rnalater",
              "sangre",
              "toe",
              "spleen",
              "fin",
              "fetge",
              "cor",
              "teixit"));

  public static void interpretHasTissue(ExtendedRecord er, BasicRecord br) {
    String value = extractNullAwareValue(er, DwcTerm.preparations);
    DynamicProperty.Builder builder = DynamicProperty.newBuilder().setType(Type.BOOLEAN);
    if (value == null || value.isEmpty()) {
      builder.setValue(Boolean.FALSE.toString());
    } else {
      boolean any = TISSUE_TOKENS.stream().anyMatch(value::contains);
      builder.setValue(Boolean.valueOf(any).toString());
    }
    br.getDynamicProperties().put(Key.HAS_TISSUE, builder.build());
  }

  public static void interpretSex(ExtendedRecord er, BasicRecord br) {
    if (br.getSex() == null) {

    }
  }
}
