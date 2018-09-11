package org.gbif.pipelines.examples;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.example.io.avro.ExampleRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.parsers.utils.ModelUtils;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

class ExampleInterpreter {

  private ExampleInterpreter() {
  }

  static void interpretOne(ExtendedRecord er, ExampleRecord exr) {
    String extractValue = ModelUtils.extractValue(er, DwcTerm.basisOfRecord);
    // ... do some interpretation
    exr.setOne(extractValue);
  }

  static BiConsumer<ExtendedRecord, ExampleRecord> interpretTwo() {
    return (ExtendedRecord er, ExampleRecord exr) -> {
      String extractValue = ModelUtils.extractValue(er, DwcTerm.continent);
      // ... do some interpretation
      exr.setTwo(extractValue);
    };
  }

  static BiConsumer<ExtendedRecord, ExampleRecord> interpretThree() {
    return (er, exr) -> {
      String extractValue = ModelUtils.extractValue(er, DwcTerm.county);
      // ... do some interpretation
      exr.setThree(extractValue);
    };
  }

  static Consumer<ExampleRecord> interpretFour() {
    return (ExampleRecord exr) -> {
      String one = exr.getOne();
      // ... do some interpretation
      exr.setThree(one);
    };
  }

  static Consumer<ExampleRecord> interpretFive() {
    return (exr) -> {
      String one = exr.getOne();
      // ... do some interpretation
      exr.setThree(one);
    };
  }
}
