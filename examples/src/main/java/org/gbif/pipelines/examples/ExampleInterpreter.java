package org.gbif.pipelines.examples;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.example.io.avro.ExampleRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.parsers.utils.ModelUtils;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Set of interpretation steps, uses {@link org.gbif.pipelines.io.avro.ExtendedRecord} as a source
 * data and {@link org.gbif.example.io.avro.ExampleRecord} generated from
 * resources/example-record.avsc avro scheam as a target data. Regular step looks like: 1) Get data
 * from source object 2) Do some interpretation logic 3) Set data to the target object
 */
class ExampleInterpreter {

  private ExampleInterpreter() {}

  /**
   * Regular static function, can be used as a Java 8 {@link java.util.function.BiConsumer}:
   *
   * <pre>{@code
   * (ExtendedRecord er, ExampleRecord exr) -> interpretOne(ExtendedRecord er, ExampleRecord exr)
   *
   * or
   *
   * (er, exr) -> interpretOne(ExtendedRecord er, ExampleRecord exr)
   *
   * or as a Java 8 method reference:
   *
   * ExampleInterpreter::interpretOne
   * }</pre>
   */
  static void interpretOne(ExtendedRecord er, ExampleRecord exr) {
    // Gets data from source
    String extractValue = ModelUtils.extractValue(er, DwcTerm.basisOfRecord);
    // Does some interpretation logic
    // ...
    // Sets data to the target object
    exr.setOne(extractValue);
  }

  /** @return Regular Java 8 {@link java.util.function.BiConsumer} */
  static BiConsumer<ExtendedRecord, ExampleRecord> interpretTwo() {
    return (ExtendedRecord er, ExampleRecord exr) -> {
      // Gets data from source
      String extractValue = ModelUtils.extractValue(er, DwcTerm.continent);
      // Does some interpretation logic
      // ...
      // Sets data to the target object
      exr.setTwo(extractValue);
    };
  }

  /** @return Regular Java 8 {@link java.util.function.BiConsumer} */
  static BiConsumer<ExtendedRecord, ExampleRecord> interpretThree() {
    return (er, exr) -> {
      // Gets data from source
      String extractValue = ModelUtils.extractValue(er, DwcTerm.county);
      // Does some interpretation logic
      // ...
      // Sets data to the target object
      exr.setThree(extractValue);
    };
  }

  /**
   * Case when you want to use target object as a source object
   *
   * @return Regular Java 8 {@link java.util.function.Consumer}
   */
  static Consumer<ExampleRecord> interpretFour() {
    return (ExampleRecord exr) -> {
      // Gets data from source
      String one = exr.getOne();
      // Does some interpretation logic
      // ...
      // Sets data to the target object
      exr.setThree(one);
    };
  }

  /**
   * Case when you want to use target object as a source object
   *
   * @return Regular Java 8 {@link java.util.function.Consumer}
   */
  static Consumer<ExampleRecord> interpretFive() {
    return exr -> {
      // Gets data from source
      String one = exr.getOne();
      // Does some interpretation logic
      // ...
      // Sets data to the target object
      exr.setThree(one);
    };
  }
}
