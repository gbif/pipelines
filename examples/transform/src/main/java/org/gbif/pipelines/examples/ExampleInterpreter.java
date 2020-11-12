package org.gbif.pipelines.examples;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.gbif.example.io.avro.ExampleRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

/**
 * Set of interpretation steps, uses {@link org.gbif.pipelines.io.avro.TemporalRecord} as a source
 * data and {@link org.gbif.example.io.avro.ExampleRecord} generated from
 * resources/example-record.avsc avro schema as a target data. Regular step looks like: 1) Get data
 * from source object 2) Do some interpretation logic 3) Set data to the target object
 */
class ExampleInterpreter {

  private ExampleInterpreter() {}

  /**
   * Regular static function, can be used as a Java 8 {@link java.util.function.BiConsumer}:
   *
   * <pre>{@code
   * (TemporalRecord tr, ExampleRecord exr) -> interpretOne(TemporalRecord tr, ExampleRecord exr)
   *
   * or
   *
   * (tr, exr) -> interpretOne(TemporalRecord tr, ExampleRecord exr)
   *
   * or as a Java 8 method reference:
   *
   * ExampleInterpreter::interpretOne
   * }</pre>
   */
  static void interpretOne(TemporalRecord tr, ExampleRecord exr) {
    // Gets data from source
    Integer value = tr.getMonth();
    // Does some interpretation logic
    // ...
    // Sets data to the target object
    exr.setOne(value);
  }

  /** @return Regular Java 8 {@link java.util.function.BiConsumer} */
  static BiConsumer<TemporalRecord, ExampleRecord> interpretTwo() {
    return (TemporalRecord tr, ExampleRecord exr) -> {
      // Gets data from source
      Integer value = tr.getYear();
      // Does some interpretation logic
      // ...
      // Sets data to the target object
      exr.setTwo(value);
    };
  }

  /** @return Regular Java 8 {@link java.util.function.BiConsumer} */
  static BiConsumer<TemporalRecord, ExampleRecord> interpretThree() {
    return (tr, exr) -> {
      // Gets data from source
      Integer value = tr.getDay();
      // Does some interpretation logic
      // ...
      // Sets data to the target object
      exr.setThree(value);
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
      Integer one = exr.getOne();
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
      Integer one = exr.getOne();
      // Does some interpretation logic
      // ...
      // Sets data to the target object
      exr.setThree(one);
    };
  }
}
