package org.gbif.pipelines.examples;

import org.gbif.example.io.avro.ExampleRecord;
import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.transforms.RecordTransforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

/**
 * Example how to use {@link org.gbif.pipelines.core.Interpretation}, as a sequence of
 * interpretations and wrap it into Apache Beam {@link org.apache.beam.sdk.transforms.ParDo} and use
 * it after in a {@link org.apache.beam.sdk.Pipeline}
 *
 * <p>See source code - {@link RecordTransforms}
 */
class ExampleTransform {

  private ExampleTransform() {}

  /**
   * Uses {@link org.gbif.pipelines.examples.ExampleInterpreter} as a set of interpretation steps
   *
   * @return ParDo.SingleOutput as a final transformation
   */
  static ParDo.SingleOutput<TemporalRecord, ExampleRecord> exampleOne() {
    return ParDo.of(
        new DoFn<TemporalRecord, ExampleRecord>() {
          @ProcessElement
          public void processElement(ProcessContext context) {
            TemporalRecord temporalRecord = context.element();
            // Create an interpretation function
            Interpretation.from(temporalRecord)
                // Create an instance using an anonymous class or just pass an instance
                // .to(new ExampleRecord())
                // Or use Java 8 java.util.function.Supplier to create an instance
                // .to(()-> ExampleRecord())
                // Or use Java 8 method reference to create an instance
                // .to(ExtendedRecord::new)
                // Or use Java 8 java.util.function.Function to create and set values, such as id or
                // etc.
                .to(er -> ExampleRecord.newBuilder().setId(er.getId()).build())
                // Use Java 8 java.util.function.BiConsumer function for an interpretation
                .via((ex, exr) -> ExampleInterpreter.interpretOne(ex, exr))
                // Or use Java 8 method reference for an interpretation
                .via(ExampleInterpreter::interpretOne)
                // Or use method that returns Java 8 java.util.function.BiConsumer function for an
                // interpretation
                .via(ExampleInterpreter.interpretTwo())
                .via(ExampleInterpreter.interpretThree())
                // Or use method that returns Java 8 java.util.function.Consumer function for an
                // interpretation
                .via(ExampleInterpreter.interpretFour())
                .via(ExampleInterpreter.interpretFive())
                // You can consume using Java 8 java.util.function.Function
                // .consume(example -> context.output(example));
                // Or return result value
                // .get();
                // Or consume result using Java 8 method reference
                .consume(context::output);
          }
        });
  }
}
