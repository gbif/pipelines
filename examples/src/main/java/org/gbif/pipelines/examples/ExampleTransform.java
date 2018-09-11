package org.gbif.pipelines.examples;

import org.gbif.example.io.avro.ExampleRecord;
import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

/** */
public class ExampleTransform {

  /** */
  public static ParDo.SingleOutput<ExtendedRecord, ExampleRecord> exampleOne() {
    //
    return ParDo.of(
        //
        new DoFn<ExtendedRecord, ExampleRecord>() {
          @ProcessElement
          public void processElement(ProcessContext context) {
            ExtendedRecord extendedRecord = context.element();
            // Create an interpretation function
            Interpretation.from(extendedRecord)
                // Create an instance using an anonymous class or just pass an instance
                // .to(new ExampleRecord())
                // Or use Java 8 java.util.function.Supplier to create an instance
                // .to(()-> ExampleRecord())
                // Or use Java 8 method reference to create an instance
                // .to(ExtendedRecord::new)
                // Or use Java 8 java.util.function.Function to create and set values, such as id or etc.
                .to(er -> ExampleRecord.newBuilder().setId(er.getId()).build())
                //
                .via(ExampleInterpreter::interpretOne)
                //
                .via(ExampleInterpreter.interpretTwo())
                //
                .via(ExampleInterpreter.interpretThree())
                //
                .via(ExampleInterpreter.interpretFour())
                //
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
