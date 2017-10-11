package org.gbif.pipelines.core.beam;

import org.gbif.pipelines.io.avro.TypedOccurrence;
import org.gbif.pipelines.io.avro.UntypedOccurrence;

import java.beans.IntrospectionException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.function.Function;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Utilities for writing beam functions.
 */
public class BeamFunctions {
  /**
   * A wrapper to expose the given function suitable for a beam pipeline.
   * <em>It is the reponsibility of the user to ensure that appropriate serializers are registered in Beam</em>.
   *
   * @param source To wrap.
   * @param <IN> The type of input
   * @param <OUT> The type of output
   * @return A function suitable for calling in Beam.
   */
  public static <IN,OUT> DoFn<IN, OUT> beamify(Function<IN, OUT> source) {
    return new DoFn<IN, OUT>() {

      @ProcessElement
      public void processElement(ProcessContext c)
        throws InvocationTargetException, IllegalAccessException, IntrospectionException {
        c.output(source.apply(c.element()));
      }
    };
  }

  /**
   * A function for serialising an Avro object as a JSON String within a Beam pipeline.
   * @param source Object to serialize.
   * @param <T> The type of the source object, which must be an Avro generated object.
   * @return A function that will serialise the soruce as a JSON string.
   */
  public static <T extends SpecificRecord> DoFn<T, String> asJson(Class<T> source) {
    return new DoFn<T, String>() {
      @ProcessElement
      public void processElement(ProcessContext c) throws IOException {
        T source = c.element();
        Schema schema = source.getSchema();
        DatumWriter<Object> writer = new GenericDatumWriter<Object>(schema);

        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
          final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(TypedOccurrence.getClassSchema(), os);
          writer.write(source, encoder);
          encoder.flush();
          c.output(new String(os.toByteArray(),"UTF-8"));
        }
      }
    };
  }

}
