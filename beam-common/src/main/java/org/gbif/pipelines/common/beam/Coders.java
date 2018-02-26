package org.gbif.pipelines.common.beam;

import java.util.Arrays;

import org.apache.avro.specific.SpecificRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for dealing with Beam coders.
 */
public class Coders {

  private static final Logger LOG = LoggerFactory.getLogger(Coders.class);

  /**
   * Can't instantiate utility classes.
   */
  private Coders() {
    //do nothing
  }

  /**
   * Registers a default AvroCoder for each of the classes in the provided pipelines coder registry.
   *
   * @param p              The pipeline into which the coders are to be registered
   * @param messageClasses To register coders for
   */
  @SafeVarargs
  public static void registerAvroCoders(Pipeline p, Class<? extends SpecificRecord>... messageClasses) {
    Arrays.stream(messageClasses).forEach(clazz -> {
      LOG.debug("Registering default AvroCoder for {}", clazz);
      p.getCoderRegistry().registerCoderForClass(clazz, AvroCoder.of(clazz));
    });
  }

}
