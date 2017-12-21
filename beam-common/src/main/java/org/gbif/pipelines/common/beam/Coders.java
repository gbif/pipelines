package org.gbif.pipelines.common.beam;

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
   * Registers a default AvroCoder for each of the classes in the provided pipelines coder registry.
   * @param p The pipeline into which the coders are to be registered
   * @param messageClasses To register coders for
   */
  public static void registerAvroCoders(Pipeline p, Class<? extends SpecificRecord>... messageClasses) {
    for (Class<?> c : messageClasses) {
      LOG.debug("Registering default AvroCoder for {}", c);
      p.getCoderRegistry().registerCoderForClass(c, AvroCoder.of(c));
    }
  }
}
