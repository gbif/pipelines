package org.gbif.pipelines.common.beam;

import org.apache.avro.specific.SpecificRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
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
    for (Class<?> c : messageClasses) {
      LOG.debug("Registering default AvroCoder for {}", c);
      p.getCoderRegistry().registerCoderForClass(c, AvroCoder.of(c));
    }
  }

  /**
   * Registers a default AvroCoder for each of the types in the provided pipelines coder registry.
   * Note: use this for plain avro coder types not for KV types
   *
   * @param p    The pipeline into which the coders are to be registered
   * @param tags To register coders for tuple tags descriptors
   */
  public static void registerAvroCodersForTypes(Pipeline p, TupleTag<? extends SpecificRecord>... tags) {

    for (TupleTag<?> c : tags) {
      LOG.debug("Registering default AvroCoder for type {}", c);
      p.getCoderRegistry().registerCoderForType(c.getTypeDescriptor(), AvroCoder.of(c.getTypeDescriptor()));
    }
  }

  /**
   * Registers a default coder for a tuple tag of type KV, where Key is string and value is an avro type.
   *
   * @param p           The pipeline into which the coders are to be registered
   * @param tags        To register coders for tuple tags descriptors
   * @param avroClasses array of avro classes in the order of tuple tags
   */
  public static void registerAvroCodersForKVTypes(
    Pipeline p,
    TupleTag<KV<String, ? extends SpecificRecord>>[] tags,
    Class<? extends SpecificRecord>... avroClasses
  ) {
    for (int i = 0; i < tags.length; i++) {
      LOG.debug("Registering default KV Coder for type {}", tags[i].getTypeDescriptor().getType());
      p.getCoderRegistry()
        .registerCoderForType(tags[i].getTypeDescriptor(),
                              KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(avroClasses[i])));
    }
  }

}
