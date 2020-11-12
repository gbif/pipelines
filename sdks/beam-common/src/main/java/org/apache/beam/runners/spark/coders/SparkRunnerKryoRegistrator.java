package org.apache.beam.runners.spark.coders;

import com.esotericsoftware.kryo.Kryo;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.beam.runners.spark.io.MicrobatchSource;
import org.apache.beam.runners.spark.stateful.SparkGroupAlsoByWindowViaWindowSet.StateAndTimers;
import org.apache.beam.runners.spark.translation.ValueAndCoderKryoSerializer;
import org.apache.beam.runners.spark.translation.ValueAndCoderLazySerializable;
import org.apache.beam.runners.spark.util.ByteArray;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.HashBasedTable;
import org.apache.spark.serializer.GenericAvroSerializer;
import org.apache.spark.serializer.KryoRegistrator;
import org.gbif.pipelines.io.avro.AmplificationRecord;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.IssueRecord;
import org.gbif.pipelines.io.avro.LocationFeatureRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFact;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.OccurrenceHdfsRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import scala.Tuple2;
import scala.collection.mutable.WrappedArray;

/**
 * Overrides the Beam SparkRunnerKryoRegistrator to register generic Avro classes with the {@link
 * GenericAvroSerializer} when using the {@link org.apache.spark.serializer.KryoSerializer}.
 */
public class SparkRunnerKryoRegistrator implements KryoRegistrator {

  // same function as in {@link org.apache.spark.SparkConf#registerAvroSchemas}
  private static final Function<Schema, Tuple2<Object, String>> TUPLE_SCHEMA =
      schema ->
          Tuple2.apply(
              "avro.schema." + SchemaNormalization.parsingFingerprint64(schema), schema.toString());

  private static final scala.collection.immutable.Map<Object, String> AVRO_SCHEMAS =
      scala.collection.immutable.Map$.MODULE$.empty();

  static {
    // core
    AVRO_SCHEMAS.$plus(TUPLE_SCHEMA.apply(BasicRecord.SCHEMA$));
    AVRO_SCHEMAS.$plus(TUPLE_SCHEMA.apply(LocationRecord.SCHEMA$));
    AVRO_SCHEMAS.$plus(TUPLE_SCHEMA.apply(MetadataRecord.SCHEMA$));
    AVRO_SCHEMAS.$plus(TUPLE_SCHEMA.apply(TaxonRecord.SCHEMA$));
    AVRO_SCHEMAS.$plus(TUPLE_SCHEMA.apply(TemporalRecord.SCHEMA$));
    AVRO_SCHEMAS.$plus(TUPLE_SCHEMA.apply(ExtendedRecord.SCHEMA$));
    AVRO_SCHEMAS.$plus(TUPLE_SCHEMA.apply(IssueRecord.SCHEMA$));
    AVRO_SCHEMAS.$plus(TUPLE_SCHEMA.apply(OccurrenceHdfsRecord.SCHEMA$));

    // extensions
    AVRO_SCHEMAS.$plus(TUPLE_SCHEMA.apply(AmplificationRecord.SCHEMA$));
    AVRO_SCHEMAS.$plus(TUPLE_SCHEMA.apply(AudubonRecord.SCHEMA$));
    AVRO_SCHEMAS.$plus(TUPLE_SCHEMA.apply(ImageRecord.SCHEMA$));
    AVRO_SCHEMAS.$plus(TUPLE_SCHEMA.apply(MeasurementOrFact.SCHEMA$));
    AVRO_SCHEMAS.$plus(TUPLE_SCHEMA.apply(MultimediaRecord.SCHEMA$));
    AVRO_SCHEMAS.$plus(TUPLE_SCHEMA.apply(LocationFeatureRecord.SCHEMA$));
  }

  /** Copied from BEAM, except last line */
  @Override
  public void registerClasses(Kryo kryo) {
    // MicrobatchSource is serialized as data and may not be Kryo-serializable.
    kryo.register(MicrobatchSource.class, new StatelessJavaSerializer());
    kryo.register(ValueAndCoderLazySerializable.class, new ValueAndCoderKryoSerializer());

    kryo.register(ArrayList.class);
    kryo.register(ByteArray.class);
    kryo.register(HashBasedTable.class);
    kryo.register(KV.class);
    kryo.register(LinkedHashMap.class);
    kryo.register(Object[].class);
    kryo.register(PaneInfo.class);
    kryo.register(StateAndTimers.class);
    kryo.register(TupleTag.class);
    kryo.register(WrappedArray.ofRef.class);

    try {
      kryo.register(
          Class.forName("org.apache.beam.sdk.util.WindowedValue$TimestampedValueInGlobalWindow"));
      kryo.register(
          Class.forName(
              "org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.HashBasedTable$Factory"));
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Unable to register classes with kryo.", e);
    }

    customRegister(kryo);
  }

  /** GBIF custom classes for registration */
  private void customRegister(Kryo kryo) {
    try {
      // custom types added
      kryo.register(
          Class.forName("org.apache.avro.generic.GenericData"),
          new GenericAvroSerializer(AVRO_SCHEMAS));
      kryo.register(
          Class.forName("org.apache.avro.generic.GenericData$Array"),
          new GenericAvroSerializer(AVRO_SCHEMAS));
      kryo.register(
          Class.forName("org.apache.avro.generic.GenericData$Record"),
          new GenericAvroSerializer(AVRO_SCHEMAS));
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Unable to register classes with kryo.", e);
    }
  }
}
