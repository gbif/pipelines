package org.apache.beam.runners.spark.coders;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.gbif.pipelines.io.avro.AmplificationRecord;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.AustraliaSpatialRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.IssueRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFact;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.OccurrenceHdfsRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

import org.apache.beam.runners.spark.io.MicrobatchSource;
import org.apache.beam.runners.spark.stateful.SparkGroupAlsoByWindowViaWindowSet.StateAndTimers;
import org.apache.beam.runners.spark.translation.GroupNonMergingWindowsFunctions.WindowedKey;
import org.apache.beam.runners.spark.translation.ValueAndCoderKryoSerializer;
import org.apache.beam.runners.spark.translation.ValueAndCoderLazySerializable;
import org.apache.beam.runners.spark.util.ByteArray;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.HashBasedTable;
import org.apache.spark.serializer.GenericAvroSerializer;
import org.apache.spark.serializer.KryoRegistrator;

import com.esotericsoftware.kryo.Kryo;
import scala.Tuple2;
import scala.collection.mutable.WrappedArray.ofRef;

/**
 * Overrides the Beam SparkRunnerKryoRegistrator to register generic Avro classes with the {@link GenericAvroSerializer}
 * when using the {@link org.apache.spark.serializer.KryoSerializer}.
 */
public class SparkRunnerKryoRegistrator implements KryoRegistrator {

  private static final scala.collection.immutable.Map<Object, String> AVRO_SCHEMAS =
      scala.collection.immutable.Map$.MODULE$.empty();

  static {
    AVRO_SCHEMAS.$plus(Tuple2.apply(LocationRecord.SCHEMA$.getName(), LocationRecord.SCHEMA$.toString()));
    AVRO_SCHEMAS.$plus(Tuple2.apply(TemporalRecord.SCHEMA$.getName(), TemporalRecord.SCHEMA$.toString()));
    AVRO_SCHEMAS.$plus(Tuple2.apply(TaxonRecord.SCHEMA$.getName(), TaxonRecord.SCHEMA$.toString()));
    AVRO_SCHEMAS.$plus(Tuple2.apply(BasicRecord.SCHEMA$.getName(), BasicRecord.SCHEMA$.toString()));
    AVRO_SCHEMAS.$plus(Tuple2.apply(MetadataRecord.SCHEMA$.getName(), MetadataRecord.SCHEMA$.toString()));
    AVRO_SCHEMAS.$plus(Tuple2.apply(AmplificationRecord.SCHEMA$.getName(), AmplificationRecord.SCHEMA$.toString()));
    AVRO_SCHEMAS.$plus(Tuple2.apply(AudubonRecord.SCHEMA$.getName(), AudubonRecord.SCHEMA$.toString()));
    AVRO_SCHEMAS.$plus(Tuple2.apply(ImageRecord.SCHEMA$.getName(), ImageRecord.SCHEMA$.toString()));
    AVRO_SCHEMAS.$plus(Tuple2.apply(MeasurementOrFact.SCHEMA$.getName(), MeasurementOrFact.SCHEMA$.toString()));
    AVRO_SCHEMAS.$plus(Tuple2.apply(MultimediaRecord.SCHEMA$.getName(), MultimediaRecord.SCHEMA$.toString()));
    AVRO_SCHEMAS.$plus(Tuple2.apply(ExtendedRecord.SCHEMA$.getName(), ExtendedRecord.SCHEMA$.toString()));
    AVRO_SCHEMAS.$plus(Tuple2.apply(IssueRecord.SCHEMA$.getName(), IssueRecord.SCHEMA$.toString()));
    AVRO_SCHEMAS.$plus(Tuple2.apply(OccurrenceHdfsRecord.SCHEMA$.getName(), OccurrenceHdfsRecord.SCHEMA$.toString()));
    AVRO_SCHEMAS.$plus(
        Tuple2.apply(AustraliaSpatialRecord.SCHEMA$.getName(), AustraliaSpatialRecord.SCHEMA$.toString()));
  }

  @Override
  public void registerClasses(Kryo kryo) {
    // copied from Beam
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
    kryo.register(WindowedKey.class);
    kryo.register(ofRef.class);

    try {
      // copied from Beam
      kryo.register(Class.forName("org.apache.beam.sdk.util.WindowedValue$TimestampedValueInGlobalWindow"));
      kryo.register(
          Class.forName("org.apache.beam.vendor.guava.v20_0.com.google.common.collect.HashBasedTable$Factory"));

      // custom types added
      kryo.register(Class.forName("org.apache.avro.generic.GenericData"), new GenericAvroSerializer(AVRO_SCHEMAS));
      kryo.register(Class.forName("org.apache.avro.generic.GenericData$Array"),
          new GenericAvroSerializer(AVRO_SCHEMAS));
      kryo.register(Class.forName("org.apache.avro.generic.GenericData$Record"),
          new GenericAvroSerializer(AVRO_SCHEMAS));
    } catch (ClassNotFoundException ex) {
      throw new IllegalStateException("Unable to register classes with kryo.", ex);
    }
  }
}
