package org.gbif.pipelines.common.beam.coders;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.gbif.pipelines.core.pojo.Edge;
import org.gbif.pipelines.io.avro.Record;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AvroKvCoder {
  public static <T extends SpecificRecordBase> KvCoder<String, T> of(Class<T> ofClass) {
    return KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(ofClass));
  }

  public static <T extends SpecificRecordBase & Record> KvCoder<String, Edge<T>> ofEdge(
      Class<T> ofClass) {
    return KvCoder.of(StringUtf8Coder.of(), EdgeCoder.of(AvroCoder.of(ofClass)));
  }
}
