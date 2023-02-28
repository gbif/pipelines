package org.gbif.pipelines.common.beam.coders;

import static org.junit.Assert.*;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.gbif.pipelines.core.pojo.Edge;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Test;

public class AvroKvCoderTest {

  @Test
  public void coderTest() {

    // When
    KvCoder<String, ExtendedRecord> coder = AvroKvCoder.of(ExtendedRecord.class);
    coder.getKeyCoder().getEncodedTypeDescriptor().getType();

    // Should
    assertTrue(coder.getKeyCoder() instanceof StringUtf8Coder);
    assertTrue(coder.getValueCoder() instanceof AvroCoder);
  }

  @Test
  public void edgeCoderTest() {

    // When
    KvCoder<String, Edge<ExtendedRecord>> coder = AvroKvCoder.ofEdge(ExtendedRecord.class);
    coder.getKeyCoder().getEncodedTypeDescriptor().getType();

    // Should
    assertTrue(coder.getKeyCoder() instanceof StringUtf8Coder);
    assertTrue(coder.getValueCoder() instanceof EdgeCoder);
  }
}
