package org.gbif.pipelines.ingest.pipelines.fragmenter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.gbif.pipelines.fragmenter.common.RawRecord;

public class RawRecordCoder extends AtomicCoder<RawRecord> {
  private final NullableCoder<String> keyCoder;
  private final NullableCoder<String> recordBodyCoder;
  private final NullableCoder<String> hashValueCoder;
  private final NullableCoder<Long> createdDateCoder;

  private RawRecordCoder() {
    this.keyCoder = NullableCoder.of(StringUtf8Coder.of());
    this.recordBodyCoder = NullableCoder.of(StringUtf8Coder.of());
    this.hashValueCoder = NullableCoder.of(StringUtf8Coder.of());
    this.createdDateCoder = NullableCoder.of(VarLongCoder.of());
  }

  public static RawRecordCoder of() {
    return new RawRecordCoder();
  }

  @Override
  public void encode(RawRecord value, OutputStream outStream) throws IOException {
    keyCoder.encode(value.getKey(), outStream);
    recordBodyCoder.encode(value.getRecordBody(), outStream);
    hashValueCoder.encode(value.getHashValue(), outStream);
    createdDateCoder.encode(value.getCreatedDate(), outStream);
  }

  @Override
  public RawRecord decode(InputStream inStream) throws IOException {
    String key = keyCoder.decode(inStream);
    String recordBody = recordBodyCoder.decode(inStream);
    String hashValue = hashValueCoder.decode(inStream);
    Long createdDate = createdDateCoder.decode(inStream);

    return new RawRecord(key, recordBody, hashValue, createdDate);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    keyCoder.verifyDeterministic();
    recordBodyCoder.verifyDeterministic();
    hashValueCoder.verifyDeterministic();
    createdDateCoder.verifyDeterministic();
  }
}
