package org.gbif.pipelines.ingest.pipelines.fragmenter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.gbif.pipelines.fragmenter.record.DwcaOccurrenceRecord;

public class DwcaOccurrenceRecordCoder extends AtomicCoder<DwcaOccurrenceRecord> {
  private final NullableCoder<String> tripletCoder;
  private final NullableCoder<String> occurrenceIdCoder;
  private final NullableCoder<String> stringRecordCoder;

  private DwcaOccurrenceRecordCoder() {
    this.tripletCoder = NullableCoder.of(StringUtf8Coder.of());
    this.occurrenceIdCoder = NullableCoder.of(StringUtf8Coder.of());
    this.stringRecordCoder = NullableCoder.of(StringUtf8Coder.of());
  }

  public static DwcaOccurrenceRecordCoder of() {
    return new DwcaOccurrenceRecordCoder();
  }

  @Override
  public void encode(DwcaOccurrenceRecord value, OutputStream outStream) throws IOException {
    tripletCoder.encode(value.getTriplet().orElse(null), outStream);
    occurrenceIdCoder.encode(value.getOccurrenceId().orElse(null), outStream);
    stringRecordCoder.encode(value.getStringRecord(), outStream);
  }

  @Override
  public DwcaOccurrenceRecord decode(InputStream inStream) throws IOException {
    String triplet = tripletCoder.decode(inStream);
    String occurrenceId = occurrenceIdCoder.decode(inStream);
    String stringRecord = stringRecordCoder.decode(inStream);

    return new DwcaOccurrenceRecord(triplet, occurrenceId, stringRecord);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    tripletCoder.verifyDeterministic();
    occurrenceIdCoder.verifyDeterministic();
    stringRecordCoder.verifyDeterministic();
  }
}
