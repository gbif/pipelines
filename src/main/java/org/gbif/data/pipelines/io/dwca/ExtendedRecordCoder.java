package org.gbif.data.pipelines.io.dwca;

import org.gbif.data.io.avro.ExtendedRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;

/**
 * SerDe for ExtendedRecords using Avro.
 * @deprecated use AvroCoder.of(ExtendedRecord.class)
 */
@Deprecated
public class ExtendedRecordCoder extends Coder<ExtendedRecord> {
  @Override
  public void encode(ExtendedRecord record, OutputStream outStream) throws CoderException, IOException {
    Encoder encoder = EncoderFactory.get().directBinaryEncoder(outStream, null);
    DatumWriter<ExtendedRecord> datumWriter = new SpecificDatumWriter<>(ExtendedRecord.class);
    datumWriter.write(record, encoder);
  }

  @Override
  public ExtendedRecord decode(InputStream inStream) throws CoderException, IOException {
    DatumReader<ExtendedRecord> reader = new SpecificDatumReader<>(ExtendedRecord.class);
    Decoder decoder = DecoderFactory.get().binaryDecoder(inStream, null);
    return reader.read(null, decoder);
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Collections.EMPTY_LIST;
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
  }
}
