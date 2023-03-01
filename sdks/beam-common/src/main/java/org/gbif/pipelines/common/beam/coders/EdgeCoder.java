package org.gbif.pipelines.common.beam.coders;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.gbif.pipelines.core.pojo.Edge;
import org.gbif.pipelines.io.avro.Record;

/**
 * A {@code EdgeCoder} encodes {@link Edge}s.
 *
 * @param <T> the type of the records of the edge
 */
@AllArgsConstructor(staticName = "of")
@Data
public class EdgeCoder<T extends SpecificRecordBase & Record> extends StructuredCoder<Edge<T>> {

  private final Coder<T> recordCoder;

  private final StringUtf8Coder fromIdCoder = StringUtf8Coder.of();

  private final StringUtf8Coder toIdCoder = StringUtf8Coder.of();

  @Override
  public void encode(Edge<T> edge, OutputStream outStream) throws IOException {
    if (edge == null) {
      throw new CoderException("cannot encode a null edge");
    }
    fromIdCoder.encode(edge.getFromId(), outStream);
    toIdCoder.encode(edge.getToId(), outStream);
    recordCoder.encode(edge.getRecord(), outStream);
  }

  @Override
  public Edge<T> decode(InputStream inStream) throws IOException {
    String fromId = fromIdCoder.decode(inStream);
    String toId = toIdCoder.decode(inStream);
    T r = recordCoder.decode(inStream);
    return Edge.of(fromId, toId, r);
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Arrays.asList(fromIdCoder, toIdCoder, recordCoder);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    verifyDeterministic(this, "FromId coder must be deterministic", getFromIdCoder());
    verifyDeterministic(this, "ToId coder must be deterministic", getToIdCoder());
    verifyDeterministic(this, "Record coder must be deterministic", getRecordCoder());
  }

  public KvCoder<String, T> getKvRecordCoder() {
    return KvCoder.of(StringUtf8Coder.of(), recordCoder);
  }

  public KvCoder<String, Edge<T>> getKvEdgeCoder() {
    return KvCoder.of(StringUtf8Coder.of(), this);
  }
}
