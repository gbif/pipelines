package org.gbif.pipelines.transforms.extension;

import static org.gbif.api.model.pipelines.InterpretationType.RecordType.AMPLIFICATION;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AMPLIFICATION_RECORDS_COUNT;
import static org.gbif.pipelines.core.utils.ModelUtils.hasExtension;

import java.time.Instant;
import java.util.Optional;
import lombok.Builder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.extension.AmplificationInterpreter;
import org.gbif.pipelines.core.ws.blast.BlastServiceClient;
import org.gbif.pipelines.io.avro.AmplificationRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transforms.Transform;

/**
 * Beam level transformations for the Amplification extension, reads an avro, writes an avro, maps
 * from value to keyValue and transforms form {@link ExtendedRecord} to {@link AmplificationRecord}.
 *
 * <p>ParDo runs sequence of interpretations for {@link AmplificationRecord} using {@link
 * ExtendedRecord} as a source and {@link AmplificationInterpreter} as interpretation steps
 *
 * @see <a
 *     href="http://rs.gbif.org/extension/ggbn/amplification.xml">http://rs.gbif.org/extension/ggbn/amplification.xml</a>
 */
public class AmplificationTransform extends Transform<ExtendedRecord, AmplificationRecord> {

  private final SerializableSupplier<BlastServiceClient> clientSupplier;
  private BlastServiceClient client;

  @Builder(buildMethodName = "create")
  private AmplificationTransform(SerializableSupplier<BlastServiceClient> clientSupplier) {
    super(
        AmplificationRecord.class,
        AMPLIFICATION,
        AmplificationTransform.class.getName(),
        AMPLIFICATION_RECORDS_COUNT);
    this.clientSupplier = clientSupplier;
  }

  /**
   * Maps {@link AmplificationRecord} to key value, where key is {@link AmplificationRecord#getId}
   */
  public MapElements<AmplificationRecord, KV<String, AmplificationRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, AmplificationRecord>>() {})
        .via((AmplificationRecord ar) -> KV.of(ar.getId(), ar));
  }

  public AmplificationTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  @Setup
  public void setup() {
    if (client == null && clientSupplier != null) {
      client = clientSupplier.get();
    }
  }

  /** Beam @Setup can be applied only to void method */
  public AmplificationTransform init() {
    setup();
    return this;
  }

  @Override
  public Optional<AmplificationRecord> convert(ExtendedRecord source) {
    return Interpretation.from(source)
        .to(
            er ->
                AmplificationRecord.newBuilder()
                    .setId(er.getId())
                    .setCreated(Instant.now().toEpochMilli())
                    .build())
        .when(er -> hasExtension(er, AmplificationInterpreter.EXTENSION_ROW_TYPE))
        .via(AmplificationInterpreter.interpret(client))
        .getOfNullable();
  }
}
