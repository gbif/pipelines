package org.gbif.pipelines.transforms.core;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.GRSCICOLL_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.GRSCICOLL;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import lombok.Builder;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.grscicoll.GrscicollLookupRequest;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.core.GrscicollInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.transforms.Transform;
import org.gbif.rest.client.grscicoll.GrscicollLookupResponse;

@Slf4j
public class GrscicollTransform extends Transform<ExtendedRecord, GrscicollRecord> {

  private final SerializableSupplier<KeyValueStore<GrscicollLookupRequest, GrscicollLookupResponse>>
      kvStoreSupplier;
  private KeyValueStore<GrscicollLookupRequest, GrscicollLookupResponse> kvStore;

  @Setter private PCollectionView<MetadataRecord> metadataView;

  @Builder(buildMethodName = "create")
  private GrscicollTransform(
      SerializableSupplier<KeyValueStore<GrscicollLookupRequest, GrscicollLookupResponse>>
          kvStoreSupplier,
      PCollectionView<MetadataRecord> metadataView) {
    super(
        GrscicollRecord.class,
        GRSCICOLL,
        GrscicollTransform.class.getName(),
        GRSCICOLL_RECORDS_COUNT);
    this.kvStoreSupplier = kvStoreSupplier;
    this.metadataView = metadataView;
  }

  /** Maps {@link GrscicollRecord} to key value, where key is {@link GrscicollRecord#getId} */
  public MapElements<GrscicollRecord, KV<String, GrscicollRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, GrscicollRecord>>() {})
        .via((GrscicollRecord gr) -> KV.of(gr.getId(), gr));
  }

  public GrscicollTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  @Override
  public SingleOutput<ExtendedRecord, GrscicollRecord> interpret() {
    return ParDo.of(this).withSideInputs(metadataView);
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {
    if (kvStore == null && kvStoreSupplier != null) {
      log.info("Initialize GrscicollLookupKvStore");
      kvStore = kvStoreSupplier.get();
    }
  }

  /** Beam @Setup can be applied only to void method */
  public GrscicollTransform init() {
    setup();
    return this;
  }

  /** Beam @Teardown closes initialized resources */
  @Teardown
  public void tearDown() {
    if (Objects.nonNull(kvStore)) {
      try {
        log.info("Close GrscicollLookupKvStore");
        kvStore.close();
      } catch (IOException ex) {
        log.error("Error closing KV Store", ex);
      }
    }
  }

  @Override
  public Optional<GrscicollRecord> convert(ExtendedRecord source) {
    throw new IllegalArgumentException("Method is not implemented!");
  }

  @Override
  @ProcessElement
  public void processElement(ProcessContext c) {
    ExtendedRecord er = c.element();
    processElement(er, c.sideInput(metadataView)).ifPresent(c::output);
  }

  public Optional<GrscicollRecord> processElement(ExtendedRecord source, MetadataRecord mdr) {
    if (source == null) {
      return Optional.empty();
    }

    return Interpretation.from(source)
        .to(GrscicollRecord.newBuilder().setCreated(Instant.now().toEpochMilli()).build())
        .when(er -> !er.getCoreTerms().isEmpty())
        .via(GrscicollInterpreter.grscicollInterpreter(kvStore, mdr))
        .skipWhen(gr -> gr.getId() == null)
        .getOfNullable();
  }
}
