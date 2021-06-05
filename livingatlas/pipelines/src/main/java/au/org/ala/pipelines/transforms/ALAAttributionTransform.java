package au.org.ala.pipelines.transforms;

import static au.org.ala.pipelines.common.ALARecordTypes.ALA_ATTRIBUTION;

import au.org.ala.kvs.client.ALACollectionLookup;
import au.org.ala.kvs.client.ALACollectionMatch;
import au.org.ala.pipelines.interpreters.ALAAttributionInterpreter;
import java.util.Optional;
import lombok.Builder;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.transforms.Transform;

/**
 * ALA attribution transform for adding ALA attribution retrieved from the collectory to interpreted
 * occurrence data.
 *
 * <p>Beam level transformations for the DWC Taxon, reads an avro, writes an avro, maps from value
 * to keyValue and transforms form {@link ExtendedRecord} to {@link ALAAttributionRecord}.
 *
 * <p>ParDo runs sequence of interpretations for {@link ALAAttributionRecord} using {@link
 * ExtendedRecord} as a source and {@link ALAAttributionInterpreter} as interpretation steps
 *
 * @see <a href="https://dwc.tdwg.org/terms/#taxon</a>
 */
@Slf4j
public class ALAAttributionTransform extends Transform<ExtendedRecord, ALAAttributionRecord> {

  private KeyValueStore<ALACollectionLookup, ALACollectionMatch> collectionKvStore;
  private final SerializableSupplier<KeyValueStore<ALACollectionLookup, ALACollectionMatch>>
      collectionKvStoreSupplier;

  @Setter private PCollectionView<ALAMetadataRecord> metadataView;

  @Builder(buildMethodName = "create")
  private ALAAttributionTransform(
      KeyValueStore<ALACollectionLookup, ALACollectionMatch> collectionKvStore,
      SerializableSupplier<KeyValueStore<ALACollectionLookup, ALACollectionMatch>>
          collectionKvStoreSupplier,
      PCollectionView<ALAMetadataRecord> metadataView) {
    super(
        ALAAttributionRecord.class,
        ALA_ATTRIBUTION,
        ALAAttributionTransform.class.getName(),
        "alaAttributionRecordsCount");
    this.collectionKvStore = collectionKvStore;
    this.collectionKvStoreSupplier = collectionKvStoreSupplier;
    this.metadataView = metadataView;
  }

  @Override
  public ParDo.SingleOutput<ExtendedRecord, ALAAttributionRecord> interpret() {
    return ParDo.of(this).withSideInputs(metadataView);
  }

  public ALAAttributionTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  /** Beam @Setup can be applied only to void method */
  public ALAAttributionTransform init() {
    setup();
    return this;
  }

  /** Maps {@link ALATaxonRecord} to key value, where key is {@link TaxonRecord#getId} */
  public MapElements<ALAAttributionRecord, KV<String, ALAAttributionRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, ALAAttributionRecord>>() {})
        .via((ALAAttributionRecord tr) -> KV.of(tr.getId(), tr));
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {
    if (collectionKvStore == null && collectionKvStoreSupplier != null) {
      collectionKvStore = collectionKvStoreSupplier.get();
    }
  }

  @Override
  public Optional<ALAAttributionRecord> convert(ExtendedRecord source) {
    throw new IllegalArgumentException("Method is not implemented!");
  }

  @Override
  @ProcessElement
  public void processElement(ProcessContext c) {
    processElement(c.element(), c.sideInput(metadataView)).ifPresent(c::output);
  }

  public Optional<ALAAttributionRecord> processElement(
      ExtendedRecord source, ALAMetadataRecord mdr) {
    return Interpretation.from(source)
        .to(ALAAttributionRecord.newBuilder().setId(source.getId()).build())
        .via(ALAAttributionInterpreter.interpretCodes(collectionKvStore, mdr))
        .getOfNullable();
  }
}
