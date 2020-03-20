package org.gbif.pipelines.transforms.core;

import java.nio.file.Paths;
import java.time.Instant;
import java.util.Optional;
import java.util.Properties;
import java.util.function.BiConsumer;

import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.core.interpreters.core.BasicInterpreter;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.common.HbaseConnection;
import org.gbif.pipelines.keygen.common.HbaseConnectionFactory;
import org.gbif.pipelines.keygen.config.KeygenConfig;
import org.gbif.pipelines.keygen.config.KeygenConfigFactory;
import org.gbif.pipelines.transforms.SerializableConsumer;
import org.gbif.pipelines.transforms.Transform;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.hadoop.hbase.client.Connection;

import lombok.SneakyThrows;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.BASIC_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.BASIC;
import static org.gbif.pipelines.core.interpreters.core.BasicInterpreter.GBIF_ID_INVALID;
import static org.gbif.pipelines.core.interpreters.core.BasicInterpreter.interpretCopyGbifId;

/**
 * Beam level transformations for the DWC Occurrence, reads an avro, writs an avro, maps from value to keyValue and
 * transforms form {@link ExtendedRecord} to {@link BasicRecord}.
 *
 * @see <a href="https://dwc.tdwg.org/terms/#occurrence</a>
 */
public class BasicTransform extends Transform<ExtendedRecord, BasicRecord> {

  private final KeygenConfig keygenConfig;
  private final String datasetId;
  private final boolean isTripletValid;
  private final boolean isOccurrenceIdValid;
  private final boolean useExtendedRecordId;
  private final BiConsumer<ExtendedRecord, BasicRecord> gbifIdFn;

  private Connection connection;
  private HBaseLockingKeyService keygenService;

  private BasicTransform(KeygenConfig keygenConfig, String datasetId, boolean isTripletValid,
      boolean isOccurrenceIdValid, boolean useExtendedRecordId, BiConsumer<ExtendedRecord, BasicRecord> gbifIdFn) {
    super(BasicRecord.class, BASIC, BasicTransform.class.getName(), BASIC_RECORDS_COUNT);
    this.keygenConfig = keygenConfig;
    this.datasetId = datasetId;
    this.isTripletValid = isTripletValid;
    this.isOccurrenceIdValid = isOccurrenceIdValid;
    this.useExtendedRecordId = useExtendedRecordId;
    this.gbifIdFn = gbifIdFn;
  }

  public static BasicTransform create() {
    return new BasicTransform(null, null, false, false, false, null);
  }

  public static BasicTransform create(String propertiesPath, String datasetId, boolean isTripletValid,
      boolean isOccurrenceIdValid, boolean useExtendedRecordId) {
    KeygenConfig config = null;
    if (!useExtendedRecordId) {
      config = KeygenConfigFactory.create(Paths.get(propertiesPath));
    }
    return new BasicTransform(config, datasetId, isTripletValid, isOccurrenceIdValid, useExtendedRecordId, null);
  }

  public static BasicTransform create(Properties properties, String datasetId, boolean isTripletValid,
      boolean isOccurrenceIdValid, boolean useExtendedRecordId) {
    KeygenConfig config = null;
    if (!useExtendedRecordId) {
      config = KeygenConfigFactory.create(properties);
    }
    return new BasicTransform(config, datasetId, isTripletValid, isOccurrenceIdValid, useExtendedRecordId, null);
  }

  public static BasicTransform create(BiConsumer<ExtendedRecord, BasicRecord> gbifIdFn) {
    return new BasicTransform(null, null, false, false, true, gbifIdFn);
  }

  /** Maps {@link BasicRecord} to key value, where key is {@link BasicRecord#getId} */
  public MapElements<BasicRecord, KV<String, BasicRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, BasicRecord>>() {})
        .via((BasicRecord br) -> KV.of(br.getId(), br));
  }

  /** Maps {@link BasicRecord} to key value, where key is {@link BasicRecord#getGbifId()} */
  public MapElements<BasicRecord, KV<String, BasicRecord>> toGbifIdKv() {
    return MapElements.into(new TypeDescriptor<KV<String, BasicRecord>>() {})
        .via((BasicRecord br) -> {
          String key = Optional.ofNullable(br.getGbifId()).map(Object::toString).orElse(GBIF_ID_INVALID);
          return KV.of(key, br);
        });
  }

  public BasicTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  /** Initializes resources using singleton factory can be useful in case of non-Beam pipeline */
  public BasicTransform init() {
    if (keygenConfig != null) {
      connection = HbaseConnectionFactory.getInstance(keygenConfig.getHbaseZk()).getConnection();
      keygenService = new HBaseLockingKeyService(keygenConfig, connection, datasetId);
    }
    return this;
  }

  /** Beam @Setup initializes resources */
  @SneakyThrows
  @Setup
  public void setup() {
    if (keygenConfig != null) {
      connection = HbaseConnection.create(keygenConfig.getHbaseZk());
      keygenService = new HBaseLockingKeyService(keygenConfig, connection, datasetId);
    }
  }

  /** Beam @Teardown closes initialized resources */
  @SneakyThrows
  @Teardown
  public void tearDown() {
    if (connection != null) {
      connection.close();
    }
  }

  @Override
  public Optional<BasicRecord> convert(ExtendedRecord source) {

    BasicRecord br = BasicRecord.newBuilder()
        .setId(source.getId())
        .setGbifId(useExtendedRecordId && source.getCoreTerms().isEmpty() ? Long.parseLong(source.getId()) : null)
        .setCreated(Instant.now().toEpochMilli())
        .build();

    if (useExtendedRecordId && source.getCoreTerms().isEmpty()) {
      interpretCopyGbifId().accept(source, br);
    }

    return Interpretation.from(source)
        .to(br)
        .when(er -> !er.getCoreTerms().isEmpty())
        .via(BasicInterpreter.interpretGbifId(keygenService, isTripletValid, isOccurrenceIdValid, useExtendedRecordId, gbifIdFn))
        .via(BasicInterpreter::interpretBasisOfRecord)
        .via(BasicInterpreter::interpretTypifiedName)
        .via(BasicInterpreter::interpretSex)
        .via(BasicInterpreter::interpretEstablishmentMeans)
        .via(BasicInterpreter::interpretLifeStage)
        .via(BasicInterpreter::interpretTypeStatus)
        .via(BasicInterpreter::interpretIndividualCount)
        .via(BasicInterpreter::interpretReferences)
        .via(BasicInterpreter::interpretOrganismQuantity)
        .via(BasicInterpreter::interpretOrganismQuantityType)
        .via(BasicInterpreter::interpretSampleSizeUnit)
        .via(BasicInterpreter::interpretSampleSizeValue)
        .via(BasicInterpreter::interpretRelativeOrganismQuantity)
        .via(BasicInterpreter::interpretLicense)
        .via(BasicInterpreter::interpretIdentifiedByIds)
        .via(BasicInterpreter::interpretRecordedByIds)
        .get();
  }
}
