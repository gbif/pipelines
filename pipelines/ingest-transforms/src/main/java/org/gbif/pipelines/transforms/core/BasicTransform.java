package org.gbif.pipelines.transforms.core;

import java.nio.file.Paths;
import java.time.Instant;
import java.util.Optional;
import java.util.Properties;

import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.core.interpreters.core.BasicInterpreter;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.common.HbaseConnectionFactory;
import org.gbif.pipelines.keygen.config.KeygenConfig;
import org.gbif.pipelines.keygen.config.KeygenConfigFactory;
import org.gbif.pipelines.transforms.Transform;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.hadoop.hbase.client.Connection;

import lombok.SneakyThrows;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.BASIC_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.BASIC;

/**
 * Beam level transformations for the DWC Occurrence, reads an avro, writs an avro, maps from value to keyValue and
 * transforms form {@link ExtendedRecord} to {@link BasicRecord}.
 *
 * @see <a href="https://dwc.tdwg.org/terms/#occurrence</a>
 */
public class BasicTransform extends Transform<ExtendedRecord, BasicRecord> {

  private final Counter counter = Metrics.counter(BasicTransform.class, BASIC_RECORDS_COUNT);

  private final KeygenConfig keygenConfig;
  private final String datasetId;
  private final boolean isTripletValid;
  private final boolean isOccurrenceIdValid;
  private final boolean useExtendedRecordId;

  private Connection connection;
  private HBaseLockingKeyService keygenService;

  private BasicTransform(KeygenConfig keygenConfig, String datasetId, boolean isTripletValid,
      boolean isOccurrenceIdValid, boolean useExtendedRecordId) {
    super(BasicRecord.class, BASIC);
    this.keygenConfig = keygenConfig;
    this.datasetId = datasetId;
    this.isTripletValid = isTripletValid;
    this.isOccurrenceIdValid = isOccurrenceIdValid;
    this.useExtendedRecordId = useExtendedRecordId;
  }

  public static BasicTransform create() {
    return new BasicTransform(null, null, false, false, false);
  }

  public static BasicTransform create(String propertiesPath, String datasetId, boolean isTripletValid,
      boolean isOccurrenceIdValid, boolean useExtendedRecordId) {
    KeygenConfig config = KeygenConfigFactory.create(Paths.get(propertiesPath));
    return new BasicTransform(config, datasetId, isTripletValid, isOccurrenceIdValid, useExtendedRecordId);
  }

  public static BasicTransform create(Properties properties, String datasetId, boolean isTripletValid,
      boolean isOccurrenceIdValid, boolean useExtendedRecordId) {
    KeygenConfig config = KeygenConfigFactory.create(properties);
    return new BasicTransform(config, datasetId, isTripletValid, isOccurrenceIdValid, useExtendedRecordId);
  }

  /** Maps {@link BasicRecord} to key value, where key is {@link BasicRecord#getId} */
  public MapElements<BasicRecord, KV<String, BasicRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, BasicRecord>>() {})
        .via((BasicRecord br) -> KV.of(br.getId(), br));
  }

  @SneakyThrows
  @Setup
  public void setup() {
    if (keygenConfig != null) {
      connection = HbaseConnectionFactory.create(keygenConfig.getHbaseZk());
      keygenService = new HBaseLockingKeyService(keygenConfig, connection, datasetId);
    }
  }

  @SneakyThrows
  @Teardown
  public void tearDown() {
    if (connection != null) {
      connection.close();
    }
  }

  @Override
  public void incCounter() {
    counter.inc();
  }

  @Override
  public Optional<BasicRecord> processElement(ExtendedRecord source) {

    BasicRecord br = BasicRecord.newBuilder()
        .setId(source.getId())
        .setGbifId(useExtendedRecordId && source.getCoreTerms().isEmpty() ? Long.parseLong(source.getId()) : null)
        .setCreated(Instant.now().toEpochMilli())
        .build();

    return Interpretation.from(source)
        .to(br)
        .when(er -> !er.getCoreTerms().isEmpty())
        .via(BasicInterpreter.interpretGbifId(keygenService, isTripletValid, isOccurrenceIdValid, useExtendedRecordId))
        .via(BasicInterpreter::interpretBasisOfRecord)
        .via(BasicInterpreter::interpretTypifiedName)
        .via(BasicInterpreter::interpretSex)
        .via(BasicInterpreter::interpretEstablishmentMeans)
        .via(BasicInterpreter::interpretLifeStage)
        .via(BasicInterpreter::interpretTypeStatus)
        .via(BasicInterpreter::interpretIndividualCount)
        .via(BasicInterpreter::interpretReferences)
        .get();
  }

}
