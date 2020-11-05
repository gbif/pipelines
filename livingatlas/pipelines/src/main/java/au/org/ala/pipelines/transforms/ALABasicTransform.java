package au.org.ala.pipelines.transforms;

import static org.gbif.pipelines.core.interpreters.core.BasicInterpreter.interpretCopyGbifId;

import au.org.ala.pipelines.interpreters.ALABasicInterpreter;
import java.time.Instant;
import java.util.Optional;
import java.util.function.BiConsumer;
import org.gbif.api.vocabulary.OccurrenceStatus;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.core.BasicInterpreter;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.transforms.core.BasicTransform;
import org.gbif.vocabulary.lookup.VocabularyLookup;


public class ALABasicTransform extends BasicTransform {

  protected ALABasicTransform(boolean isTripletValid, boolean isOccurrenceIdValid,
      boolean useExtendedRecordId,
      BiConsumer<ExtendedRecord, BasicRecord> gbifIdFn,
      SerializableSupplier<HBaseLockingKeyService> keygenServiceSupplier,
      SerializableSupplier<VocabularyLookup> lifeStageLookupSupplier,
      SerializableSupplier<KeyValueStore<String, OccurrenceStatus>> occStatusKvStoreSupplier) {
    super(isTripletValid, isOccurrenceIdValid, useExtendedRecordId, gbifIdFn, keygenServiceSupplier,
        lifeStageLookupSupplier, occStatusKvStoreSupplier);
  }

  @Override
  public Optional<BasicRecord> convert(ExtendedRecord source) {
    BasicRecord br =
        BasicRecord.newBuilder()
            .setId(source.getId())
            .setGbifId(
                useExtendedRecordId && source.getCoreTerms().isEmpty()
                    ? Long.parseLong(source.getId())
                    : null)
            .setCreated(Instant.now().toEpochMilli())
            .build();

    if (useExtendedRecordId && source.getCoreTerms().isEmpty()) {
      interpretCopyGbifId().accept(source, br);
    }

    return Interpretation.from(source)
        .to(br)
        .when(er -> !er.getCoreTerms().isEmpty())
        .via(
            BasicInterpreter.interpretGbifId(
                keygenService, isTripletValid, isOccurrenceIdValid, useExtendedRecordId, gbifIdFn))
        .via(BasicInterpreter::interpretBasisOfRecord)
        .via(BasicInterpreter::interpretTypifiedName)
        .via(BasicInterpreter::interpretSex)
        .via(BasicInterpreter::interpretEstablishmentMeans)
        .via(BasicInterpreter.interpretLifeStage(lifeStageLookup))
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
        .via(BasicInterpreter.interpretOccurrenceStatus(occStatusKvStore))
        .getOfNullable();
  }
}
