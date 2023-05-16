package au.org.ala.pipelines.transforms;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.SEEDBANK_RECORDS_COUNT;
import static org.gbif.pipelines.core.utils.ModelUtils.hasExtension;

import au.org.ala.pipelines.common.ALARecordTypes;
import au.org.ala.pipelines.interpreters.SeedbankInterpreter;
import java.util.List;
import java.util.Optional;
import lombok.Builder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.functions.SerializableFunction;
import org.gbif.pipelines.core.interpreters.ExtensionInterpretation;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.SeedbankRecord;
import org.gbif.pipelines.transforms.Transform;

public class SeedbankTransform extends Transform<ExtendedRecord, SeedbankRecord> {

  public static final String SEED_BANK_ROW_TYPE =
      "http://ala.org.au/terms/seedbank/0.1/SeedbankRecord";
  private SeedbankInterpreter seedbankInterpreter;
  private List<DateComponentOrdering> orderings;
  private SerializableFunction<String, String> preprocessDateFn;

  @Builder(buildMethodName = "create")
  private SeedbankTransform(
      List<DateComponentOrdering> orderings,
      SerializableFunction<String, String> preprocessDateFn) {
    super(
        SeedbankRecord.class,
        ALARecordTypes.SEEDBANK,
        SeedbankTransform.class.getName(),
        SEEDBANK_RECORDS_COUNT);
    this.orderings = orderings;
    this.preprocessDateFn = preprocessDateFn;
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {
    if (seedbankInterpreter == null) {
      seedbankInterpreter =
          SeedbankInterpreter.builder()
              .orderings(orderings)
              .preprocessDateFn(preprocessDateFn)
              .create();
    }
  }

  public MapElements<SeedbankRecord, KV<String, SeedbankRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, SeedbankRecord>>() {})
        .via((SeedbankRecord lr) -> KV.of(lr.getId(), lr));
  }

  public SeedbankTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  @Override
  public Optional<SeedbankRecord> convert(ExtendedRecord source) {
    if (!hasExtension(source, SEED_BANK_ROW_TYPE)) {
      return Optional.empty();
    }
    ExtensionInterpretation.Result<SeedbankRecord> sr = seedbankInterpreter.HANDLER.convert(source);
    if (sr.get().isPresent()) {
      sr.get().get().setId(source.getId());
    }
    return sr.get();
  }
}
