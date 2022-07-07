package au.org.ala.pipelines.transforms;

import au.org.ala.pipelines.interpreters.ALATemporalInterpreter;
import java.util.List;
import java.util.Optional;
import lombok.Builder;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.functions.SerializableFunction;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.core.TemporalInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.transforms.Transform;

public class ALATemporalTransform extends Transform<ExtendedRecord, TemporalRecord> {

  private final SerializableFunction<String, String> preprocessDateFn;
  private final List<DateComponentOrdering> orderings;
  private TemporalInterpreter temporalInterpreter;
  private ALATemporalInterpreter alaTemporalInterpreter;

  @Builder(buildMethodName = "create")
  private ALATemporalTransform(
      List<DateComponentOrdering> orderings,
      SerializableFunction<String, String> preprocessDateFn) {
    super(
        TemporalRecord.class,
        RecordType.TEMPORAL,
        ALATemporalTransform.class.getName(),
        "alaTemporalCount");
    this.orderings = orderings;
    this.preprocessDateFn = preprocessDateFn;
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {
    if (temporalInterpreter == null) {
      temporalInterpreter =
          TemporalInterpreter.builder()
              .orderings(orderings)
              .preprocessDateFn(preprocessDateFn)
              .create();
    }
    if (alaTemporalInterpreter == null) {
      alaTemporalInterpreter =
          ALATemporalInterpreter.builder()
              .orderings(orderings)
              .preprocessDateFn(preprocessDateFn)
              .create();
    }
  }

  public ALATemporalTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  @Override
  public Optional<TemporalRecord> convert(ExtendedRecord source) {
    TemporalRecord tr = TemporalRecord.newBuilder().setId(source.getId()).build();
    return Interpretation.from(source)
        .to(tr)
        .when(er -> !er.getCoreTerms().isEmpty())
        .via(temporalInterpreter::interpretTemporal)
        .via(temporalInterpreter::interpretModified)
        .via(temporalInterpreter::interpretDateIdentified)
        .via(alaTemporalInterpreter::checkRecordDateQuality)
        .via(alaTemporalInterpreter::checkDateIdentified)
        .via(alaTemporalInterpreter::checkGeoreferencedDate)
        .via(ALATemporalInterpreter::checkDatePrecision)
        .getOfNullable();
  }
}
