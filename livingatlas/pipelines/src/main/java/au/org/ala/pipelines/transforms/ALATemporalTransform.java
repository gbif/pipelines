package au.org.ala.pipelines.transforms;

import au.org.ala.pipelines.interpreters.ALATemporalInterpreter;
import java.util.Optional;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.transforms.SerializableConsumer;
import org.gbif.pipelines.transforms.Transform;

public class ALATemporalTransform extends Transform<ExtendedRecord, TemporalRecord> {

  private ALATemporalTransform() {
    super(
        TemporalRecord.class,
        RecordType.TEMPORAL,
        ALATemporalTransform.class.getName(),
        "alaTemporalCount");
  }

  public static ALATemporalTransform create() {
    ALATemporalTransform tr = new ALATemporalTransform();
    ALATemporalInterpreter.init();
    return tr;
  }

  public ALATemporalTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  @Override
  public Optional<TemporalRecord> convert(ExtendedRecord source) {

    TemporalRecord tr = TemporalRecord.newBuilder().setId(source.getId()).build();
    Interpretation.from(source)
        .to(tr)
        .when(er -> !er.getCoreTerms().isEmpty())
        .via(ALATemporalInterpreter::interpretTemporal);

    // the id is null when there is an error in the interpretation. In these
    // cases we do not write the taxonRecord because it is totally empty.
    return Optional.of(tr);
  }
}
