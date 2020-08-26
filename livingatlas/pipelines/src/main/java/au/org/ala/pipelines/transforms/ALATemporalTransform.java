package au.org.ala.pipelines.transforms;

import static org.gbif.common.parsers.date.DateComponentOrdering.DMY_FORMATS;
import static org.gbif.common.parsers.date.DateComponentOrdering.MDY_FORMATS;

import java.util.Optional;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.core.interpreters.core.DefaultTemporalInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.parsers.config.model.PipelinesConfig;
import org.gbif.pipelines.transforms.SerializableConsumer;
import org.gbif.pipelines.transforms.Transform;

public class ALATemporalTransform extends Transform<ExtendedRecord, TemporalRecord> {

  private DefaultTemporalInterpreter temporalInterpreter;

  private ALATemporalTransform() {
    super(
        TemporalRecord.class,
        RecordType.TEMPORAL,
        ALATemporalTransform.class.getName(),
        "alaTemporalCount");
  }

  public static ALATemporalTransform create() {
    ALATemporalTransform tr = new ALATemporalTransform();
    tr.temporalInterpreter = DefaultTemporalInterpreter.getInstance();
    return tr;
  }

  public static ALATemporalTransform create(PipelinesConfig config) {
    ALATemporalTransform tr = new ALATemporalTransform();
    if (config.getDefaultDateFormat().equalsIgnoreCase("DMY")) {
      tr.temporalInterpreter = DefaultTemporalInterpreter.getInstance(DMY_FORMATS);
    } else if (config.getDefaultDateFormat().equalsIgnoreCase("MDY")) {
      tr.temporalInterpreter = DefaultTemporalInterpreter.getInstance(MDY_FORMATS);
    } else tr.temporalInterpreter = DefaultTemporalInterpreter.getInstance();
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
        .via(temporalInterpreter::interpretTemporal);

    // the id is null when there is an error in the interpretation. In these
    // cases we do not write the taxonRecord because it is totally empty.
    return Optional.of(tr);
  }
}
