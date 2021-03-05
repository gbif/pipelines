package au.org.ala.pipelines.transforms;

import static au.org.ala.pipelines.common.ALARecordTypes.JACKKNIFE_OUTLIER;

import java.util.Optional;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.JackKnifeOutlierRecord;
import org.gbif.pipelines.transforms.Transform;

@Slf4j
public class JackKnifeOutlierTransform extends Transform<ExtendedRecord, JackKnifeOutlierRecord> {

  @Builder(buildMethodName = "create")
  private JackKnifeOutlierTransform() {

    super(
        JackKnifeOutlierRecord.class,
        JACKKNIFE_OUTLIER,
        JackKnifeOutlierTransform.class.getName(),
        "jackknifeOutlierRecordCount");
  }

  /**
   * Maps {@link JackKnifeOutlierRecord} to key value, where key is {@link
   * JackKnifeOutlierRecord#getId}
   */
  public MapElements<JackKnifeOutlierRecord, KV<String, JackKnifeOutlierRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, JackKnifeOutlierRecord>>() {})
        .via((JackKnifeOutlierRecord lr) -> KV.of(lr.getId(), lr));
  }

  @Override
  public SingleOutput<ExtendedRecord, JackKnifeOutlierRecord> interpret() {
    return ParDo.of(this);
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {}

  /** Beam @Teardown closes initialized resources */
  @Teardown
  public void tearDown() {}

  @Override
  public Optional<JackKnifeOutlierRecord> convert(ExtendedRecord source) {
    JackKnifeOutlierRecord jkor = JackKnifeOutlierRecord.newBuilder().setId(source.getId()).build();

    Interpretation.from(source).to(jkor);

    // the id is null when there is an error in the interpretation. In these
    // cases we do not write the taxonRecord because it is totally empty.
    return Optional.of(jkor);
  }
}
