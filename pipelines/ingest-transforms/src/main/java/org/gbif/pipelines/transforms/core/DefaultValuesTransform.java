package org.gbif.pipelines.transforms.core;

import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.core.interpreters.core.MetadataInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.transforms.Transform;

import java.util.Objects;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.values.PCollectionView;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.VERBATIM;

/**
 * Beam level transformations to use verbatim default term values defined as MachineTags in an MetadataRecord.
 * transforms form {@link ExtendedRecord} to {@link ExtendedRecord}.
 */
@Slf4j
public class DefaultValuesTransform extends Transform<ExtendedRecord, ExtendedRecord> {

  private PCollectionView<MetadataRecord> metadataView;

  private DefaultValuesTransform() {
    super(ExtendedRecord.class, VERBATIM);
  }

  public static DefaultValuesTransform create() {
    return new DefaultValuesTransform();
  }


  public SingleOutput<ExtendedRecord, ExtendedRecord> interpret(PCollectionView<MetadataRecord> metadataView) {
    this.metadataView = metadataView;
    return ParDo.of(this).withSideInputs(metadataView);
  }

  @ProcessElement
  public void processElement(@Element ExtendedRecord source, OutputReceiver<ExtendedRecord> out, ProcessContext c) {
    MetadataRecord mr = c.sideInput(metadataView);
    if (Objects.nonNull(mr.getMachineTags()) && !mr.getMachineTags().isEmpty()) {
      ExtendedRecord erWithDefault = ExtendedRecord.newBuilder(source).build();

      Interpretation.from(erWithDefault).to(erWithDefault).via(MetadataInterpreter.interpretDefaultValues(mr));

      out.output(erWithDefault);
    } else {
      out.output(source);
    }
  }
}
