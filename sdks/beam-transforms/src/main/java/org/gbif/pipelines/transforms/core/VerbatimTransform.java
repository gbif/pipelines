package org.gbif.pipelines.transforms.core;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.VERBATIM_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.VERBATIM;
import static org.gbif.pipelines.core.utils.ModelUtils.extractValue;

import java.util.Optional;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transforms.Transform;

/**
 * Beam level transformations for the raw representation of DWC, reads an avro, writes an avro, maps
 * from value to keyValue
 */
public class VerbatimTransform extends Transform<ExtendedRecord, ExtendedRecord> {

  private VerbatimTransform() {
    super(
        ExtendedRecord.class, VERBATIM, VerbatimTransform.class.getName(), VERBATIM_RECORDS_COUNT);
  }

  public static VerbatimTransform create() {
    return new VerbatimTransform();
  }

  /** Maps {@link ExtendedRecord} to key value, where key is {@link ExtendedRecord#getId} */
  public MapElements<ExtendedRecord, KV<String, ExtendedRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, ExtendedRecord>>() {})
        .via((ExtendedRecord er) -> KV.of(er.getId(), er));
  }

  /**
   * Maps parent event IDs to key value, where key is {@link ExtendedRecord#getId} and the value is
   * the {@link DwcTerm#parentEventID}.
   */
  public MapElements<ExtendedRecord, KV<String, String>> toParentEventsKv() {
    return MapElements.into(new TypeDescriptor<KV<String, String>>() {})
        .via((ExtendedRecord er) -> KV.of(er.getId(), extractValue(er, DwcTerm.parentEventID)));
  }

  /** Create an empty collection of {@link PCollection<ExtendedRecord>} */
  public PCollection<ExtendedRecord> emptyCollection(Pipeline p) {
    return Create.empty(TypeDescriptor.of(ExtendedRecord.class)).expand(PBegin.in(p));
  }

  public VerbatimTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  @Override
  public Optional<ExtendedRecord> convert(ExtendedRecord source) {
    return Optional.ofNullable(source);
  }
}
