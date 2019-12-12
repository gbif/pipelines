package org.gbif.pipelines.transforms.core;

import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.core.interpreters.core.TaggedValuesInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.TaggedValueRecord;
import org.gbif.pipelines.transforms.Transform;

import java.util.Optional;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.TAGGED_VALUES_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.TAGGED_VALUES;

/**
 * Beam level transformations for the GBIF tagged value in dataset metadata, reads an avro, writes an avro, maps from value to keyValue and
 * transforms form {@link ExtendedRecord} to {@link TaggedValueRecord}.
 * <p>
 * ParDo runs sequence of interpretations for {@link TaggedValueRecord} using {@link ExtendedRecord}
 * as a source and {@link TaggedValuesInterpreter} as interpretation steps
 */
public class TaggedValuesTransform extends Transform<ExtendedRecord, TaggedValueRecord> {

  private final MetadataRecord mr;

  private TaggedValuesTransform(MetadataRecord metadataRecord) {
    super(TaggedValueRecord.class, TAGGED_VALUES, TaggedValueRecord.class.getName(), TAGGED_VALUES_RECORDS_COUNT);
    this.mr = metadataRecord;
  }

  public static TaggedValuesTransform create(MetadataRecord metadataRecord) {
    return new TaggedValuesTransform(metadataRecord);
  }

  @Override
  public Optional<TaggedValueRecord> convert(ExtendedRecord source) {
    return Interpretation.from(source)
            .to(id -> TaggedValueRecord.newBuilder().setId(source.getId()).build())
            .via(TaggedValuesInterpreter.interpret(mr))
            .get();

  }

}
