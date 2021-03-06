package org.gbif.pipelines.transforms.table;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.RESOURCE_RELATIONSHIP_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.RESOURCE_RELATIONSHIP_TABLE;

import java.util.Set;
import lombok.Builder;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.core.converters.ResourceRelationshipTableConverter;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.extension.dwc.ResourceRelationshipTable;

public class ResourceRelationshipTableTransform extends TableTransform<ResourceRelationshipTable> {

  @Builder
  public ResourceRelationshipTableTransform(
      TupleTag<ExtendedRecord> extendedRecordTag,
      TupleTag<BasicRecord> basicRecordTag,
      String path,
      Integer numShards,
      Set<String> types) {
    super(
        ResourceRelationshipTable.class,
        RESOURCE_RELATIONSHIP_TABLE,
        ResourceRelationshipTableTransform.class.getName(),
        RESOURCE_RELATIONSHIP_TABLE_RECORDS_COUNT,
        ResourceRelationshipTableConverter::convert);
    this.setExtendedRecordTag(extendedRecordTag)
        .setBasicRecordTag(basicRecordTag)
        .setPath(path)
        .setNumShards(numShards)
        .setTypes(types);
  }
}
