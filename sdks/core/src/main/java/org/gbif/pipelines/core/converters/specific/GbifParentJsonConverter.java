package org.gbif.pipelines.core.converters.specific;

import lombok.experimental.SuperBuilder;
import org.gbif.pipelines.core.converters.JsonConverter;
import org.gbif.pipelines.core.converters.ParentJsonConverter;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.json.EventJsonRecord.Builder;

@SuperBuilder
public class GbifParentJsonConverter extends ParentJsonConverter {

  private final TaxonRecord taxon;

  @Override
  protected void mapTaxonRecord(Builder builder) {
    builder.setGbifClassification(JsonConverter.convertClassification(verbatim, taxon));
  }
}
