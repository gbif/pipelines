package org.gbif.pipelines.spark.util;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.apache.spark.sql.types.StructType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.pipelines.core.config.model.TableBuildConfig;
import org.junit.jupiter.api.Test;

class TableUtilTest {

  @Test
  void dnaClassificationColumnsFromConfig() {
    TableBuildConfig config = new TableBuildConfig();
    config.setClassifications(Map.of("d7dddbf4-2cf0-4f39-9b2a-bb099caae36c", "gbif"));
    config.setDnaClassifications(Map.of("d7dddbf4-2cf0-4f39-9b2a-bb099caae36c", "gbif_dna"));

    StructType schema =
        StructType.fromDDL(TableUtil.getFieldDefinitions(config, DatasetType.OCCURRENCE));

    // same struct as the classification columns
    assertEquals(
        schema.apply("gbif_classification").dataType(),
        schema.apply("gbif_dna_classification").dataType());
  }
}
