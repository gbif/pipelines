package org.gbif.pipelines.interpretation;

import org.gbif.pipelines.core.tools.AvroSchemaGenerator;

import java.io.IOException;

import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class TaxonSchemaGeneratorTest {

  private static final String DEFAULT_TAXON_SCHEMA_PATH = "src/main/avro/taxonRecord.avsc";

  @Test
  @Ignore
  public void generateDefaultTaxonomicSchemaTest() {
    Schema schemaGenerated = AvroSchemaGenerator.generateDefaultTaxonomicSchema();
    System.out.println(schemaGenerated.toString(true));

    try {
      AvroSchemaGenerator.writeSchemaToFile(DEFAULT_TAXON_SCHEMA_PATH, schemaGenerated);
    } catch (IOException e) {
      Assert.fail("Could not generate default taxonomic schema in path "
                  + DEFAULT_TAXON_SCHEMA_PATH
                  + ": "
                  + e.getMessage());
    }

  }

}
