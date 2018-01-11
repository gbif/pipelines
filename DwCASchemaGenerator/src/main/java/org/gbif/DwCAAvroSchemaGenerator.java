package org.gbif;

import org.gbif.dwca.generator.util.DwCADataModel;
import org.gbif.dwca.generator.util.DwCAHTMLParsingTool;
import org.gbif.dwca.generator.util.DwCASchemaFactory;
import org.gbif.dwca.generator.util.SchemaType;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Run this class to generate DwCA schema
 *
 * @author clf358
 */
public class DwCAAvroSchemaGenerator {

  private static final Logger LOG = LogManager.getLogger(DwCAAvroSchemaGenerator.class);

  public static void main(String[] args) throws IOException {

    if (args.length != 1) {
      LOG.warn(" Run the program with DwCAAvroSchemaGenerator /path/to/schema");
      System.exit(1);
    }

    File file = new File(args[0]);
    if (!file.isDirectory()) {
      LOG.error("Provide directory to generate schema, provided path is not a directory");
      System.exit(1);
    }
    if (file.isDirectory() && !file.exists()) {
      file.mkdirs();
    }

    DwCAHTMLParsingTool tool = new DwCAHTMLParsingTool();
    DwCADataModel model = tool.parseAndGenerateModel();
    if (LOG.isDebugEnabled()) LOG.debug("Data Model generated " + model);

    DwCASchemaFactory schemaFactory = new DwCASchemaFactory(model);
    schemaFactory.generateAllCategorySchema(SchemaType.Avro, file);
    LOG.info("Schema generation finished at " + file.getAbsolutePath());
  }

}
