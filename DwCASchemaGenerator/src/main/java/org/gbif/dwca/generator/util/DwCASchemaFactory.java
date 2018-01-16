package org.gbif.dwca.generator.util;

import org.gbif.dwca.Category;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * Main Entry class to get or generate schema for DwCA.
 *
 * @author clf358
 */
public class DwCASchemaFactory {

  private final DwCADataModel model;

  public DwCASchemaFactory(DwCADataModel dwcModel) {
    this.model = dwcModel;
  }

  /**
   * gets Schema as String associated with the provided DwCA Category
   *
   * @param category DwCA Category
   * @param type     type Of Schema needed
   */
  public String getSchema(Category category, SchemaType type) {
    switch (type) {
      case Avro:
        return new DwCAAvroSchemaBuilder().buildSchema(category, model);
      default:
        throw new IllegalArgumentException("Schema type " + type.name() + " is not supported");
    }
  }

  /**
   * generates Schema for provided category and write as avsc file to the directory provided
   *
   * @param category      DwCA Category
   * @param type          type of Schema needed
   * @param pathForSchema path to directory where schema needed to be exported
   */
  public void generateSchema(Category category, SchemaType type, File pathForSchema) throws IOException {
    if (pathForSchema.isFile()) throw new IllegalArgumentException("Provide path to directory to generate schema");

    if (pathForSchema.isDirectory() && !pathForSchema.exists()) pathForSchema.mkdirs();
    File targetFile =
      new File(pathForSchema.getAbsolutePath().concat(File.separator).concat(category.name() + ".avsc"));
    String schema = getSchema(category, type);

    Files.write(targetFile.toPath(), schema.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * @param type type Of Schema
   *
   * @return list of schema for all the available categories
   */
  public List<String> getAllCategorySchema(SchemaType type) {
    List<String> schemas = new ArrayList<>();
    for (Category c : Category.values()) {
      schemas.add(getSchema(c, type));
    }
    return schemas;
  }

  /**
   * exports schemas to the provided directory in specified schema type
   *
   * @param pathForSchema provide the directory to export schema
   */
  public void generateAllCategorySchema(SchemaType type, File pathForSchema) throws IOException {
    for (Category c : Category.values()) {
      generateSchema(c, type, pathForSchema);
    }
  }

}
