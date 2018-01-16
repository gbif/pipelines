package org.gbif.dwca.test;

import org.gbif.dwca.Category;
import org.gbif.dwca.generator.util.DwCADataModel;
import org.gbif.dwca.generator.util.DwCAHTMLParsingTool;
import org.gbif.dwca.generator.util.DwCASchemaFactory;
import org.gbif.dwca.generator.util.SchemaType;

import java.io.File;
import java.io.IOException;
import java.util.List;

import junit.framework.Assert;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.testng.annotations.Test;

public class DwCASchemaFactoryTest {

  private static final Logger LOG = LogManager.getLogger(DwCASchemaFactoryTest.class);

  @Test(description = "getting schema as string ")
  public void test1() throws IOException {
    DwCAHTMLParsingTool tool = new DwCAHTMLParsingTool();
    DwCADataModel dataModel = tool.parseAndGenerateModel();
    DwCASchemaFactory factory = new DwCASchemaFactory(dataModel);
    String avroSchema = factory.getSchema(Category.ExtendedOccurence, SchemaType.Avro);
    if (LOG.isDebugEnabled()) LOG.debug(avroSchema);
    Assert.assertNotNull(avroSchema);
    Assert.assertEquals(false, avroSchema.isEmpty());
  }

  @Test(description = "get all schemas and check count")
  public void test2() throws IOException {
    DwCAHTMLParsingTool tool = new DwCAHTMLParsingTool();
    DwCADataModel dataModel = tool.parseAndGenerateModel();
    DwCASchemaFactory factory = new DwCASchemaFactory(dataModel);
    List<String> allSchemas = factory.getAllCategorySchema(SchemaType.Avro);
    if (LOG.isDebugEnabled()) LOG.debug(allSchemas);
    Assert.assertNotNull(allSchemas);
    Assert.assertEquals(16, allSchemas.size());
    for (String schema : allSchemas) {
      Assert.assertEquals(false, schema.isEmpty());
    }
  }

  @Test(description = "export Extended occurence schema and check if it exists")
  public void test3() throws IOException {
    DwCAHTMLParsingTool tool = new DwCAHTMLParsingTool();
    DwCADataModel dataModel = tool.parseAndGenerateModel();
    DwCASchemaFactory factory = new DwCASchemaFactory(dataModel);
    File f = new File(DwCASchemaFactory.class.getClassLoader().getResource("").getFile());
    factory.generateSchema(Category.ExtendedOccurence, SchemaType.Avro, f);

    Assert.assertEquals(true, f.exists());

  }

  @Test(description = "export all schema and verify they exist")
  public void test4() throws IOException {
    DwCAHTMLParsingTool tool = new DwCAHTMLParsingTool();
    DwCADataModel dataModel = tool.parseAndGenerateModel();
    DwCASchemaFactory factory = new DwCASchemaFactory(dataModel);
    File f = new File(DwCASchemaFactory.class.getClassLoader().getResource("").getFile());
    factory.generateAllCategorySchema(SchemaType.Avro, f);

    Assert.assertEquals(true, f.exists());

  }

}
