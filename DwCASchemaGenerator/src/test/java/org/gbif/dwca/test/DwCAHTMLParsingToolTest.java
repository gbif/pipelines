package org.gbif.dwca.test;

import org.gbif.dwca.generator.util.DwCADataModel;
import org.gbif.dwca.generator.util.DwCAHTMLParsingTool;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

public class DwCAHTMLParsingToolTest {

  @Test(description = "check tool could parse the webpage")
  public void test1() throws IOException {
    DwCAHTMLParsingTool tool = new DwCAHTMLParsingTool();
    DwCADataModel dataModel = tool.parseAndGenerateModel();

    Assert.assertEquals(dataModel.getCategories().size(), 16);
    Assert.assertEquals(dataModel.getIdentifierCategoryMap().keySet().contains("ExtendedOccurrence"), true);
    Assert.assertEquals(dataModel.getListOfAllTerms().size(), 200);
  }

}
