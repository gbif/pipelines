package org.gbif.pipelines.esindexing.response;

import org.gbif.pipelines.esindexing.common.JsonHandler;

import java.io.UnsupportedEncodingException;
import java.util.Set;

import org.apache.http.HttpEntity;
import org.apache.http.nio.entity.NStringEntity;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests the {@link ResponseParser}. */
public class ResponseParserTest {

  @Test
  public void parseCreatedIndexResponseTest() {

    // State
    String path = "/responses/create-index.json";

    // When
    String index = ResponseParser.parseCreatedIndexResponse(getEntityFromResponse(path));

    // Should
    assertEquals("idxtest", index);
  }

  @Test
  public void parseIndexesTest() {

    // State
    String path = "/responses/alias-indexes.json";

    // When
    Set<String> indexes = ResponseParser.parseIndexesInAliasResponse(getEntityFromResponse(path));

    // Should
    assertEquals(2, indexes.size());
    assertTrue(indexes.contains("idx1"));
    assertTrue(indexes.contains("idx2"));
  }

  private HttpEntity getEntityFromResponse(String path) {
    String json = JsonHandler.writeToString(getClass().getResourceAsStream(path));
    try {
      return new NStringEntity(json);
    } catch (UnsupportedEncodingException ex) {
      throw new IllegalStateException(ex.getMessage(), ex);
    }
  }
}
