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

  private static final String CREATE_INDEX_RESPONSE_PATH = "/responses/create-index.json";
  private static final String ALIAS_INDEXES_RESPONSE_PATH = "/responses/alias-indexes.json";

  @Test
  public void parseCreatedIndexResponseTest() {
    String index =
        ResponseParser.parseCreatedIndexResponse(getEntityFromResponse(CREATE_INDEX_RESPONSE_PATH));
    assertEquals("idxtest", index);
  }

  @Test
  public void parseIndexesTest() {
    Set<String> indexes =
        ResponseParser.parseIndexesInAliasResponse(
            getEntityFromResponse(ALIAS_INDEXES_RESPONSE_PATH));

    assertEquals(2, indexes.size());
    assertTrue(indexes.contains("idx1"));
    assertTrue(indexes.contains("idx2"));
  }

  private HttpEntity getEntityFromResponse(String path) {
    String json = JsonHandler.writeToString(getClass().getResourceAsStream(path));
    try {
      return new NStringEntity(json);
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException(e.getMessage(), e);
    }
  }
}
