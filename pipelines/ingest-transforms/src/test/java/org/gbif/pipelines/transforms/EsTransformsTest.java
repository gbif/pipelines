package org.gbif.pipelines.transforms;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class EsTransformsTest {

  @Test
  public void sha1FromTripletTest() {

    // State
    ObjectNode node = createObjectNode("2f38c815-1cab-4367", "ae39-9747b9e3d51a");

    String expected = "dcbd0fbaaaa56a8b46025d96f9567e4c2e97f451";

    // When
    String result = EsTransforms.getEsTripletIdFn().apply(node);

    // Should
    assertEquals(expected, result);
  }

  @Test
  public void identicalTripletTest() {

    // State
    ObjectNode node1 = createObjectNode("2f38c815-1cab-4367", "ae39-9747b9e3d51a");
    ObjectNode node2 = createObjectNode("2f38c815-1cab-4367", "ae39-9747b9e3d51a");

    // When
    String result1 = EsTransforms.getEsTripletIdFn().apply(node1);
    String result2 = EsTransforms.getEsTripletIdFn().apply(node2);

    // Should
    assertEquals(result1, result2);
  }

  private ObjectNode createObjectNode(String collectionCode, String catalogNumber) {
    ObjectMapper objectMapper = new ObjectMapper();

    ObjectNode mainNode = objectMapper.createObjectNode();
    mainNode.set("datasetKey", new TextNode(collectionCode));
    mainNode.set("occurrenceId", new TextNode(catalogNumber));

    return mainNode;
  }

}
