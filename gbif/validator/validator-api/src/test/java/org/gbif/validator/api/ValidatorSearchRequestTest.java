package org.gbif.validator.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/** Tests cases for ValidationRequest custom code added to its Builder sub-class. */
public class ValidatorSearchRequestTest {

  @Test
  public void sortByTest() {
    ValidationSearchRequest searchRequest =
        ValidationSearchRequest.builder()
            .sortByKey(ValidationSearchRequest.SortOrder.ASC)
            .sortByCreated(ValidationSearchRequest.SortOrder.DESC)
            .build();
    assertEquals(2, searchRequest.getSortBy().size());
    assertTrue(
        searchRequest.getSortBy().stream()
            .anyMatch(
                sort ->
                    sort.getField().equals("created")
                        && sort.getOrder() == ValidationSearchRequest.SortOrder.DESC));
    assertTrue(
        searchRequest.getSortBy().stream()
            .anyMatch(
                sort ->
                    sort.getField().equals("key")
                        && sort.getOrder() == ValidationSearchRequest.SortOrder.ASC));
  }
}
