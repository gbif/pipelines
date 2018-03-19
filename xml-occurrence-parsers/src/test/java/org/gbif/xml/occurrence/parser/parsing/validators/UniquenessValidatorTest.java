package org.gbif.xml.occurrence.parser.parsing.validators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the {@link UniquenessValidator}.
 */
public class UniquenessValidatorTest {

  @Test
  public void givenUniqueIdsWhenMappedThenNoDuplicates() {
    int n = 1000;

    try (UniquenessValidator mapCache = UniquenessValidator.getNewInstance()) {
      for (int i = 0; i < n; i++) {
        Assert.assertTrue(mapCache.isUnique(UUID.randomUUID().toString()));
      }
    }
  }

  @Test
  public void givenDuplicatedIdsWhenMappedThenDuplicateFound() {
    int n = 10;
    int duplicatesFound = 0;

    try (UniquenessValidator mapCache = UniquenessValidator.getNewInstance()) {
      // add duplicates
      List<String> duplicates = new ArrayList<>();
      duplicates.addAll(Arrays.asList("12.1", "1234.1", "111.1", "12.12"));

      for (String duplicate : duplicates) {
        if (!mapCache.isUnique(duplicate)) {
          duplicatesFound++;
        }
      }

      Stopwatch watch = Stopwatch.createStarted();
      for (int i = 0; i < n; i++) {
        if (!mapCache.isUnique(UUID.randomUUID().toString())) {
          duplicatesFound++;
        }
      }
      System.out.println("time: " + watch.stop().elapsed(TimeUnit.MILLISECONDS));

      // add duplicates
      duplicates = new ArrayList<>();
      duplicates.addAll(Arrays.asList("12.1", "1234.1", "11.1"));
      duplicates.addAll(Arrays.asList("12.1", "1234.1", "11.1", "12.12"));
      duplicates.addAll(Arrays.asList("12.1", "1234.1", "11.1"));

      for (String duplicate : duplicates) {
        if (!mapCache.isUnique(duplicate)) {
          duplicatesFound++;
        }
      }

    }

    Assert.assertEquals(9, duplicatesFound);
  }

  @Test(expected = NullPointerException.class)
  public void givenNullIdWhenMappedThenExceptionThrown() {
    try (UniquenessValidator mapCache = UniquenessValidator.getNewInstance()) {
      mapCache.isUnique(null);
    }
  }

}
