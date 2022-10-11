package org.gbif.converters.parser.xml.parsing.validators;

import com.google.common.base.Stopwatch;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests the {@link UniquenessValidator}. */
@Slf4j
public class UniquenessValidatorTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void givenUniqueIdsWhenMappedThenNoDuplicates() {
    // increase it to test with a higher volume of data
    int n = 1000;

    try (UniquenessValidator validator = UniquenessValidator.getNewInstance()) {
      for (int i = 0; i < n; i++) {
        Assert.assertTrue(validator.isUnique(UUID.randomUUID().toString()));
      }
    }
  }

  @Test
  public void givenDuplicatedIdsWhenMappedThenDuplicateFound() {
    // increase it to test with a higher volume of data
    int n = 10;

    try (UniquenessValidator validator = UniquenessValidator.getNewInstance()) {
      // add duplicates
      List<String> duplicates = Arrays.asList("12.1", "1234.1", "111.1", "12.12");

      long duplicatesFound = duplicates.stream().filter(id -> !validator.isUnique(id)).count();

      Assert.assertEquals(0, duplicatesFound);

      // add more values to test with high volume of data
      Stopwatch watch = Stopwatch.createStarted();
      for (int i = 0; i < n; i++) {
        if (!validator.isUnique(UUID.randomUUID().toString())) {
          duplicatesFound++;
        }
      }
      log.info("time: {}", watch.stop().elapsed(TimeUnit.MILLISECONDS));

      Assert.assertEquals(0, duplicatesFound);

      // add more duplicates
      duplicates =
          Arrays.asList(
              "12.1", "1234.1", "11.1", "12.1", "1234.1", "11.1", "12.12", "12.1", "1234.1",
              "11.1");

      duplicatesFound = duplicates.stream().filter(id -> !validator.isUnique(id)).count();

      Assert.assertEquals(9, duplicatesFound);
    }
  }

  @Test
  public void givenNullIdWhenMappedThenExceptionThrown() {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("ID is required");

    try (UniquenessValidator validator = UniquenessValidator.getNewInstance()) {
      validator.isUnique(null);
    }
  }
}
