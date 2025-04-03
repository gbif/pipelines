package org.gbif.pipelines.core.utils;

import static org.gbif.pipelines.core.utils.SortUtils.*;
import static org.junit.Assert.*;

import org.junit.Test;

public class SortUtilsTest {

  @Test
  public void yearDescMonthAscGbifIdAscSortKeyTest() {
    assertFirstValueLowerThanSecond(2025, 1, 1146411502, 2025, 4, 1146411521);
    assertFirstValueLowerThanSecond(2025, 1, 1146411502, 1900, 1, 1146411521);
    assertFirstValueLowerThanSecond(4000, 1, 1146411502, 1900, 1, 1146411521);
    assertFirstValueLowerThanSecond(2025, 1, 1146411502, 2024, 12, 1146411502);
    assertFirstValueLowerThanSecond(2025, 1, 1146411502, 2025, 1, 1146411503);
    assertFirstValueLowerThanSecond(2025, 1, 1146411502, 2025, 1, 9999999999L);
    assertFirstValueLowerThanSecond(2025, 1, 99999999999L, 2025, 2, 1);
    assertFirstValueLowerThanSecond(2025, 1, 9999999999L, 2025, 2, 9999999999L);
    assertFirstValueLowerThanSecond(2025, 12, 1146411502, null, 4, 1146411521);
    assertFirstValueLowerThanSecond(2025, 12, 1146411502, null, 12, 1146411521);
    assertFirstValueLowerThanSecond(2025, 12, 1146411502, null, null, 1146411521);

    assertEquals(
        yearDescMonthAscGbifIdAscSortKey(2025, 1, 1146411502),
        yearDescMonthAscGbifIdAscSortKey(2025, 1, 1146411502));
  }

  private static void assertFirstValueLowerThanSecond(
      Integer year1, Integer month1, long gbifId1, Integer year2, Integer month2, long gbifId2) {
    assertTrue(
        yearDescMonthAscGbifIdAscSortKey(year1, month1, gbifId1)
            < yearDescMonthAscGbifIdAscSortKey(year2, month2, gbifId2));
  }
}
