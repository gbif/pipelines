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
    assertFirstValueLowerThanSecond(2025, 12, 1146411502, 1800, null, 1146411521);
    assertFirstValueLowerThanSecond(2030, 12, 1146411502, 2020, null, 1146411521);

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

  @Test
  public void yearDescMonthAscEventIDAscSortKeyTest() {
    assertFirstValueLowerThanSecondWithString(2025, 1, "S189842677", 2025, 4, "S189842699");
    assertFirstValueLowerThanSecondWithString(2025, 1, "S189842677", 1900, 1, "S189842699");
    assertFirstValueLowerThanSecondWithString(4000, 1, "S189842677", 1900, 1, "S189842699");
    assertFirstValueLowerThanSecondWithString(2025, 1, "S189842677", 2024, 12, "S189842677");
    assertFirstValueLowerThanSecondWithString(2025, 1, "S189842677", 2025, 1, "S189842678");
    assertFirstValueLowerThanSecondWithString(2025, 1, "S189842677", 2025, 1, "ZZZZZ");
    assertFirstValueLowerThanSecondWithString(2025, 1, "ZZZZZ", 2025, 2, "a");
    assertFirstValueLowerThanSecondWithString(2025, 1, "ZZZZZ", 2025, 2, "ZZZZZ");
    assertFirstValueLowerThanSecondWithString(2025, 12, "S189842677", null, 4, "S189842699");
    assertFirstValueLowerThanSecondWithString(2025, 12, "S189842677", null, 12, "S189842699");
    assertFirstValueLowerThanSecondWithString(2025, 12, "S189842677", null, null, "S189842699");
    assertFirstValueLowerThanSecondWithString(2025, 12, "S189842677", 1800, null, "S189842699");
    assertFirstValueLowerThanSecondWithString(2030, 12, "S189842677", 2020, null, "S189842699");
    assertFirstValueLowerThanSecondWithString(2025, 12, "S189842677", 2025, 12, "T");

    assertEquals(
        yearDescMonthAscEventIDAscSortKey(2025, 1, "S189842677"),
        yearDescMonthAscEventIDAscSortKey(2025, 1, "S189842677"));
  }

  private static void assertFirstValueLowerThanSecondWithString(
      Integer year1,
      Integer month1,
      String eventId1,
      Integer year2,
      Integer month2,
      String eventId2) {
    assertTrue(
        yearDescMonthAscEventIDAscSortKey(year1, month1, eventId1)
                .compareTo(yearDescMonthAscEventIDAscSortKey(year2, month2, eventId2))
            < 0);
  }
}
