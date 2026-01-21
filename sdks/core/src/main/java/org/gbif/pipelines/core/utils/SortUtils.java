package org.gbif.pipelines.core.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class SortUtils {

  public static long yearDescMonthAscGbifIdAscSortKey(Integer year, Integer month, long gbifId) {
    // invert the year for descending order
    long invertedYear = year != null ? 9999 - year : 10000;
    long monthAsLong = month != null ? month : 13;
    // Combine the values into a single long keeping the weight of the fields
    return (invertedYear << 48) | (monthAsLong << 44) | gbifId;
  }

  public static String yearDescMonthAscEventIDAscSortKey(
      Integer year, Integer month, String eventId) {
    // Invert the year for descending order and pad to 4 digits
    long invertedYear = year != null ? 9999 - year : 10000;
    // Use 5 digits so that the special null value (10000) is lexicographically
    // greater than any valid inverted year (0-9999) when compared as strings.
    String yearPart = String.format("%05d", invertedYear);

    // Pad the month for ascending order
    long monthAsLong = month != null ? month : 13;
    String monthPart = String.format("%02d", monthAsLong);

    // The sortString is used as is for ascending alphabetical order
    String stringPart = eventId != null ? eventId : "";

    // Combine the values into a single String key
    return yearPart + monthPart + stringPart;
  }
}
