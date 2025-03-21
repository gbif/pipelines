package org.gbif.pipelines.core.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class SortUtils {

  public static long yearDescMonthAscGbifIdAscSortKey(Integer year, Integer month, long gbifId) {
    // invert the year for descending order
    long invertedYear =
        year != null
            ? 9999 - year
            : 8191; // 8191 is the highest positive number in 14bits so nulls go last
    long monthAsLong = month != null ? month : 13;
    // Combine the values into a single long keeping the weight of the fields
    return (invertedYear << 50) | (monthAsLong << 46) | gbifId;
  }
}
