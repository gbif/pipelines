package org.gbif.pipelines.spark.util;

import java.time.Duration;
import java.time.Instant;

public class LogUtil {
  public static String timeAndRecPerSecond(String jobName, long start, long recordsNumber) {
    long end = System.currentTimeMillis();
    Duration d = Duration.between(Instant.ofEpochMilli(start), Instant.ofEpochMilli(end));

    long hours = d.toHours();
    long minutes = d.toMinutesPart();
    long seconds = d.toSecondsPart();
    long millis = d.toMillis() % 1000;

    double secs = Math.max(d.toMillis() / 1000.0, 0.000001);
    double recPerSec = recordsNumber / secs;

    return String.format(
        "Finished %s in %02dh %02dm %02ds %03dms. Rec/s: %6.2f, Total records: %,d",
        jobName, hours, minutes, seconds, millis, recPerSec, recordsNumber);
  }
}
