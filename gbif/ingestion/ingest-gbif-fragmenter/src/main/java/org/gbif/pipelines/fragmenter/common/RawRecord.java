package org.gbif.pipelines.fragmenter.common;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.core.utils.HashConverter;

@Getter
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class RawRecord {

  private String key;
  private String record;
  private String hashValue;

  private RawRecord(String key, String record) {
    this.key = key;
    this.record = record;
    this.hashValue = HashConverter.getSha1(record);
  }

  public static RawRecord create(String key, String record) {
    return new RawRecord(key, record);
  }
}
