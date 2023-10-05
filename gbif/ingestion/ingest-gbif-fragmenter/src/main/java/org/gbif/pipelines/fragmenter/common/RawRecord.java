package org.gbif.pipelines.fragmenter.common;

import lombok.Getter;
import lombok.Setter;
import org.gbif.pipelines.core.utils.HashConverter;

@Getter
public class RawRecord {

  private final String key;
  private final String recordBody;
  private final String hashValue;

  @Setter private Long createdDate;

  public RawRecord(String key, String recordBody, String hashValue, Long createdDate) {
    this.key = key;
    this.recordBody = recordBody;
    this.hashValue = hashValue;
    this.createdDate = createdDate;
  }

  private RawRecord(String key, String recordBody) {
    this.key = key;
    this.recordBody = recordBody;
    this.hashValue = HashConverter.getSha1(recordBody);
  }

  public static RawRecord create(String key, String recordBody) {
    return new RawRecord(key, recordBody);
  }
}
