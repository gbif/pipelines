package org.gbif.pipelines.fragmenter.common;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.gbif.pipelines.core.utils.HashConverter;

@Getter
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class RawRecord {

  private String key;
  private String recordBody;
  private String hashValue;

  @Setter private Long createdDate;

  private RawRecord(String key, String recordBody) {
    this.key = key;
    this.recordBody = recordBody;
    this.hashValue = HashConverter.getSha1(recordBody);
  }

  public static RawRecord create(String key, String recordBody) {
    return new RawRecord(key, recordBody);
  }
}
