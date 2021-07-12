package org.gbif.validator.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Date;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Validation {

  public enum Status {
    DOWNLOADING,
    SUBMITTED,
    RUNNING,
    FINISHED,
    FAILED
  }

  private UUID key;
  private Date created;
  private Date modified;
  private String username;
  private String result;
  private String file;
  private Long fileSize;
  private FileFormat fileFormat;
  private Status status;
}
