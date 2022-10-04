package org.gbif.pipelines.estools.model;

import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DeleteByQueryTask implements Serializable {

  private boolean completed;
  private long recordsDeleted;
}
