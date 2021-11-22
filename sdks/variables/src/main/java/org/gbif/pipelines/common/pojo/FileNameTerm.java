package org.gbif.pipelines.common.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

@Data
@Getter
@AllArgsConstructor(staticName = "create")
public class FileNameTerm {

  private String fileName;
  private String termQualifiedName;
}
