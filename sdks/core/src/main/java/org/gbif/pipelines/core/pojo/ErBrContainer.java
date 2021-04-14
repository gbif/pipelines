package org.gbif.pipelines.core.pojo;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;

/** This container helps with Beam serialization */
@Getter
@AllArgsConstructor(staticName = "create")
public class ErBrContainer implements Serializable {

  private final ExtendedRecord er;
  private final BasicRecord br;
}
