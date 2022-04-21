package org.gbif.pipelines.core.pojo;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GbifIdRecord;

/** This container helps with Beam serialization */
@Getter
@AllArgsConstructor(staticName = "create")
public class ErIdContainer implements Serializable {

  private static final long serialVersionUID = 2953355237274578443L;

  private final ExtendedRecord er;
  private final GbifIdRecord id;
}
