package org.gbif.pipelines.core.pojo;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;

/** This container helps with Beam serialization */
@Getter
@AllArgsConstructor(staticName = "create")
public class ErIdrMdrContainer implements Serializable {

  private static final long serialVersionUID = 2953359865274578444L;

  private final ExtendedRecord er;
  private final IdentifierRecord idr;
  private final MetadataRecord mdr;
}
