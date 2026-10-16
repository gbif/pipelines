package org.gbif.pipelines.spark.pojo;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** One row of the HBase occurrence lookup table (triplet/occurrenceID -> gbifID). */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LookupRow implements Serializable {

  public static final String TRIPLET = "TRIPLET";
  public static final String OCCURRENCE_ID = "OCCURRENCE_ID";

  private String datasetKey;
  /** Salt bucket of the row key. */
  private Integer salt;
  /** Row key in Bytes.toStringBinary form, as printed by the HBase shell. */
  private String rowKey;
  /** Exact row key bytes, e.g. for deletes. */
  private byte[] rowKeyBytes;
  /** TRIPLET (ic|cc|cn|null) or OCCURRENCE_ID. */
  private String keyType;
  /** The triplet or occurrenceID. */
  private String uniqueKey;
  /** Value of o:i, null if missing or not an 8 byte long. */
  private Long gbifId;
  /** Timestamp (epoch ms) of o:i. */
  private Long gbifIdTs;
  /** Value of o:s (ALLOCATED/ALLOCATING). */
  private String status;
  /** Timestamp (epoch ms) of o:s. */
  private Long statusTs;
  /** Value of o:l, only present while a key is being allocated or for an abandoned lock. */
  private String lock;
  /** Timestamp (epoch ms) of o:l. */
  private Long lockTs;
}
