package org.gbif.pipelines.spark.dwcdp.mapping;

/** How independently imported extension fragments compose their row sets. */
public enum ExtensionRowComposition {
  /** One fragment defines rows; remaining fragments enrich that same row set. */
  ENRICH,

  /** Every fragment defines an independent row set; rows are unioned into one extension. */
  UNION
}
