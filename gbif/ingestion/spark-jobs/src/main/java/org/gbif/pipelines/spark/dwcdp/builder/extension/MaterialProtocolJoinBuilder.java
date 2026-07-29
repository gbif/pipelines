package org.gbif.pipelines.spark.dwcdp.builder.extension;

import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Enriches occurrences with protocol descriptions linked through their single material evidence.
 *
 * <p>A material protocol has no direct DwC-A home. When exactly one material record identifies an
 * occurrence as evidence, its protocol descriptions are merged into that occurrence's {@code
 * samplingProtocol}, using the same unambiguous-material rule as the other material joins.
 */
@Slf4j
public class MaterialProtocolJoinBuilder {

  static final String TABLE_MATERIAL_PROTOCOL = "material-protocol";

  private MaterialProtocolJoinBuilder() {}

  public static Dataset<Row> enrichOccurrences(TableLoader loader, Dataset<Row> occurrenceDf) {
    Optional<Dataset<Row>> materialLinksOpt =
        MaterialJoinBuilder.singleMaterialOccurrenceLinks(loader);
    if (materialLinksOpt.isEmpty()) {
      return occurrenceDf;
    }

    Optional<Dataset<Row>> protocols =
        ProtocolJoinBuilder.aggregateJunctionProtocolDescriptions(
            loader,
            TABLE_MATERIAL_PROTOCOL,
            "materialEntity_fk",
            MaterialJoinBuilder.TABLE_MATERIAL,
            "materialEntity_pk",
            "materialEntity_pk");
    if (protocols.isEmpty()) {
      return occurrenceDf;
    }

    Dataset<Row> byOccurrence =
        protocols
            .get()
            .join(
                materialLinksOpt.get(),
                protocols
                    .get()
                    .col("materialEntity_pk")
                    .equalTo(materialLinksOpt.get().col("materialEntity_pk")),
                "inner")
            .drop("materialEntity_pk");

    return ProtocolJoinBuilder.mergeJunctionProtocolsInto(
        occurrenceDf,
        Optional.of(byOccurrence),
        "occurrenceID",
        "occurrenceID",
        "samplingProtocol");
  }
}
