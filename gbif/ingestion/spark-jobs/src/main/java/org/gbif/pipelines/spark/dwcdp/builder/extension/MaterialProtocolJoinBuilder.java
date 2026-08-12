package org.gbif.pipelines.spark.dwcdp.builder.extension;

import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Enriches occurrences with protocol descriptions linked through their single material evidence.
 *
 * <p><b>Joins:</b>
 *
 * <ul>
 *   <li>material-protocol (via {@link MaterialJoinBuilder#singleMaterialOccurrenceLinks}) =
 *       protocol.protocol_pk (inner, junction) → merged into occurrence's samplingProtocol
 * </ul>
 *
 * <p>See mapping doc §4.2.
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

  /** No "dangling FK" bucket, unlike {@link ProtocolJoinBuilder#computeFunnel} — a material either resolves or doesn't. */
  public static Optional<JoinFunnel> computeFunnel(TableLoader loader) {
    Optional<Dataset<Row>> materialLinksOpt =
        MaterialJoinBuilder.singleMaterialOccurrenceLinks(loader);
    if (materialLinksOpt.isEmpty()) {
      return Optional.empty();
    }
    Dataset<Row> materialLinks = materialLinksOpt.get();

    String label = "MaterialProtocolJoinBuilder (occurrence samplingProtocol via single material)";
    long base = materialLinks.count();
    if (base == 0L) {
      return Optional.of(
          new JoinFunnel(
              label,
              List.of(new JoinFunnel.Bucket("unambiguous single-material links (base)", 0L))));
    }

    Optional<Dataset<Row>> protocolsOpt =
        ProtocolJoinBuilder.aggregateJunctionProtocolDescriptions(
            loader,
            TABLE_MATERIAL_PROTOCOL,
            "materialEntity_fk",
            MaterialJoinBuilder.TABLE_MATERIAL,
            "materialEntity_pk",
            "materialEntity_pk");
    if (protocolsOpt.isEmpty()) {
      return Optional.of(
          new JoinFunnel(
              label,
              List.of(
                  new JoinFunnel.Bucket("unambiguous single-material links (base)", base),
                  new JoinFunnel.Bucket("no material-protocol data available, unresolved", base))));
    }

    long resolved =
        protocolsOpt
            .get()
            .join(
                materialLinks,
                protocolsOpt
                    .get()
                    .col("materialEntity_pk")
                    .equalTo(materialLinks.col("materialEntity_pk")),
                "left_semi")
            .count();
    long unresolved = base - resolved;

    return Optional.of(
        new JoinFunnel(
            label,
            List.of(
                new JoinFunnel.Bucket("unambiguous single-material links (base)", base),
                new JoinFunnel.Bucket("resolved, samplingProtocol merged", resolved),
                new JoinFunnel.Bucket(
                    "no material-protocol data for this material, unresolved", unresolved))));
  }
}
