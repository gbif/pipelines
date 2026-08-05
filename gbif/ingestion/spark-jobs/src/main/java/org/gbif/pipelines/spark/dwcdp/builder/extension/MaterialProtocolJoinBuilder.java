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

  /**
   * Computes a {@link JoinFunnel} breakdown of {@code samplingProtocol} enrichment via single
   * material evidence, mirroring {@link #enrichOccurrences}'s decision logic. Unlike {@link
   * ProtocolJoinBuilder#computeFunnel}, there's no "dangling FK" bucket here — {@link
   * ProtocolJoinBuilder#aggregateJunctionProtocolDescriptions} already only returns rows for
   * materials with at least one protocol description that actually resolved, so a material either
   * contributes a resolved value or it doesn't. Buckets are mutually exclusive and sum to the base
   * count:
   *
   * <ul>
   *   <li><b>base</b> — occurrences with exactly one material citing them as evidence (per {@link
   *       MaterialJoinBuilder#singleMaterialOccurrenceLinks}) — the population {@link
   *       #enrichOccurrences} can possibly enrich at all
   *   <li><b>resolved, samplingProtocol merged</b> — that material has {@code material-protocol}
   *       data
   *   <li><b>no material-protocol data for this material, unresolved</b> — that material has none
   * </ul>
   *
   * @return empty if there are no unambiguous single-material occurrence links at all (same no-op
   *     case {@link #enrichOccurrences} treats as absent)
   */
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
