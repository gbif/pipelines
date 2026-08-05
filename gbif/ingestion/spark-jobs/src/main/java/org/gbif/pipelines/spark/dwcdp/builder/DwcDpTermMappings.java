package org.gbif.pipelines.spark.dwcdp.builder;

import java.util.Map;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifDnaTerm;
import org.gbif.dwc.terms.MixsTerm;

/**
 * Explicit field name renames from DwC-DP to DwC-A.
 *
 * <p>DwC-DP occasionally uses field names that differ from the DwC-A term names they correspond to.
 * These differences are intentional DwC-DP design decisions, not synonyms registered in the DwC
 * standard — so {@link org.gbif.dwc.terms.TermFactory} cannot bridge them automatically. This map
 * provides the explicit mapping.
 *
 * <p>{@link TermResolver} consults this map before calling {@code TermFactory}, so any field name
 * listed here is resolved to the correct qualified URI regardless of what the library knows —
 * including cases where {@code TermFactory} *does* resolve the name, just to the wrong term (see
 * {@code accessURI} below): this map is checked first and short-circuits, so it always wins.
 *
 * <h2>What belongs here</h2>
 *
 * Only confirmed renames where:
 *
 * <ol>
 *   <li>The DwC-DP field name differs from the target DwC-A term's simple name, AND
 *   <li>{@code TermFactory} does not already correctly resolve the DwC-DP name to the intended
 *       target term (either because it doesn't recognize the name at all, or — as with {@code
 *       accessURI} — because it resolves the name to a different, real term that isn't the one
 *       intended for this mapping).
 * </ol>
 *
 * <h2>What does NOT belong here</h2>
 *
 * <ul>
 *   <li>Terms absent from the current {@code dwc-api} library version (e.g. {@code causeOfDeath},
 *       {@code substrate}, {@code feedbackURL}) — wait for the library to be upgraded; {@code
 *       TermFactory} will then resolve them automatically.
 *   <li>Terms that {@code TermFactory} already resolves correctly via registered alternatives.
 * </ul>
 *
 * <h2>Adding new entries</h2>
 *
 * When a new DwC-DP rename is confirmed, add it here with a comment citing the source (ingestion
 * guide section, DwC-DP schema reference, or GBIF pipeline decision). If an entry becomes redundant
 * because {@code dwc-api} adds the name as a registered alternative, remove it and note the library
 * version in which it was fixed.
 */
public final class DwcDpTermMappings {

  /**
   * DwC-DP field name → qualified DwC-A term URI.
   *
   * <p>Keyed by the exact column name as it appears in the DwC-DP Parquet file.
   */
  public static final Map<String, String> RENAMES =
      Map.ofEntries(
          // DwC-DP renamed dwc:associatedReferences to occurrenceReferences on the occurrence
          // table. Source: ingestion guide section 2, DwC-DP occurrence schema.
          Map.entry("occurrenceReferences", DwcTerm.associatedReferences.qualifiedName()),

          // DwC-DP uses eventConductedBy / eventConductedByID on the event table where DwC-A
          // uses recordedBy / recordedByID. Source: ingestion guide section 1.
          Map.entry("eventConductedBy", DwcTerm.recordedBy.qualifiedName()),
          Map.entry("eventConductedByID", DwcTerm.recordedByID.qualifiedName()),

          // media.accessURI is DwC-DP/Audubon Core's name for the media resource's location.
          // The Simple Multimedia extension (the confirmed DwC-DP→DwC-A media target — see
          // project mapping notes) expects this under dc:identifier, which is what
          // MultimediaInterpreter actually reads. Without this rename, TermFactory resolves
          // "accessURI" to ac:accessURI (Audubon Core's own term) instead — a real term, just
          // the wrong one for this target, so this isn't a case TermFactory would ever fix on
          // its own even with a library upgrade.
          Map.entry("accessURI", DcTerm.identifier.qualifiedName()),

          // media.mediaType is DwC-DP's name for the media's high-level kind (StillImage,
          // MovingImage, Sound, ...). MultimediaInterpreter reads this via dc:type
          // (MultimediaInterpreter.parseAndSetType). "mediaType" doesn't match dc:type's simple
          // name, so TermFactory would never resolve it — it would fall through to the raw
          // column name and be invisible to interpretation.
          Map.entry("mediaType", DcTerm.type.qualifiedName()),

          // nucleotide-sequence.sequence is the raw DNA/RNA sequence string. TermFactory *does*
          // resolve "sequence" — but to ggbn:sequence (org.gbif.dwc.terms.GgbnTerm.sequence is a
          // real, registered term with that exact simple name), not the term the DNA Derived
          // Data extension interpreter actually reads
          // (org.gbif.pipelines.core.interpreters.extension.DnaDerivedDataInterpreter maps
          // GbifDnaTerm.dna_sequence). Same "resolves to a real term, just the wrong one" shape as
          // accessURI above — confirmed via dwc-api javadoc: GbifDnaTerm has no "sequence"
          // constant (only dna_sequence, pcr_primer_forward/reverse/name_forward/name_reverse/
          // reference), so this rename is needed regardless of any future GgbnTerm changes.
          Map.entry("sequence", GbifDnaTerm.dna_sequence.qualifiedName()),

          // molecular-protocol.single_cell_lysis_appr / single_cell_lysis_prot are DwC-DP's names
          // for the MIxS single-cell lysis fields — but org.gbif.dwc.terms.MixsTerm registers
          // these under the abbreviated MIxS field IDs sc_lysis_approach / sc_lysis_method
          // instead (confirmed via dwc-api javadoc: MixsTerm has no
          // single_cell_lysis_appr/single_cell_lysis_prot constants at all), so TermFactory would
          // never resolve the DwC-DP names on its own — every other molecular-protocol column
          // observed matches its MixsTerm/GbifDnaTerm simple name exactly and needs no entry
          // here; these two are the only confirmed exceptions.
          Map.entry("single_cell_lysis_appr", MixsTerm.sc_lysis_approach.qualifiedName()),
          Map.entry("single_cell_lysis_prot", MixsTerm.sc_lysis_method.qualifiedName()));

  private DwcDpTermMappings() {}
}
