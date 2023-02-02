package org.gbif.pipelines.keygen.hbase;

import static com.google.common.base.Preconditions.checkNotNull;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.hadoop.hbase.util.Bytes;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.UnknownTerm;
import org.gbif.pipelines.keygen.common.TermUtils;

/**
 * Utility class to deal with occurrence hbase columns. Primarily translate from Terms to their
 * corresponding HBase column name (in the occurrence table), but also deals with any other names
 * used, e.g. identifiers, issue columns, etc.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Columns {

  // the one column family for all columns of the occurrence table
  public static final String OCCURRENCE_COLUMN_FAMILY = "o";
  public static final byte[] CF = Bytes.toBytes(OCCURRENCE_COLUMN_FAMILY);

  // the counter table is a single cell that is the "autoincrement" number for new keys, with column
  // family, column,
  // and key ("row" in hbase speak)
  public static final String COUNTER_COLUMN = "id";

  // the lookup table is a secondary index of unique ids (holy triplet or publisher-provided) to
  // GBIF integer keys
  public static final String LOOKUP_KEY_COLUMN = "i";
  public static final String LOOKUP_LOCK_COLUMN = "l";
  public static final String LOOKUP_STATUS_COLUMN = "s";

  // each UnknownTerm is prefixed differently
  private static final String VERBATIM_TERM_PREFIX = "v_";

  /**
   * Returns the column for the given term. If an interpreted column exists for the given term it
   * will be returned, otherwise the verbatim column will be used. Not that GbifInternalTerm are
   * always interpreted and do not exist as verbatim columns. Asking for a "secondary" interpreted
   * term like country which is used during interpretation but not stored will result in an
   * IllegalArgumentException. dwc:countryCode is the right term in this case. Key terms like
   * taxonID or occurrenceID are considered verbatim terms and do not map to the respective GBIF
   * columns. Please use the GbifTerm enum for those!
   */
  public static String column(Term term) {
    if (term instanceof GbifInternalTerm
        || TermUtils.isOccurrenceJavaProperty(term)
        || GbifTerm.mediaType == term) {
      return column(term, "");

    } else if (TermUtils.isInterpretedSourceTerm(term)) {
      // "secondary" terms used in interpretation but not used to store the interpreted values
      // should never be asked for
      throw new IllegalArgumentException(
          "The term " + term + " is interpreted and only relevant for verbatim values");

    } else {
      return verbatimColumn(term);
    }
  }

  /**
   * Returns the verbatim column for a term. GbifInternalTerm is not permitted and will result in an
   * IllegalArgumentException!
   */
  public static String verbatimColumn(Term term) {
    if (term instanceof GbifInternalTerm) {
      throw new IllegalArgumentException(
          "Internal terms (like the tried ["
              + term.simpleName()
              + "]) do not exist as verbatim columns");
    }
    return column(term, VERBATIM_TERM_PREFIX);
  }

  private static String column(Term term, String colPrefix) {
    checkNotNull(term, "term can't be null");

    // unknown terms will never be mapped in Hive, and we can't replace : with anything and
    // guarantee that it will
    // be reversible
    if (term instanceof UnknownTerm) {
      return colPrefix + term.qualifiedName();
    }

    // known terms are mapped to their unique simple name with an optional (v_) prefix
    return colPrefix + term.simpleName();
  }
}
