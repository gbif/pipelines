package org.gbif.pipelines.spark.dwcdp;

import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;

/** Qualified row type URIs used by DwC-DP conversion. */
public final class DwcDpRowTypes {

  public static final String CORE_ROW_TYPE_EVENT = DwcTerm.Event.qualifiedName();
  public static final String CORE_ROW_TYPE_OCCURRENCE = DwcTerm.Occurrence.qualifiedName();
  public static final String ROW_TYPE_OCCURRENCE = DwcTerm.Occurrence.qualifiedName();
  public static final String ROW_TYPE_MULTIMEDIA = Extension.MULTIMEDIA.getRowType();

  private DwcDpRowTypes() {}
}
