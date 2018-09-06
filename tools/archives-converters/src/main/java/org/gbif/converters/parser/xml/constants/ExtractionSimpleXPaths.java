package org.gbif.converters.parser.xml.constants;

/**
 * These aren't all real XPaths but save setting up the full parse machinery in order to run real
 * XPaths when quick String searches for the goal strings are much faster.
 */
public class ExtractionSimpleXPaths {

  private ExtractionSimpleXPaths() {
    // NOP
  }

  public static final String ABCD_RECORD_XPATH = "*/Unit";
  public static final String ABCD_HEADER_XPATH = "*/OriginalSource";
  public static final String DWC_1_0_RECORD_XPATH = "*/record/";
  public static final String DWC_MANIS_RECORD_XPATH = "*/record/";
  public static final String DWC_1_4_RECORD_XPATH = "*/DarwinRecord";
  public static final String DWC_2009_RECORD_XPATH = "*/SimpleDarwinRecord";

  public static final String DWC_1_0_RECORD = "<record";
  public static final String DWC_1_0_INSTITUTION = "<InstitutionCode";
  public static final String DWC_1_0_COLLECTION = "<CollectionCode";
  public static final String DWC_1_0_CATALOG = "<CatalogNumber";

  public static final String DWC_1_4_RECORD = "<DarwinRecord";
  public static final String DWC_1_4_INSTITUTION = "<InstitutionCode";
  public static final String DWC_1_4_COLLECTION = "<CollectionCode";
  public static final String DWC_1_4_CATALOG = "<CatalogNumber";

  public static final String DWC_MANIS_RECORD = "<record";
  public static final String DWC_MANIS_INSTITUTION = "<InstitutionCode";
  public static final String DWC_MANIS_COLLECTION = "<CollectionCode";
  public static final String DWC_MANIS_CATALOG = "<CatalogNumberText";

  public static final String DWC_2009_RECORD = "<SimpleDarwinRecord";
  public static final String DWC_2009_INSTITUTION = "<institutionCode";
  public static final String DWC_2009_COLLECTION = "<collectionCode";
  public static final String DWC_2009_CATALOG = "<catalogNumber";

  public static final String ABCD_1_2_RECORD = "<Unit";
  public static final String ABCD_1_2_INSTITUTION = "<SourceInstitutionCode";
  public static final String ABCD_1_2_COLLECTION = "<SourceName";
  public static final String ABCD_1_2_CATALOG = "<UnitID";

  public static final String ABCD_2_0_6_RECORD = "<Unit";
  public static final String ABCD_2_0_6_INSTITUTION = "<SourceInstitutionID";
  public static final String ABCD_2_0_6_COLLECTION = "<SourceID";
  public static final String ABCD_2_0_6_CATALOG = "<UnitID";
}
