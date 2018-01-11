package org.gbif.dwca;

/**
 * Categorical classification of types of records supported in Darwin Core Archive
 *
 * @author clf358
 * @see <a href=
 * "http://tdwg.github.io/dwc/terms/index.htm">http://tdwg.github.io/dwc/terms/index.htm</a>
 */
public enum Category {

  Occurrence("http://rs.tdwg.org/dwc/terms/Occurrence"), Organism("http://rs.tdwg.org/dwc/terms/Organism"), MaterialSample(
    "http://rs.tdwg.org/dwc/terms/MaterialSample"), LivingSpecimen("http://rs.tdwg.org/dwc/terms/LivingSpecimen"), PreservedSpecimen(
    "http://rs.tdwg.org/dwc/terms/PreservedSpecimen"), FossilSpecimen("http://rs.tdwg.org/dwc/terms/FossilSpecimen"), Event(
    "http://rs.tdwg.org/dwc/terms/Event"), HumanObservation("http://rs.tdwg.org/dwc/terms/HumanObservation"), MachineObservation(
    "http://rs.tdwg.org/dwc/terms/MachineObservation"), Location("http://purl.org/dc/terms/Location"), GeologicalContext(
    "http://rs.tdwg.org/dwc/terms/GeologicalContext"), Identification("http://rs.tdwg.org/dwc/terms/Identification"), Taxon(
    "http://rs.tdwg.org/dwc/terms/Taxon"), MeasurementOrFact("http://rs.tdwg.org/dwc/terms/MeasurementOrFact"), ResourceRelationship(
    "http://rs.tdwg.org/dwc/terms/ResourceRelationship"), // New custom made category which will contain composition of all fields except Auxiliary category
  // fields
  ExtendedOccurence("ExtendedOccurrence");

  private final String categoryIdentifier;

  Category(String categoryIdentifier) {
    this.categoryIdentifier = categoryIdentifier;
  }

  public String getCategoryIdentifier() {
    return categoryIdentifier;
  }

}
