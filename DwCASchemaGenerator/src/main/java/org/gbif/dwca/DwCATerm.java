package org.gbif.dwca;

/**
 * This is a data structure to support terms in DwCA.
 *
 * @author clf358
 */
public class DwCATerm {

  private final String termName;
  private final String classOfTerm;
  private final String identifier;
  private final String definition;
  private final String comment;
  private final String details;
  private boolean isCategory;

  public DwCATerm(
    String termName, String classOfTerm, String identifier, String definition, String comment, String details
  ) {
    super();
    this.termName = termName;
    this.classOfTerm = classOfTerm;
    this.identifier = identifier;
    this.definition = definition;
    this.comment = comment;
    this.details = details;
    if (classOfTerm.trim().isEmpty()) isCategory = true;
  }

  public String getTermName() {
    return termName;
  }

  public String getClassOfTerm() {
    return classOfTerm;
  }

  public String getIdentifier() {
    return identifier;
  }

  public String getDefinition() {
    return definition;
  }

  public String getComment() {
    return comment;
  }

  public String getDetails() {
    return details;
  }

  public boolean isCategory() {
    return isCategory;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("DwCATerm [termName=")
      .append(termName)
      .append(", classOfTerm=")
      .append(classOfTerm)
      .append(", identifier=")
      .append(identifier)
      .append(", definition=")
      .append(definition)
      .append(", comment=")
      .append(comment)
      .append(", details=")
      .append(details)
      .append(", isCategory=")
      .append(isCategory)
      .append("]");
    return builder.toString();
  }

}
