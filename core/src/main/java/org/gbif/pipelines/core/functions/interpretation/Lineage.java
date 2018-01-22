package org.gbif.pipelines.core.functions.interpretation;

public enum Lineage {
  InterpretedAsNull("Cannot interpret an empty or null data"),
  UnIdentifiedDayInterpretedAsNull("The day should be an integer, between 1 to 31"),
  UnIdentifiedMonthInterpretedAsNull("The month should be an integer, between 1 to 12"),
  UnIdentifiedYearInterpretedAsNull("The year should be an integer, between 1 to 12");


  private String description = "";
  Lineage(String generaldescripition){
    this.description = generaldescripition;
  }

  public String getDescription() {
    return description;
  }
}
