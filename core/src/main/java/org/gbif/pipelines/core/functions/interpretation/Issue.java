package org.gbif.pipelines.core.functions.interpretation;

public enum Issue {
  EmptyField("Field is empty"),
  NoData("Field is null"),
  DayTooLong("Day field provided is too long, it should not be more than 2"),
  UnIdentifiedInteger("The data represented is not a valid integer"),
  DayOutOfRange("The data represented is not a valid day"),
  MonthTooLong("Month field provided is too long, it should not be more than 2"),
  MonthOutOfRange("The data represented is not a valid month"),
  YearTooLong("Month field provided is too long, it should not be more than 4"),
  YearOutOfRange("The data represented is not a valid year");


  private String description = "";
  Issue(String generaldescripition){
    this.description = generaldescripition;
  }

  public String getDescription() {
    return description;
  }

}
