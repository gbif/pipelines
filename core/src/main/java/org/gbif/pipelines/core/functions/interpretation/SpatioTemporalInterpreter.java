package org.gbif.pipelines.core.functions.interpretation;

import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Instant;

public class SpatioTemporalInterpreter {

  public static class DayInterpreter extends InterpretableDataFunction<String,Integer>{
      private static final int MIN_DAY=1;
      private static final int MAX_DAY=31;
      private static final int MAX_DAY_LENGTH=2;

    /**
     * validate the input data for any issues and adds the issues in issueList.
     *
     * @return if validation is passed true else fail.
     */
    @Override
    protected boolean validate(ProcessContext context) {
      rawData = context.element().toString();
      try {
        interpretedData = Integer.parseInt(rawData);
      }
      catch(NumberFormatException ex){

        if(rawData.isEmpty()) {
          getIssueList().add(Issue.EmptyField);
          getLineage().add(Lineage.InterpretedAsNull);
          interpretedData = null;
          return false;
        }
        else if(rawData==null) {
          getIssueList().add(Issue.NoData);
          getLineage().add(Lineage.InterpretedAsNull);
          interpretedData = null;
          return false;
        }
        else if(rawData.length()>MAX_DAY_LENGTH) {
          getIssueList().add(Issue.DayTooLong);
          getLineage().add(Lineage.UnIdentifiedDayInterpretedAsNull);
          interpretedData = null;
          return false;
        }
        else {
          getIssueList().add(Issue.UnIdentifiedInteger);
          getLineage().add(Lineage.UnIdentifiedDayInterpretedAsNull);
          interpretedData = null;
          return false;
        }
      }
      if((interpretedData < MIN_DAY) || (interpretedData > MAX_DAY)){
        getIssueList().add(Issue.DayOutOfRange);
        getLineage().add(Lineage.UnIdentifiedDayInterpretedAsNull);
        interpretedData = null;
        return false;
      }

      return true;
    }

    /**
     * apply the interpreted value and set lineage in case of changes
     */
    @Override
    protected void apply(ProcessContext context) {
      context.outputWithTimestamp(resultTag, interpretedData, Instant.now());

    }
  }

  public static class MonthInterpreter extends InterpretableDataFunction<String,Integer>{
    private static final int MIN_MONTH=1;
    private static final int MAX_MONTH=12;
    private static final int MAX_DAY_LENGTH=2;
    /**
     * validate the input data for any issues and adds the issues in issueList.
     *
     * @return if validation is passed true else fail.
     */
    @Override
    protected boolean validate(ProcessContext context) {
      rawData = context.element().toString();
      try {
        interpretedData = Integer.parseInt(rawData);
      }
      catch(NumberFormatException ex){

        if(rawData.isEmpty()) {
          getIssueList().add(Issue.EmptyField);
          getLineage().add(Lineage.InterpretedAsNull);
          interpretedData = null;
          return false;
        }
        else if(rawData==null) {
          getIssueList().add(Issue.NoData);
          getLineage().add(Lineage.InterpretedAsNull);
          interpretedData = null;
          return false;
        }
        else if(rawData.length()>MAX_DAY_LENGTH) {
          getIssueList().add(Issue.MonthTooLong);
          getLineage().add(Lineage.UnIdentifiedMonthInterpretedAsNull);
          interpretedData = null;
          return false;
        }
        else {
          getIssueList().add(Issue.UnIdentifiedInteger);
          getLineage().add(Lineage.UnIdentifiedMonthInterpretedAsNull);
          interpretedData = null;
          return false;
        }
      }
      if((interpretedData < MIN_MONTH) || (interpretedData > MAX_MONTH)){
        getIssueList().add(Issue.MonthOutOfRange);
        getLineage().add(Lineage.UnIdentifiedMonthInterpretedAsNull);
        interpretedData = null;
        return false;
      }

      return true;
    }

    /**
     * apply the interpreted value and set lineage in case of changes
     */
    @Override
    protected void apply(ProcessContext context) {
      context.outputWithTimestamp(resultTag, interpretedData, Instant.now());
    }
  }

  public static class YearInterpreter extends InterpretableDataFunction<String,Integer>{
    private static final int MIN_YEAR=1000;
    private static final int MAX_YEAR=9999;
    private static final int MAX_YEAR_LENGTH=4;
    /**
     * validate the input data for any issues and adds the issues in issueList.
     *
     * @return if validation is passed true else fail.
     */
    @Override
    protected boolean validate(ProcessContext context) {
      rawData = context.element().toString();
      try {
        interpretedData = Integer.parseInt(rawData);
      }
      catch(NumberFormatException ex){

        if(rawData.isEmpty()) {
          getIssueList().add(Issue.EmptyField);
          getLineage().add(Lineage.InterpretedAsNull);
          interpretedData = null;
          return false;
        }
        else if(rawData==null) {
          getIssueList().add(Issue.NoData);
          getLineage().add(Lineage.InterpretedAsNull);
          interpretedData = null;
          return false;
        }
        else if(rawData.length()>MAX_YEAR_LENGTH) {
          getIssueList().add(Issue.YearTooLong);
          getLineage().add(Lineage.UnIdentifiedYearInterpretedAsNull);
          interpretedData = null;
          return false;
        }
        else {
          getIssueList().add(Issue.UnIdentifiedInteger);
          getLineage().add(Lineage.UnIdentifiedYearInterpretedAsNull);
          interpretedData = null;
          return false;
        }
      }
      if((interpretedData < MIN_YEAR) || (interpretedData > MAX_YEAR)){
        getIssueList().add(Issue.YearOutOfRange);
        getLineage().add(Lineage.UnIdentifiedYearInterpretedAsNull);
        interpretedData = null;
        return false;
      }

      return true;
    }

    /**
     * apply the interpreted value and set lineage in case of changes
     */
    @Override
    protected void apply(ProcessContext context) {
      context.outputWithTimestamp(resultTag, interpretedData, Instant.now());
    }
  }


}
