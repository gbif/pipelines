package org.gbif.pipelines.interpretation.columns;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.interpretation.column.InterpretationFactory;
import org.gbif.pipelines.interpretation.column.InterpretationResult;

import org.junit.Assert;
import org.junit.Test;

public class BasicTemporalInterpretationTest {

  @Test
  public void testNullOnDay() {
    final InterpretationResult<Integer> interpret = InterpretationFactory.interpret(DwcTerm.day, null);
    System.out.println(interpret);
    Assert.assertEquals(0, interpret.getIssueList().size());
    Assert.assertEquals(0, interpret.getLineageList().size());
  }

  @Test
  public void testOutOfRangeDay() {
    final InterpretationResult<Integer> interpret = InterpretationFactory.interpret(DwcTerm.day, "-35");
    System.out.println(interpret);
    Assert.assertEquals(1, interpret.getIssueList().size());
    Assert.assertEquals(1, interpret.getLineageList().size());
  }

  @Test
  public void testOutOfRangeDay2() {
    final InterpretationResult<Integer> interpret = InterpretationFactory.interpret(DwcTerm.day, "32");
    System.out.println(interpret);
    Assert.assertEquals(1, interpret.getIssueList().size());
    Assert.assertEquals(1, interpret.getLineageList().size());
  }

  @Test
  public void testEmptyDay() {
    final InterpretationResult<Integer> interpret = InterpretationFactory.interpret(DwcTerm.day, "");
    System.out.println(interpret);
    Assert.assertEquals(1, interpret.getIssueList().size());
    Assert.assertEquals(1, interpret.getLineageList().size());

  }

  @Test
  public void testValidDay() {
    final InterpretationResult<Integer> interpret = InterpretationFactory.interpret(DwcTerm.day, "12");
    System.out.println(interpret);
    Assert.assertEquals(0, interpret.getIssueList().size());
    Assert.assertEquals(0, interpret.getLineageList().size());
  }

  //********************* MONTH **************************
  @Test
  public void testNullOnMonth() {
    final InterpretationResult<Integer> interpret = InterpretationFactory.interpret(DwcTerm.month, null);
    System.out.println(interpret);
    Assert.assertEquals(0, interpret.getIssueList().size());
    Assert.assertEquals(0, interpret.getLineageList().size());
  }

  @Test
  public void testOutOfRangeMonth() {
    final InterpretationResult<Integer> interpret = InterpretationFactory.interpret(DwcTerm.month, "-1");
    System.out.println(interpret);
    Assert.assertEquals(1, interpret.getIssueList().size());
    Assert.assertEquals(1, interpret.getLineageList().size());
  }

  @Test
  public void testOutOfRangeMonth2() {
    final InterpretationResult<Integer> interpret = InterpretationFactory.interpret(DwcTerm.month, "22");
    System.out.println(interpret);
    Assert.assertEquals(1, interpret.getIssueList().size());
    Assert.assertEquals(1, interpret.getLineageList().size());
  }

  @Test
  public void testEmptyMonth() {
    final InterpretationResult<Integer> interpret = InterpretationFactory.interpret(DwcTerm.month, "");
    System.out.println(interpret);
    Assert.assertEquals(1, interpret.getIssueList().size());
    Assert.assertEquals(1, interpret.getLineageList().size());

  }

  @Test
  public void testValidMonth() {
    final InterpretationResult<Integer> interpret = InterpretationFactory.interpret(DwcTerm.month, "12");
    System.out.println(interpret);
    Assert.assertEquals(0, interpret.getIssueList().size());
    Assert.assertEquals(0, interpret.getLineageList().size());
  }

  /***************************YEAR **************************/

  @Test
  public void testNullOnYear() {
    final InterpretationResult<Integer> interpret = InterpretationFactory.interpret(DwcTerm.year, null);
    System.out.println(interpret);
    Assert.assertEquals(0, interpret.getIssueList().size());
    Assert.assertEquals(0, interpret.getLineageList().size());
  }

  @Test
  public void testOutOfRangeYear() {
    final InterpretationResult<Integer> interpret = InterpretationFactory.interpret(DwcTerm.year, "-1");
    System.out.println(interpret);
    Assert.assertEquals(1, interpret.getIssueList().size());
    Assert.assertEquals(1, interpret.getLineageList().size());
  }

  @Test
  public void testEmptyYear() {
    final InterpretationResult<Integer> interpret = InterpretationFactory.interpret(DwcTerm.year, "");
    System.out.println(interpret);
    Assert.assertEquals(1, interpret.getIssueList().size());
    Assert.assertEquals(1, interpret.getLineageList().size());

  }

  @Test
  public void testValidYear() {
    final InterpretationResult<Integer> interpret = InterpretationFactory.interpret(DwcTerm.year, "2012");
    System.out.println(interpret);
    Assert.assertEquals(0, interpret.getIssueList().size());
    Assert.assertEquals(0, interpret.getLineageList().size());
  }

}
