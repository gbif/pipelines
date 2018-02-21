package org.gbif.pipelines.interpretation.columns;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.interpretation.column.InterpretationFactory;
import org.gbif.pipelines.interpretation.column.InterpretationResult;

import org.junit.Assert;
import org.junit.Test;

public class BasicLocationInterpretationTest {

  /*********************************country **********************/
  @Test
  public void testCountryWhenNull() {
    final InterpretationResult<Integer> interpret = InterpretationFactory.interpret(DwcTerm.country, null);
    System.out.println(interpret);
    Assert.assertEquals(0, interpret.getIssueList().size());
    Assert.assertEquals(0, interpret.getLineageList().size());
  }

  @Test
  public void testCountryWhenEmpty() {
    final InterpretationResult<Integer> interpret = InterpretationFactory.interpret(DwcTerm.country, "");
    System.out.println(interpret);
    Assert.assertEquals(1, interpret.getIssueList().size());
    Assert.assertEquals(1, interpret.getLineageList().size());
  }

  @Test
  public void testCountryWhenValid() {
    final InterpretationResult<Integer> interpret = InterpretationFactory.interpret(DwcTerm.country, "DENMARK");
    System.out.println(interpret);
    Assert.assertEquals(0, interpret.getIssueList().size());
    Assert.assertEquals(0, interpret.getLineageList().size());
  }

  @Test
  public void testCountryWhenInValid() {
    final InterpretationResult<Integer> interpret = InterpretationFactory.interpret(DwcTerm.country, "DAM");
    System.out.println(interpret);
    Assert.assertEquals(1, interpret.getIssueList().size());
    Assert.assertEquals(1, interpret.getLineageList().size());
  }

  /**********************************************continent*************/
  @Test
  public void testContinentWhenNull() {
    final InterpretationResult<Integer> interpret = InterpretationFactory.interpret(DwcTerm.continent, null);
    System.out.println(interpret);
    Assert.assertEquals(0, interpret.getIssueList().size());
    Assert.assertEquals(0, interpret.getLineageList().size());
  }

  @Test
  public void testContinentWhenEmpty() {
    final InterpretationResult<Integer> interpret = InterpretationFactory.interpret(DwcTerm.continent, "");
    System.out.println(interpret);
    Assert.assertEquals(1, interpret.getIssueList().size());
    Assert.assertEquals(1, interpret.getLineageList().size());
  }

  @Test
  public void testContinentWhenValid() {
    final InterpretationResult<Integer> interpret = InterpretationFactory.interpret(DwcTerm.continent, "EUROPE");
    System.out.println(interpret);
    Assert.assertEquals(0, interpret.getIssueList().size());
    Assert.assertEquals(0, interpret.getLineageList().size());
  }

  @Test
  public void testContinentWhenInValid() {
    final InterpretationResult<Integer> interpret = InterpretationFactory.interpret(DwcTerm.continent, "EPR");
    System.out.println(interpret);
    Assert.assertEquals(1, interpret.getIssueList().size());
    Assert.assertEquals(1, interpret.getLineageList().size());
  }

  /**********************************************countrycode*************/
  @Test
  public void testCountryCodeWhenNull() {
    final InterpretationResult<Integer> interpret = InterpretationFactory.interpret(DwcTerm.countryCode, null);
    System.out.println(interpret);
    Assert.assertEquals(0, interpret.getIssueList().size());
    Assert.assertEquals(0, interpret.getLineageList().size());
  }

  @Test
  public void testCountryCodeWhenEmpty() {
    final InterpretationResult<Integer> interpret = InterpretationFactory.interpret(DwcTerm.countryCode, "");
    System.out.println(interpret);
    Assert.assertEquals(1, interpret.getIssueList().size());
    Assert.assertEquals(1, interpret.getLineageList().size());
  }

  @Test
  public void testCountryCodeWhenValid() {
    final InterpretationResult<Integer> interpret = InterpretationFactory.interpret(DwcTerm.countryCode, "AUS");
    System.out.println(interpret);
    Assert.assertEquals(0, interpret.getIssueList().size());
    Assert.assertEquals(0, interpret.getLineageList().size());
  }

  @Test
  public void testCountryCodeWhenInValid() {
    final InterpretationResult<Integer> interpret = InterpretationFactory.interpret(DwcTerm.countryCode, "EPR");
    System.out.println(interpret);
    Assert.assertEquals(1, interpret.getIssueList().size());
    Assert.assertEquals(1, interpret.getLineageList().size());
  }

}
