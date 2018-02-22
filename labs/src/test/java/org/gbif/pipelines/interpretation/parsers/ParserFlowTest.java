package org.gbif.pipelines.interpretation.parsers;

import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;

public class ParserFlowTest {


  @Test
  public void intParseTest(){
    //This is an example of naive Integer parser
    ParserFlow<String,Integer> parser = ParserFlow.<String,Integer>of(Integer::parseInt)
      .onException(ex -> System.out.println("Error: " + ex))
      .onParseError(in -> System.out.println("Value " + in + " is can't be parsed"))
      .onSuccess(out -> System.out.println("ParsedValue " + out))
      .withValidation(x -> x > 100, (x,y) -> {System.out.println(y + " Not bigger that 100");});

    Assert.assertEquals(Optional.empty(), parser.parse("199"));
    Assert.assertEquals(Optional.of(77), parser.parse("77"));
    Assert.assertEquals(Optional.empty(), parser.parse("NejNot"));
  }
}
