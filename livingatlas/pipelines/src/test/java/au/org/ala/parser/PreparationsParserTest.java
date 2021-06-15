package au.org.ala.parser;

import static org.junit.Assert.assertEquals;

import au.org.ala.pipelines.vocabulary.PreparationsParser;
import lombok.SneakyThrows;
import org.junit.Before;
import org.junit.Test;

public class PreparationsParserTest {

  private PreparationsParser pp;

  @Before
  @SneakyThrows
  public void init() {
    pp = PreparationsParser.getInstance(null);
  }

  @Test
  public void randomTest() {
    assertEquals("Boxed", pp.parse("Box - 1").getPayload());
    assertEquals("EnvironmentalDNA", pp.parse("EnvironmentalDNA").getPayload());
    assertEquals("EnvironmentalDNA", pp.parse("eDNA").getPayload());
    assertEquals("EnvironmentalDNA", pp.parse("edna").getPayload());
  }
}
