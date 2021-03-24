package au.org.ala.pipelines.vocabulary;

import static org.junit.Assert.assertEquals;

import java.util.Optional;
import org.junit.Test;

public class SensitivityVocabTest {
  @Test
  public void testGenealised1() throws Exception {
    Sensitivity.clear();
    Vocab vocab = Sensitivity.getInstance(null);
    assertEquals(Optional.of("generalised"), vocab.matchTerm("generalised"));
    assertEquals(Optional.of("generalised"), vocab.matchTerm("Generalised"));
    assertEquals(Optional.of("generalised"), vocab.matchTerm("GeNeraLiseD"));
    assertEquals(Optional.of("generalised"), vocab.matchTerm("generalized"));
    assertEquals(Optional.empty(), vocab.matchTerm("Nothing to see here"));
  }

  @Test
  public void testGenealised2() throws Exception {
    Sensitivity.clear();
    String path = this.getClass().getResource("sensitivities-1.txt").getPath();
    Vocab vocab = Sensitivity.getInstance(path);
    assertEquals(Optional.of("generalised"), vocab.matchTerm("generalised"));
    assertEquals(Optional.of("generalised"), vocab.matchTerm("Something"));
    assertEquals(Optional.of("generalised"), vocab.matchTerm("Load of old rubbish"));
    assertEquals(Optional.empty(), vocab.matchTerm("Nothing to see here"));
  }

  @Test
  public void testAlreadyGenealised1() throws Exception {
    Sensitivity.clear();
    Vocab vocab = Sensitivity.getInstance(null);
    assertEquals(Optional.of("alreadyGeneralised"), vocab.matchTerm("alreadyGeneralised"));
    assertEquals(Optional.of("alreadyGeneralised"), vocab.matchTerm("Already Generalised"));
    assertEquals(Optional.empty(), vocab.matchTerm("Blah de blah de blah"));
  }
}
