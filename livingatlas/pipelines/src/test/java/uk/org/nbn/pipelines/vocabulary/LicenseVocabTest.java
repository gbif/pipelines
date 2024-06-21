package uk.org.nbn.pipelines.vocabulary;

import static org.junit.Assert.assertEquals;

import au.org.ala.pipelines.vocabulary.Vocab;
import java.util.Optional;
import org.junit.Test;

public class LicenseVocabTest {

  @Test
  public void testOGL() throws Exception {
    NBNLicense.clear();
    Vocab vocab = NBNLicense.getInstance(null);
    assertEquals(Optional.of("OGL"), vocab.matchTerm("OGL"));
  }

  @Test
  public void testCC0() throws Exception {
    NBNLicense.clear();
    Vocab vocab = NBNLicense.getInstance(null);
    assertEquals(Optional.of("CC0"), vocab.matchTerm("CC0"));
    assertEquals(Optional.of("CC0"), vocab.matchTerm("CCO"));
  }

  @Test
  public void testCC_BY() throws Exception {
    NBNLicense.clear();
    Vocab vocab = NBNLicense.getInstance(null);
    assertEquals(Optional.of("CC-BY"), vocab.matchTerm("CC-BY"));
    assertEquals(Optional.of("CC-BY"), vocab.matchTerm("CC BY"));
    assertEquals(Optional.of("CC-BY"), vocab.matchTerm("CC-BY-4.0"));
  }

  @Test
  public void testCC_BY_NC() throws Exception {
    NBNLicense.clear();
    Vocab vocab = NBNLicense.getInstance(null);
    assertEquals(Optional.of("CC-BY-NC"), vocab.matchTerm("CC-BY-NC"));
    assertEquals(Optional.of("CC-BY-NC"), vocab.matchTerm("CC BY-NC"));
    assertEquals(Optional.of("CC-BY-NC"), vocab.matchTerm("CC BY NC"));
    assertEquals(Optional.of("CC-BY-NC"), vocab.matchTerm("CC-BY NC"));
    assertEquals(Optional.of("CC-BY-NC"), vocab.matchTerm("CC-NY-NC"));
  }

  @Test
  public void testEmpty() throws Exception {
    NBNLicense.clear();
    Vocab vocab = NBNLicense.getInstance(null);
    assertEquals(Optional.empty(), vocab.matchTerm(""));
  }
}
