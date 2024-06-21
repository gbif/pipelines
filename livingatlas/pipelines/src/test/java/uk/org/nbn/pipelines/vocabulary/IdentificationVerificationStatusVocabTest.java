package uk.org.nbn.pipelines.vocabulary;

import static org.junit.Assert.assertEquals;

import au.org.ala.pipelines.vocabulary.Vocab;
import java.util.Optional;
import org.junit.Test;

public class IdentificationVerificationStatusVocabTest {

  @Test
  public void testAccepted() throws Exception {
    IdentificationVerificationStatus.clear();
    Vocab vocab = IdentificationVerificationStatus.getInstance(null);
    assertEquals(Optional.of("Accepted"), vocab.matchTerm("Accepted"));
  }

  @Test
  public void testAccepted_correct() throws Exception {
    IdentificationVerificationStatus.clear();
    Vocab vocab = IdentificationVerificationStatus.getInstance(null);
    assertEquals(Optional.of("Accepted - correct"), vocab.matchTerm("Accepted - correct"));
  }

  @Test
  public void testAccepted_considered_correct() throws Exception {
    IdentificationVerificationStatus.clear();
    Vocab vocab = IdentificationVerificationStatus.getInstance(null);
    assertEquals(
        Optional.of("Accepted - considered correct"),
        vocab.matchTerm("Accepted - considered correct"));
  }

  @Test
  public void testUnconfirmed() throws Exception {
    IdentificationVerificationStatus.clear();
    Vocab vocab = IdentificationVerificationStatus.getInstance(null);
    assertEquals(Optional.of("Unconfirmed"), vocab.matchTerm("Unconfirmed"));
  }

  @Test
  public void testUnconfirmed_plausible() throws Exception {
    IdentificationVerificationStatus.clear();
    Vocab vocab = IdentificationVerificationStatus.getInstance(null);
    assertEquals(
        Optional.of("Unconfirmed - plausible"), vocab.matchTerm("Unconfirmed - plausible"));
  }

  @Test
  public void testUnconfirmed_not_reviewed() throws Exception {
    IdentificationVerificationStatus.clear();
    Vocab vocab = IdentificationVerificationStatus.getInstance(null);
    assertEquals(
        Optional.of("Unconfirmed - not reviewed"), vocab.matchTerm("Unconfirmed - not reviewed"));
  }

  @Test
  public void testUnconfirmed_unreviewed() throws Exception {
    IdentificationVerificationStatus.clear();
    Vocab vocab = IdentificationVerificationStatus.getInstance(null);
    assertEquals(
        Optional.of("Unconfirmed - not reviewed"), vocab.matchTerm("Unconfirmed - unreviewed"));
  }

  @Test
  public void testConsidered_Correct() throws Exception {
    IdentificationVerificationStatus.clear();
    Vocab vocab = IdentificationVerificationStatus.getInstance(null);
    assertEquals(
        Optional.of("Accepted - considered correct"), vocab.matchTerm("Considered Correct"));
  }

  @Test
  public void testCorrect() throws Exception {
    IdentificationVerificationStatus.clear();
    Vocab vocab = IdentificationVerificationStatus.getInstance(null);
    assertEquals(Optional.of("Accepted - correct"), vocab.matchTerm("Correct"));
  }

  @Test
  public void testUnassessed() throws Exception {
    IdentificationVerificationStatus.clear();
    Vocab vocab = IdentificationVerificationStatus.getInstance(null);
    assertEquals(Optional.of("Unconfirmed - not reviewed"), vocab.matchTerm("Unassessed"));
  }

  @Test
  public void testNot_Reviewed() throws Exception {
    IdentificationVerificationStatus.clear();
    Vocab vocab = IdentificationVerificationStatus.getInstance(null);
    assertEquals(Optional.of("Unconfirmed - not reviewed"), vocab.matchTerm("Not Reviewed"));
  }

  @Test
  public void testConfirmed() throws Exception {
    IdentificationVerificationStatus.clear();
    Vocab vocab = IdentificationVerificationStatus.getInstance(null);
    assertEquals(Optional.of("Accepted"), vocab.matchTerm("Confirmed"));
  }

  @Test
  public void testconfirmed() throws Exception {
    IdentificationVerificationStatus.clear();
    Vocab vocab = IdentificationVerificationStatus.getInstance(null);
    assertEquals(Optional.of("Accepted"), vocab.matchTerm("confirmed"));
  }

  @Test
  public void testUnconfirmed_but_plausible() throws Exception {
    IdentificationVerificationStatus.clear();
    Vocab vocab = IdentificationVerificationStatus.getInstance(null);
    assertEquals(
        Optional.of("Unconfirmed - plausible"), vocab.matchTerm("Unconfirmed but plausible"));
  }
}
