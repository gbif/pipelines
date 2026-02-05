package org.gbif.converters.converter;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test class for ConverterToVerbatim, specifically for the toNamespacedYamlKey method and extension
 * counting logic.
 */
@RunWith(JUnit4.class)
public class ConverterToVerbatimTest {

  /**
   * Test with a standard Darwin Core URI. Expected: namespace extracted from host and term from
   * path.
   */
  @Test
  public void testToNamespacedYamlKeyWithStandardUri() {
    String uri = "http://rs.tdwg.org/dwc/terms/Occurrence";
    String result = ConverterToVerbatim.toNamespacedYamlKey(uri);
    Assert.assertEquals("rs_Occurrence", result);
  }

  /** Test with a URI containing www. prefix. Expected: www. should be removed from the host. */
  @Test
  public void testToNamespacedYamlKeyWithWwwPrefix() {
    String uri = "http://www.example.org/terms/MyTerm";
    String result = ConverterToVerbatim.toNamespacedYamlKey(uri);
    Assert.assertEquals("example_MyTerm", result);
  }

  /**
   * Test with a URI that has a path with multiple segments. Expected: only the last segment should
   * be used as the term.
   */
  @Test
  public void testToNamespacedYamlKeyWithMultiplePathSegments() {
    String uri = "http://purl.org/dc/terms/modified";
    String result = ConverterToVerbatim.toNamespacedYamlKey(uri);
    Assert.assertEquals("purl_modified", result);
  }

  /**
   * Test with a host that has no dots (single-level domain). Expected: Should handle gracefully -
   * the entire host becomes the namespace.
   */
  @Test
  public void testToNamespacedYamlKeyWithHostWithoutDots() {
    String uri = "http://localhost/terms/MyTerm";
    String result = ConverterToVerbatim.toNamespacedYamlKey(uri);
    Assert.assertEquals("localhost_MyTerm", result);
  }

  /**
   * Test with an empty path. Expected: Should handle edge case - returns namespace with empty term.
   */
  @Test
  public void testToNamespacedYamlKeyWithEmptyPath() {
    String uri = "http://example.org";
    String result = ConverterToVerbatim.toNamespacedYamlKey(uri);
    // Empty path will cause pathParts to be [""], so the last element is ""
    // This results in namespace + "_" + empty string
    Assert.assertEquals("example", result);
  }

  /** Test with a URI that has only a slash as path. Expected: Returns namespace with empty term. */
  @Test
  public void testToNamespacedYamlKeyWithRootPath() {
    String uri = "http://example.org/";
    String result = ConverterToVerbatim.toNamespacedYamlKey(uri);
    // Path "/" split will give ["", ""], last element is ""
    Assert.assertEquals("example_", result);
  }

  /** Test with a malformed URI. Expected: Should catch exception and return the original string. */
  @Test
  public void testToNamespacedYamlKeyWithMalformedUri() {
    String uri = "not-a-valid-uri";
    String result = ConverterToVerbatim.toNamespacedYamlKey(uri);
    Assert.assertEquals(uri, result);
  }

  /**
   * Test with a URI without a host (e.g., relative URI). Expected: Should catch exception and
   * return the original string.
   */
  @Test
  public void testToNamespacedYamlKeyWithoutHost() {
    String uri = "/terms/MyTerm";
    String result = ConverterToVerbatim.toNamespacedYamlKey(uri);
    Assert.assertEquals(uri, result);
  }

  /** Test with null input. Expected: Should catch exception and return null. */
  @Test
  public void testToNamespacedYamlKeyWithNull() {
    String result = ConverterToVerbatim.toNamespacedYamlKey(null);
    Assert.assertNull(result);
  }

  /** Test with empty string. Expected: Should catch exception and return empty string. */
  @Test
  public void testToNamespacedYamlKeyWithEmptyString() {
    String uri = "";
    String result = ConverterToVerbatim.toNamespacedYamlKey(uri);
    Assert.assertEquals(uri, result);
  }

  /** Test with HTTPS URI. Expected: Should work the same as HTTP. */
  @Test
  public void testToNamespacedYamlKeyWithHttps() {
    String uri = "https://rs.gbif.org/terms/Multimedia";
    String result = ConverterToVerbatim.toNamespacedYamlKey(uri);
    Assert.assertEquals("rs_Multimedia", result);
  }

  /**
   * Test with a URI containing special characters in the term. Expected: Should preserve the term
   * as-is.
   */
  @Test
  public void testToNamespacedYamlKeyWithSpecialCharsInTerm() {
    String uri = "http://example.org/terms/My-Special_Term";
    String result = ConverterToVerbatim.toNamespacedYamlKey(uri);
    Assert.assertEquals("example_My-Special_Term", result);
  }
}
