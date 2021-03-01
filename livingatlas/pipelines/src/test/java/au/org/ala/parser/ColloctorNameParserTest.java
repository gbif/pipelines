package au.org.ala.parser;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import au.org.ala.pipelines.parser.CollectorNameParser;
import org.junit.Test;

public class ColloctorNameParserTest {

  @Test
  public void numericTest() {
    assertEquals(null, CollectorNameParser.parse("123"));
  }

  @Test
  public void singleNameTest() {
    assertEquals(
        "Hegedus, Alexandra Danica", CollectorNameParser.parse("Hegedus, Ms Alexandra Danica"));
    assertEquals("Field, P. Ross", CollectorNameParser.parse("Field, Ross P."));
    assertEquals("van Leeuwen, S.", CollectorNameParser.parse("van Leeuwen, S."));
    assertEquals("Starr, Simon", CollectorNameParser.parse("Simon Starr"));
    assertEquals("Kaspiew, B.", CollectorNameParser.parse("B Kaspiew (Professor)"));
    assertEquals("Starr, S.S. Simon", CollectorNameParser.parse("Simon S.S Starr"));
  }

  @Test
  public void titleTest() {
    assertEquals("Kirby, N.L.", CollectorNameParser.parse("Dr NL Kirby"));
    assertEquals("Dittrich", CollectorNameParser.parse("Dittrich, Lieutenant"));
  }

  @Test
  public void surnameFirstnameTest() {
    assertEquals("Beauglehole, A.C.", CollectorNameParser.parse("Beauglehole, A.C."));
    assertEquals("Beauglehole, A.C. Atest", CollectorNameParser.parse("Beauglehole, A.C. Atest"));
    assertEquals("Beauglehole, Atest", CollectorNameParser.parse("Beauglehole, Atest"));
    assertEquals("Field, P. Ross", CollectorNameParser.parse("Field, Ross P."));
    assertEquals("Robinson, A.C. Tony", CollectorNameParser.parse("\"ROBINSON A.C. Tony\""));
    assertEquals("Graham, K.L. Kate", CollectorNameParser.parse("GRAHAM K.L. Kate"));
    assertEquals("Gunness, A.G.", CollectorNameParser.parse("A.G.Gunness"));
    assertArrayEquals(
        new String[] {"Graham, K.L. Kate"}, CollectorNameParser.parseList("GRAHAM K.L. Kate"));
    assertArrayEquals(
        new String[] {"natasha.carter@csiro.au"},
        CollectorNameParser.parseList("natasha.carter@csiro.au"));
    assertArrayEquals(
        new String[] {"Gunness, A.G."}, CollectorNameParser.parseList("A.G.Gunness et. al."));
  }

  @Test
  public void hyphenNameTest() {
    assertArrayEquals(
        new String[] {"Davies, R.J-P. Richard"},
        CollectorNameParser.parseList("\"DAVIES R.J-P. Richard\""));
    assertArrayEquals(
        new String[] {"Kenny, S.D. Sue", "Wallace-Ward, D. Di"},
        CollectorNameParser.parseList("\"KENNY S.D. Sue\"\"WALLACE-WARD D. Di\""));
    assertArrayEquals(
        new String[] {"Russell-Smith, J."}, CollectorNameParser.parseList("Russell-Smith, J."));
  }

  @Test
  public void prefixSurnamesTest() {
    assertArrayEquals(
        new String[] {"van Leeuwen, S."}, CollectorNameParser.parseList("van Leeuwen, S."));
    assertArrayEquals(
        new String[] {"von Blandowski, J.W.T.L."},
        CollectorNameParser.parseList("Blandowski, J.W.T.L. von"));
    assertArrayEquals(
        new String[] {"van der Leeuwen, Simon"},
        CollectorNameParser.parseList("van der Leeuwen, Simon"));
  }

  @Test
  public void ignoreBracketsTest() {
    assertArrayEquals(
        new String[] {"Kinnear, A.J."}, CollectorNameParser.parseList("\"KINNEAR A.J. (Sandy)\""));
    assertArrayEquals(
        new String[] {"Ratkowsky, David"}, CollectorNameParser.parseList("David Ratkowsky (2589)"));
  }

  @Test
  public void initsSurnameTest() {
    assertArrayEquals(new String[] {"Kirby, N.L."}, CollectorNameParser.parseList("NL Kirby"));
    assertArrayEquals(
        new String[] {"Annabell, R. Graeme"},
        CollectorNameParser.parseList("Annabell, Mr. Graeme R"));
    assertArrayEquals(
        new String[] {"Kaspiew, B."}, CollectorNameParser.parseList("B Kaspiew (Professor)"));
    assertArrayEquals(
        new String[] {"Hegedus, Alexandra", "Australian Museum", "Science"},
        CollectorNameParser.parseList("Hegedus, Ms Alexandra - Australian Museum - Science"));
    assertArrayEquals(
        new String[] {"Hegedus, Alexandra Danica", "Australian Museum", "Science"},
        CollectorNameParser.parseList(
            "Hegedus, Ms Alexandra Danica - Australian Museum - Science"));
  }

  @Test
  public void unknownTest() {
    assertArrayEquals(
        new String[] {"UNKNOWN OR ANONYMOUS"}, CollectorNameParser.parseList("No data"));
    assertArrayEquals(
        new String[] {"UNKNOWN OR ANONYMOUS"}, CollectorNameParser.parseList("[unknown]"));
    assertArrayEquals(
        new String[] {"UNKNOWN OR ANONYMOUS"},
        CollectorNameParser.parseList("\"NOT ENTERED - SEE ORIGINAL DATA  -\""));
    assertArrayEquals(
        new String[] {"UNKNOWN OR ANONYMOUS"}, CollectorNameParser.parseList("\"ANON  N/A\""));
  }

  @Test
  public void orgTest() {
    assertArrayEquals(
        new String[] {"Canberra Ornithologists Group"},
        CollectorNameParser.parseList("Canberra Ornithologists Group"));
    assertArrayEquals(
        new String[] {"\"SA ORNITHOLOGICAL ASSOCIATION  SAOA\""},
        CollectorNameParser.parseList("\"SA ORNITHOLOGICAL ASSOCIATION  SAOA\""));
    assertArrayEquals(
        new String[] {"Macquarie Island summer and wintering parties"},
        CollectorNameParser.parseList("Macquarie Island summer and wintering parties"));
    assertArrayEquals(
        new String[] {"test Australian Museum test"},
        CollectorNameParser.parseList("test Australian Museum test"));
    assertArrayEquals(
        new String[] {"\"NPWS-(SA) N/A\""}, CollectorNameParser.parseList("\"NPWS-(SA) N/A\""));
    assertArrayEquals(
        new String[] {"UNKNOWN OR ANONYMOUS"},
        CollectorNameParser.parseList("\"NOT ENTERED - SEE ORIGINAL DATA -\""));
  }

  @Test
  public void multiCollectorTest() {
    assertArrayEquals(
        new String[] {"Gomersall, N.", "Gomersall, V."},
        CollectorNameParser.parseList("N.& V.Gomersall"));
    assertArrayEquals(
        new String[] {"James, David", "Scofield, Paul"},
        CollectorNameParser.parseList("David James, Paul Scofield"));
    assertArrayEquals(
        new String[] {"Fisher, Keith", "Fisher, Lindsay"},
        CollectorNameParser.parseList("Keith & Lindsay Fisher"));
    assertArrayEquals(
        new String[] {"Spillane, Nicole", "Jacobson, Paul"},
        CollectorNameParser.parseList("Nicole Spillane & Paul Jacobson"));
    assertArrayEquals(
        new String[] {"Spurgeon, Pauline", "Spurgeon, Arthur"},
        CollectorNameParser.parseList("Pauline and Arthur Spurgeon"));
    assertArrayEquals(
        new String[] {"Andrews-Goff, Virginia", "Spinks, Jim"},
        CollectorNameParser.parseList("Virginia Andrews-Goff and Jim Spinks"));
    assertArrayEquals(
        new String[] {"Kemper, C.M. Cath", "Carpenter, G.A. Graham"},
        CollectorNameParser.parseList("\"KEMPER C.M. Cath\"\"CARPENTER G.A. Graham\""));
    assertArrayEquals(
        new String[] {"Simmons, J.G.", "Simmons, M.H."},
        CollectorNameParser.parseList("Simmons, J.G.; Simmons, M.H."));
    assertArrayEquals(
        new String[] {"Hedley, C.", "Starkey", "Kesteven, H.L."},
        CollectorNameParser.parseList("C.Hedley, Mrs.Starkey & H.L.Kesteven"));
    assertArrayEquals(
        new String[] {"Kenny, S.D. Sue", "Wallace-Ward, D. Di"},
        CollectorNameParser.parseList("\"KENNY S.D. Sue\"\"WALLACE-WARD D. Di\""));
  }
}
