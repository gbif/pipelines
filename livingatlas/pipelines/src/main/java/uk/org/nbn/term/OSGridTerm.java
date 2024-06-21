package uk.org.nbn.term;

import java.net.URI;
import org.gbif.dwc.terms.Term;

/** Set of terms in use by seedbank */
public enum OSGridTerm implements Term {
  gridReference,
  easting,
  northing,
  zone,
  gridSizeInMeters,
  issues;
  private static final URI NS_URI = URI.create("http://nbn.org.uk/dwc/terms/osgrid/");

  OSGridTerm() {}

  public String simpleName() {
    return this.name();
  }

  @Override
  public String prefixedName() {
    return Term.super.prefixedName();
  }

  @Override
  public String qualifiedName() {
    return Term.super.qualifiedName();
  }

  @Override
  public boolean isClass() {
    return false;
  }

  public String toString() {
    return this.prefixedName();
  }

  public String prefix() {
    return "osgrid";
  }

  public URI namespace() {
    return NS_URI;
  }
}
