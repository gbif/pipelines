package org.gbif.pipelines.core.interpreters.model;

import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.Term;
import java.util.*;

/**
 * Interface for a record that provides methods to extract values using terms,
 * check for values, and handle extensions.
 */
public interface ExtendedRecord {
    List<String> extractListValue(String separatorRegex, Term term);
    List<String> extractListValue(Term term);
    Map<String, List<Map<String, String>>> getExtensions();
    Optional<String> extractLengthAwareOptValue(Term term);
    Optional<String> extractNullAwareOptValue(Map<String, String> termsSource, Term term);
    Optional<String> extractNullAwareOptValue(Term term);
    Optional<String> extractOptValue(Term term);
    String extractNullAwareValue(Term term);
    String extractValue(Term term);
    String getCoreId();
    String getId();
    boolean hasExtension(Extension extension);
    boolean hasExtension(String extension) ;
    boolean hasValue(String value) ;
    boolean hasValue(Term term);
    boolean hasValueNullAware(Term term);
    void checkEmpty();
    Map<String, String> getCoreTerms();
}
