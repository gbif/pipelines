package org.gbif.converters.parser.xml.util;

import java.io.FilterReader;
import java.io.IOException;
import java.io.Reader;
import lombok.extern.slf4j.Slf4j;

/**
 * Extends FilterReader to clean character streams of invalid xml characters while streaming. The
 * set of valid chars are defined by the w3c here:
 * http://www.w3.org/TR/2000/REC-xml-20001006#NT-Char. Note that this sanitizing is for the entire
 * xml stream - the problem of illegal characters within elements/CDATA sections (e.g. < > & ) is
 * not handled by this reader. TODO: move to gbif-common project
 */
@Slf4j
public class XmlSanitizingReader extends FilterReader {

  private boolean endOfStreamReached = false;

  public XmlSanitizingReader(Reader in) {
    super(in);
    log.debug("Starting XmlSanitizingReader");
  }

  @Override
  public synchronized int read() throws IOException {
    int nextChar = nextValidXmlChar();
    log.debug("call to read(), returning [{}]", nextChar);
    return nextChar;
  }

  /**
   * This violates the read contract slightly - the returned value is number of chars read in all
   * cases except where end of stream is the first char read. This is something that BufferedReader
   * expects for its readLine() calls (and how it behaves for its implementation of this method).
   */
  @Override
  public synchronized int read(char[] buffer, int offset, int length) throws IOException {
    log.debug("call to read(b, o, l) with l [{}]", length);
    /**
     * TODO: careful here - I think char can only represent basic multilingual plane while int can
     * represent anything, so the cast to char could fail
     */
    int charsRead = 0;
    for (int i = offset; i < (offset + length); i++) {
      int nextChar = nextValidXmlChar();
      if (nextChar != -1) {
        buffer[i] = (char) nextChar;
        charsRead++;
      } else if (charsRead == 0) {
        log.debug("End of stream is first char read: returning -1");
        return -1;
      } else {
        log.debug("At end of stream having read [{}] of requested [{}]", charsRead, length);
        break;
      }
    }

    return charsRead;
  }

  @Override
  public boolean ready() throws IOException {
    return (!endOfStreamReached && in.ready());
  }

  @Override
  public synchronized void close() throws IOException {
    if (in == null) {
      return;
    }
    in.close();
    in = null;
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  private synchronized int nextValidXmlChar() throws IOException {
    Integer validChar = null;
    while (validChar == null) {
      int nextChar = in.read();
      // -1 means end of stream
      if (nextChar == -1) {
        endOfStreamReached = true;
        return -1;
      } else if (isValidXml(nextChar)) {
        validChar = nextChar;
      } else if (log.isDebugEnabled()) {
        log.debug("Dropping invalid xml char [0x{}]", Integer.toHexString(nextChar));
      }
    }

    return validChar;
  }

  private boolean isValidXml(int charVal) {
    return charVal == 0x9
        || charVal == 0xA
        || charVal == 0xD
        || (charVal >= 0x20 && charVal <= 0xD7FF)
        || (charVal >= 0xE000 && charVal <= 0xFFFD)
        || (charVal >= 0x10000 && charVal <= 0x10FFFF);
  }
}
