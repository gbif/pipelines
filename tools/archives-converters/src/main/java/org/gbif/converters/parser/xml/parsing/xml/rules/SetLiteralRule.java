package org.gbif.converters.parser.xml.parsing.xml.rules;

import com.google.common.base.MoreObjects;
import org.apache.commons.beanutils.MethodUtils;
import org.apache.commons.digester.Rule;
import org.xml.sax.SAXException;

public class SetLiteralRule extends Rule {

  private final String methodName;
  private final Object value;

  public SetLiteralRule(String methodName, Object value) {
    this.methodName = methodName;
    this.value = value;
  }

  @Override
  public void end(String namespace, String name) throws Exception {
    super.end(namespace, name);

    // if (debug) log.debug(dumpStack());
    Object target = digester.peek();

    if (target == null) {
      throw new SAXException("Call target is null, stackdepth=" + digester.getCount() + ")");
    }

    MethodUtils.invokeExactMethod(target, methodName, value);
  }

  private String dumpStack() {
    StringBuilder sb = new StringBuilder("Digester stack:\n");
    for (int i = 0; i < digester.getCount(); i++) {
      sb.append("Element [")
          .append(i)
          .append("] is of type [")
          .append(digester.peek(i).getClass())
          .append("]\n");
    }
    return sb.toString();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("methodName", methodName)
        .add("value", value)
        .toString();
  }
}
