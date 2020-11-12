package org.gbif.converters.parser.xml.parsing.xml.rules;

import lombok.ToString;
import org.apache.commons.beanutils.MethodUtils;
import org.apache.commons.digester.Rule;
import org.xml.sax.SAXException;

@ToString
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

    Object target = digester.peek();

    if (target == null) {
      throw new SAXException("Call target is null, stackdepth=" + digester.getCount() + ")");
    }

    MethodUtils.invokeExactMethod(target, methodName, value);
  }
}
