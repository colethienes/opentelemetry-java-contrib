package io.opentelemetry.contrib.awsxray;

import io.opentelemetry.sdk.trace.ReadableSpan;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import static io.opentelemetry.semconv.trace.attributes.SemanticAttributes.HTTP_USER_AGENT;

public class AwsSyntheticsCanaryDetector implements AwsOriginDetector {
  private static final String CANARY_RESOURCE_TYPE = "AWS::Synthetics::Canary";
  private static final Pattern CANARY_ARN_PATTERN = Pattern.compile(
      "arn:aws:synthetics:[^:\\s]+:[^:\\s]+:canary:[^\\s]+");

  @Nullable
  @Override
  public AwsOrigin detectOrigin(ReadableSpan span) {
    String userAgent = span.getAttribute(HTTP_USER_AGENT);
    if (userAgent == null) {
      return null;
    }
    Matcher matcher = CANARY_ARN_PATTERN.matcher(userAgent);
    if (matcher.find()) {
      String canaryArn = matcher.group();
      return new AwsOrigin(CANARY_RESOURCE_TYPE, canaryArn);
    }
    return null;
  }
}
