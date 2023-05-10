package io.opentelemetry.contrib.awsxray;

import javax.annotation.Nullable;
import io.opentelemetry.sdk.trace.ReadableSpan;

public interface AwsOriginDetector {
  @Nullable
  AwsOrigin detectOrigin(ReadableSpan span);
}
