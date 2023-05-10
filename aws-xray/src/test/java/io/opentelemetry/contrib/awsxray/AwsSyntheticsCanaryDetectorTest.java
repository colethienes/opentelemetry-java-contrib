/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.contrib.awsxray;

import static io.opentelemetry.semconv.trace.attributes.SemanticAttributes.HTTP_USER_AGENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.opentelemetry.sdk.trace.ReadableSpan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link AwsSyntheticsCanaryDetector}. */
class AwsSyntheticsCanaryDetectorTest {

  private static final AwsOriginDetector DETECTOR = new AwsSyntheticsCanaryDetector();
  private ReadableSpan span;

  @BeforeEach
  public void setUp() {
    span = mock(ReadableSpan.class);
  }

  @Test
  void shouldDetectCanaryArn() {
    String userAgent = "Mozilla/5.0 (X11; Linux x86_64) "
        + "AppleWebKit/537.36 (KHTML, like Gecko) "
        + "HeadlessChrome/92.0.4512.0 "
        + "Safari/537.36 "
        + "CloudWatchSynthetics/arn:aws:synthetics:us-east-1:1234567890:canary:my-canary";
    when(span.getAttribute(HTTP_USER_AGENT)).thenReturn(userAgent);
    AwsOrigin origin = DETECTOR.detectOrigin(span);
    assertThat(origin).isNotNull();
    assertThat(origin.getResourceArn()).isEqualTo("arn:aws:synthetics:us-east-1:1234567890:canary:my-canary");
  }

  @Test
  void shouldNotDetectIfNoCanaryArn() {
    String userAgent = "Mozilla/5.0 (X11; Linux x86_64) Safari/537.36";
    when(span.getAttribute(HTTP_USER_AGENT)).thenReturn(userAgent);
    AwsOrigin origin = DETECTOR.detectOrigin(span);
    assertThat(origin).isNull();
  }

  @Test
  void shouldSkipDetectionIfNoUserAgent() {
    when(span.getAttribute(HTTP_USER_AGENT)).thenReturn(null);
    AwsOrigin origin = DETECTOR.detectOrigin(span);
    assertThat(origin).isNull();
  }
}
