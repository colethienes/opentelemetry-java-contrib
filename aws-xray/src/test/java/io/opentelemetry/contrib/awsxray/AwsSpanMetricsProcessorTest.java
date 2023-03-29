/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.contrib.awsxray;

import static io.opentelemetry.semconv.trace.attributes.SemanticAttributes.HTTP_STATUS_CODE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.ReadWriteSpan;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** Unit tests for {@link AwsSpanMetricsProcessor}. */
class AwsSpanMetricsProcessorTest {

  // Test constants
  private static final boolean IS_SAMPLED = true;
  private static final boolean IS_NOT_SAMPLED = false;
  private static final boolean START_REQUIRED = true;
  private static final boolean START_NOT_REQUIRED = false;
  private static final boolean END_REQUIRED = true;
  private static final boolean END_NOT_REQUIRED = false;
  private static final boolean CONTAINS_ATTRIBUTES = true;
  private static final boolean CONTAINS_NO_ATTRIBUTES = false;
  private static final double TEST_LATENCY_MILLIS = 150.0;
  private static final long TEST_LATENCY_NANOS = 150_000_000L;

  // Resource is not mockable, but tests can safely rely on an empty resource.
  private static final Resource testResource = Resource.empty();

  // Useful enum for indicating expected HTTP status code-related metrics
  private enum ExpectedStatusMetric {
    ERROR,
    FAULT,
    NEITHER
  }

  // Mocks required for tests.
  private LongCounter errorCounterMock;
  private LongCounter faultCounterMock;
  private DoubleHistogram latencyHistogramMock;
  private MetricAttributeGenerator generatorMock;
  private SpanProcessor delegateMock;

  private AwsSpanMetricsProcessor awsSpanMetricsProcessor;

  @BeforeEach
  public void setUpMocks() {
    errorCounterMock = mock(LongCounter.class);
    faultCounterMock = mock(LongCounter.class);
    latencyHistogramMock = mock(DoubleHistogram.class);
    generatorMock = mock(MetricAttributeGenerator.class);
    delegateMock = mock(SpanProcessor.class);

    awsSpanMetricsProcessor =
        AwsSpanMetricsProcessor.create(
            errorCounterMock,
            faultCounterMock,
            latencyHistogramMock,
            generatorMock,
            delegateMock,
            testResource);
  }

  @Test
  public void testIsRequired() {
    // Start requirement is dependent on the delegate
    when(delegateMock.isStartRequired()).thenReturn(START_REQUIRED);
    assertThat(awsSpanMetricsProcessor.isStartRequired()).isTrue();
    when(delegateMock.isStartRequired()).thenReturn(START_NOT_REQUIRED);
    assertThat(awsSpanMetricsProcessor.isStartRequired()).isFalse();
    verify(delegateMock, times(2)).isStartRequired();

    // End requirement is always required.
    when(delegateMock.isEndRequired()).thenReturn(END_REQUIRED);
    assertThat(awsSpanMetricsProcessor.isEndRequired()).isTrue();
    when(delegateMock.isEndRequired()).thenReturn(END_NOT_REQUIRED);
    assertThat(awsSpanMetricsProcessor.isEndRequired()).isTrue();
    verify(delegateMock, never()).isEndRequired();
  }

  @Test
  public void testPassthroughDelegations() {
    Context parentContextMock = mock(Context.class);
    ReadWriteSpan spanMock = mock(ReadWriteSpan.class);
    awsSpanMetricsProcessor.onStart(parentContextMock, spanMock);
    awsSpanMetricsProcessor.shutdown();
    awsSpanMetricsProcessor.forceFlush();
    awsSpanMetricsProcessor.close();
    verify(delegateMock, times(1)).onStart(eq(parentContextMock), eq(spanMock));
    verify(delegateMock, times(1)).shutdown();
    verify(delegateMock, times(1)).forceFlush();
    verify(delegateMock, times(1)).close();
  }

  /**
   * Tests starting with testOnEndDelegation are testing the delegation logic of onEnd - i.e. the
   * logic in AwsSpanMetricsProcessor pertaining to calling its delegate SpanProcessors' onEnd
   * method.
   */
  @Test
  public void testOnEndDelegationWithoutEndRequired() {
    Attributes spanAttributes = buildSpanAttributes(CONTAINS_ATTRIBUTES);
    ReadableSpan readableSpanMock = buildReadableSpanMock(IS_SAMPLED, spanAttributes);
    Attributes metricAttributes = buildMetricAttributes(CONTAINS_ATTRIBUTES);
    configureMocksForOnEnd(readableSpanMock, END_NOT_REQUIRED, metricAttributes);

    awsSpanMetricsProcessor.onEnd(readableSpanMock);
    verify(delegateMock, never()).onEnd(any());
  }

  @Test
  public void testOnEndDelegationWithoutMetricAttributes() {
    Attributes spanAttributes = buildSpanAttributes(CONTAINS_ATTRIBUTES);
    ReadableSpan readableSpanMock = buildReadableSpanMock(IS_SAMPLED, spanAttributes);
    Attributes metricAttributes = buildMetricAttributes(CONTAINS_NO_ATTRIBUTES);
    configureMocksForOnEnd(readableSpanMock, END_REQUIRED, metricAttributes);

    awsSpanMetricsProcessor.onEnd(readableSpanMock);
    verify(delegateMock, times(1)).onEnd(eq(readableSpanMock));
  }

  @Test
  public void testOnEndDelegationWithoutSampling() {
    Attributes spanAttributes = buildSpanAttributes(CONTAINS_ATTRIBUTES);
    ReadableSpan readableSpanMock = buildReadableSpanMock(IS_NOT_SAMPLED, spanAttributes);
    Attributes metricAttributes = buildMetricAttributes(CONTAINS_ATTRIBUTES);
    configureMocksForOnEnd(readableSpanMock, END_REQUIRED, metricAttributes);

    awsSpanMetricsProcessor.onEnd(readableSpanMock);
    verify(delegateMock, times(1)).onEnd(eq(readableSpanMock));
  }

  @Test
  public void testOnEndDelegationWithoutSpanAttributes() {
    Attributes spanAttributes = buildSpanAttributes(CONTAINS_NO_ATTRIBUTES);
    ReadableSpan readableSpanMock = buildReadableSpanMock(IS_SAMPLED, spanAttributes);
    Attributes metricAttributes = buildMetricAttributes(CONTAINS_ATTRIBUTES);
    configureMocksForOnEnd(readableSpanMock, END_REQUIRED, metricAttributes);

    awsSpanMetricsProcessor.onEnd(readableSpanMock);
    ArgumentCaptor<ReadableSpan> readableSpanCaptor = ArgumentCaptor.forClass(ReadableSpan.class);
    verify(delegateMock, times(1)).onEnd(readableSpanCaptor.capture());
    ReadableSpan delegateSpan = readableSpanCaptor.getValue();
    assertThat(delegateSpan).isNotEqualTo(readableSpanMock);

    metricAttributes.forEach((k, v) -> assertThat(delegateSpan.getAttribute(k)).isEqualTo(v));

    SpanData delegateSpanData = delegateSpan.toSpanData();
    Attributes delegateAttributes = delegateSpanData.getAttributes();
    assertThat(delegateAttributes.size()).isEqualTo(metricAttributes.size());
    assertThat(delegateSpanData.getTotalAttributeCount()).isEqualTo(metricAttributes.size());
    metricAttributes.forEach((k, v) -> assertThat(delegateAttributes.get(k)).isEqualTo(v));
  }

  @Test
  public void testOnEndDelegationWithEverything() {
    Attributes spanAttributes = buildSpanAttributes(CONTAINS_ATTRIBUTES);
    ReadableSpan readableSpanMock = buildReadableSpanMock(IS_SAMPLED, spanAttributes);
    Attributes metricAttributes = buildMetricAttributes(CONTAINS_ATTRIBUTES);
    configureMocksForOnEnd(readableSpanMock, END_REQUIRED, metricAttributes);

    awsSpanMetricsProcessor.onEnd(readableSpanMock);
    ArgumentCaptor<ReadableSpan> readableSpanCaptor = ArgumentCaptor.forClass(ReadableSpan.class);
    verify(delegateMock, times(1)).onEnd(readableSpanCaptor.capture());
    ReadableSpan delegateSpan = readableSpanCaptor.getValue();
    assertThat(delegateSpan).isNotEqualTo(readableSpanMock);

    spanAttributes.forEach((k, v) -> assertThat(delegateSpan.getAttribute(k)).isEqualTo(v));
    metricAttributes.forEach((k, v) -> assertThat(delegateSpan.getAttribute(k)).isEqualTo(v));

    SpanData delegateSpanData = delegateSpan.toSpanData();
    Attributes delegateAttributes = delegateSpanData.getAttributes();
    assertThat(delegateAttributes.size())
        .isEqualTo(metricAttributes.size() + spanAttributes.size());
    assertThat(delegateSpanData.getTotalAttributeCount())
        .isEqualTo(metricAttributes.size() + spanAttributes.size());
    spanAttributes.forEach((k, v) -> assertThat(delegateAttributes.get(k)).isEqualTo(v));
    metricAttributes.forEach((k, v) -> assertThat(delegateAttributes.get(k)).isEqualTo(v));
  }

  @Test
  public void testOnEndDelegatedSpanBehaviour() {
    Attributes spanAttributes = buildSpanAttributes(CONTAINS_ATTRIBUTES);
    ReadableSpan readableSpanMock = buildReadableSpanMock(IS_SAMPLED, spanAttributes);
    Attributes metricAttributes = buildMetricAttributes(CONTAINS_ATTRIBUTES);
    configureMocksForOnEnd(readableSpanMock, END_REQUIRED, metricAttributes);

    awsSpanMetricsProcessor.onEnd(readableSpanMock);
    ArgumentCaptor<ReadableSpan> readableSpanCaptor = ArgumentCaptor.forClass(ReadableSpan.class);
    verify(delegateMock, times(1)).onEnd(readableSpanCaptor.capture());
    ReadableSpan delegateSpan = readableSpanCaptor.getValue();
    assertThat(delegateSpan).isNotEqualTo(readableSpanMock);

    // Validate all calls to wrapper span get simply delegated to the wrapped span (except ones
    // pertaining to attributes.
    SpanContext spanContextMock = mock(SpanContext.class);
    when(readableSpanMock.getSpanContext()).thenReturn(spanContextMock);
    assertThat(delegateSpan.getSpanContext()).isEqualTo(spanContextMock);

    SpanContext parentSpanContextMock = mock(SpanContext.class);
    when(readableSpanMock.getParentSpanContext()).thenReturn(parentSpanContextMock);
    assertThat(delegateSpan.getParentSpanContext()).isEqualTo(parentSpanContextMock);

    String name = "name";
    when(readableSpanMock.getName()).thenReturn(name);
    assertThat(delegateSpan.getName()).isEqualTo(name);

    // InstrumentationLibraryInfo is deprecated, so actually invoking it causes build failures.
    // Excluding from this test.

    InstrumentationScopeInfo instrumentationScopeInfo = InstrumentationScopeInfo.empty();
    when(readableSpanMock.getInstrumentationScopeInfo()).thenReturn(instrumentationScopeInfo);
    assertThat(delegateSpan.getInstrumentationScopeInfo()).isEqualTo(instrumentationScopeInfo);

    boolean ended = true;
    when(readableSpanMock.hasEnded()).thenReturn(ended);
    assertThat(delegateSpan.hasEnded()).isEqualTo(ended);

    long latencyNanos = TEST_LATENCY_NANOS;
    when(readableSpanMock.getLatencyNanos()).thenReturn(latencyNanos);
    assertThat(delegateSpan.getLatencyNanos()).isEqualTo(latencyNanos);

    SpanKind spanKind = SpanKind.CLIENT;
    when(readableSpanMock.getKind()).thenReturn(spanKind);
    assertThat(delegateSpan.getKind()).isEqualTo(spanKind);

    Long attributeValue = 0L;
    when(readableSpanMock.getAttribute(HTTP_STATUS_CODE)).thenReturn(attributeValue);
    assertThat(delegateSpan.getAttribute(HTTP_STATUS_CODE)).isEqualTo(attributeValue);
  }

  /**
   * Tests starting with testOnEndMetricsGeneration are testing the logic in
   * AwsSpanMetricsProcessor's onEnd method pertaining to metrics generation.
   */
  @Test
  public void testOnEndMetricsGenerationWithoutSpanAttributes() {
    Attributes spanAttributes = buildSpanAttributes(CONTAINS_NO_ATTRIBUTES);
    ReadableSpan readableSpanMock = buildReadableSpanMock(IS_SAMPLED, spanAttributes);
    Attributes metricAttributes = buildMetricAttributes(CONTAINS_ATTRIBUTES);
    configureMocksForOnEnd(readableSpanMock, END_REQUIRED, metricAttributes);

    awsSpanMetricsProcessor.onEnd(readableSpanMock);
    verifyNoInteractions(errorCounterMock);
    verifyNoInteractions(faultCounterMock);
    verify(latencyHistogramMock, times(1)).record(eq(TEST_LATENCY_MILLIS), eq(metricAttributes));
  }

  @Test
  public void testOnEndMetricsGenerationWithoutMetricAttributes() {
    Attributes spanAttributes = Attributes.of(HTTP_STATUS_CODE, 500L);
    ReadableSpan readableSpanMock = buildReadableSpanMock(IS_SAMPLED, spanAttributes);
    Attributes metricAttributes = buildMetricAttributes(CONTAINS_NO_ATTRIBUTES);
    configureMocksForOnEnd(readableSpanMock, END_REQUIRED, metricAttributes);

    awsSpanMetricsProcessor.onEnd(readableSpanMock);
    verifyNoInteractions(errorCounterMock);
    verifyNoInteractions(faultCounterMock);
    verifyNoInteractions(latencyHistogramMock);
  }

  @Test
  public void testOnEndMetricsGenerationWithoutEndRequired() {
    Attributes spanAttributes = Attributes.of(HTTP_STATUS_CODE, 500L);
    ReadableSpan readableSpanMock = buildReadableSpanMock(IS_SAMPLED, spanAttributes);
    Attributes metricAttributes = buildMetricAttributes(CONTAINS_ATTRIBUTES);
    configureMocksForOnEnd(readableSpanMock, END_NOT_REQUIRED, metricAttributes);

    awsSpanMetricsProcessor.onEnd(readableSpanMock);
    verifyNoInteractions(errorCounterMock);
    verify(faultCounterMock, times(1)).add(eq(1L), eq(metricAttributes));
    verify(latencyHistogramMock, times(1)).record(eq(TEST_LATENCY_MILLIS), eq(metricAttributes));
  }

  @Test
  public void testOnEndMetricsGenerationWithLatency() {
    Attributes spanAttributes = Attributes.of(HTTP_STATUS_CODE, 200L);
    ReadableSpan readableSpanMock = buildReadableSpanMock(IS_SAMPLED, spanAttributes);
    Attributes metricAttributes = buildMetricAttributes(CONTAINS_ATTRIBUTES);
    configureMocksForOnEnd(readableSpanMock, END_REQUIRED, metricAttributes);

    when(readableSpanMock.getLatencyNanos()).thenReturn(5_500_000L);

    awsSpanMetricsProcessor.onEnd(readableSpanMock);
    verifyNoInteractions(errorCounterMock);
    verifyNoInteractions(faultCounterMock);
    verify(latencyHistogramMock, times(1)).record(eq(5.5), eq(metricAttributes));
  }

  @Test
  public void testOnEndMetricsGenerationWithStatusCodes() {
    // Invalid HTTP status codes
    validateMetricsGeneratedForHttpStatusCode(null, ExpectedStatusMetric.NEITHER);

    // Valid HTTP status codes
    validateMetricsGeneratedForHttpStatusCode(200L, ExpectedStatusMetric.NEITHER);
    validateMetricsGeneratedForHttpStatusCode(399L, ExpectedStatusMetric.NEITHER);
    validateMetricsGeneratedForHttpStatusCode(400L, ExpectedStatusMetric.ERROR);
    validateMetricsGeneratedForHttpStatusCode(499L, ExpectedStatusMetric.ERROR);
    validateMetricsGeneratedForHttpStatusCode(500L, ExpectedStatusMetric.FAULT);
    validateMetricsGeneratedForHttpStatusCode(599L, ExpectedStatusMetric.FAULT);
    validateMetricsGeneratedForHttpStatusCode(600L, ExpectedStatusMetric.NEITHER);
  }

  private static Attributes buildSpanAttributes(boolean containsAttribute) {
    if (containsAttribute) {
      return Attributes.of(AttributeKey.stringKey("original key"), "original value");
    } else {
      return Attributes.empty();
    }
  }

  private static Attributes buildMetricAttributes(boolean containsAttribute) {
    if (containsAttribute) {
      return Attributes.of(AttributeKey.stringKey("new key"), "new value");
    } else {
      return Attributes.empty();
    }
  }

  private static ReadableSpan buildReadableSpanMock(boolean isSampled, Attributes spanAttributes) {
    ReadableSpan readableSpanMock = mock(ReadableSpan.class);

    // Configure latency
    when(readableSpanMock.getLatencyNanos()).thenReturn(TEST_LATENCY_NANOS);

    // Configure isSampled
    SpanContext spanContextMock = mock(SpanContext.class);
    when(spanContextMock.isSampled()).thenReturn(isSampled);
    when(readableSpanMock.getSpanContext()).thenReturn(spanContextMock);

    // Configure attributes
    when(readableSpanMock.getAttribute(any()))
        .thenAnswer(invocation -> spanAttributes.get(invocation.getArgument(0)));

    // Configure spanData
    SpanData mockSpanData = mock(SpanData.class);
    when(mockSpanData.getAttributes()).thenReturn(spanAttributes);
    when(mockSpanData.getTotalAttributeCount()).thenReturn(spanAttributes.size());
    when(readableSpanMock.toSpanData()).thenReturn(mockSpanData);

    return readableSpanMock;
  }

  private void configureMocksForOnEnd(
      ReadableSpan readableSpanMock, boolean isEndRequired, Attributes metricAttributes) {
    // Configure isEndRequired
    when(delegateMock.isEndRequired()).thenReturn(isEndRequired);

    // Configure generated attributes
    when(generatorMock.generateMetricAttributesFromSpan(eq(readableSpanMock), eq(testResource)))
        .thenReturn(metricAttributes);
  }

  private void validateMetricsGeneratedForHttpStatusCode(
      Long httpStatusCode, ExpectedStatusMetric expectedStatusMetric) {
    Attributes spanAttributes = Attributes.of(HTTP_STATUS_CODE, httpStatusCode);
    ReadableSpan readableSpanMock = buildReadableSpanMock(IS_SAMPLED, spanAttributes);
    Attributes metricAttributes = buildMetricAttributes(CONTAINS_ATTRIBUTES);
    configureMocksForOnEnd(readableSpanMock, END_REQUIRED, metricAttributes);

    awsSpanMetricsProcessor.onEnd(readableSpanMock);
    switch (expectedStatusMetric) {
      case ERROR:
        verify(errorCounterMock, times(1)).add(eq(1L), eq(metricAttributes));
        verifyNoInteractions(faultCounterMock);
        break;
      case FAULT:
        verifyNoInteractions(errorCounterMock);
        verify(faultCounterMock, times(1)).add(eq(1L), eq(metricAttributes));
        break;
      case NEITHER:
        verifyNoInteractions(errorCounterMock);
        verifyNoInteractions(faultCounterMock);
        break;
    }

    verify(latencyHistogramMock, times(1)).record(eq(TEST_LATENCY_MILLIS), eq(metricAttributes));

    // Clear invocations so this method can be called multiple times in one test.
    clearInvocations(errorCounterMock);
    clearInvocations(faultCounterMock);
    clearInvocations(latencyHistogramMock);
  }
}
