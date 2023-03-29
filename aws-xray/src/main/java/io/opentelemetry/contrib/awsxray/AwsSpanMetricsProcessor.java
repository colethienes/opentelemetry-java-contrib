/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.contrib.awsxray;

import static io.opentelemetry.semconv.trace.attributes.SemanticAttributes.HTTP_STATUS_CODE;
import static java.util.Objects.requireNonNull;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.ReadWriteSpan;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.data.DelegatingSpanData;
import io.opentelemetry.sdk.trace.data.SpanData;
import javax.annotation.concurrent.Immutable;
import org.jetbrains.annotations.Nullable;

/**
 * This processor will generate metrics based on span data. It depends on a {@link
 * MetricAttributeGenerator} being provided on instantiation, which will provide a means to
 * determine attributes which should be used to create metrics. Also, a delegate {@link
 * SpanProcessor} must be provided, which will be used to further process and export spans. A {@link
 * Resource} must also be provided, which is used to generate metrics. Finally, two {@link
 * LongCounter}'s and a {@link DoubleHistogram} must be provided, which will be used to actually
 * create desired metrics (see below)
 *
 * <p>AwsSpanMetricsProcessor produces metrics for errors (e.g. HTTP 4XX status codes), faults (e.g.
 * HTTP 5XX status codes), and latency (in Milliseconds). Errors and faults are counted, while
 * latency is measured with a histogram. Metrics are emitted with attributes derived from span
 * attributes, and these derived attributes will also be added to the span passed to the delegate
 * SpanProcessor.
 *
 * <p>For highest fidelity metrics, this processor should be coupled with the {@link
 * AlwaysRecordSampler}, which will result in 100% of spans being sent to the processor.
 */
@Immutable
public final class AwsSpanMetricsProcessor implements SpanProcessor {

  private static final double NANOS_TO_MILLIS = 1_000_000.0;

  // Constants for deriving error and fault metrics
  private static final int ERROR_CODE_LOWER_BOUND = 400;
  private static final int ERROR_CODE_UPPER_BOUND = 499;
  private static final int FAULT_CODE_LOWER_BOUND = 500;
  private static final int FAULT_CODE_UPPER_BOUND = 599;

  // Metric instruments
  private final LongCounter errorCounter;
  private final LongCounter faultCounter;
  private final DoubleHistogram latencyHistogram;

  private final MetricAttributeGenerator generator;
  private final SpanProcessor delegate;
  private final Resource resource;

  public static AwsSpanMetricsProcessor create(
      LongCounter errorCounter,
      LongCounter faultCounter,
      DoubleHistogram latencyHistogram,
      MetricAttributeGenerator generator,
      SpanProcessor delegate,
      Resource resource) {
    return new AwsSpanMetricsProcessor(
        errorCounter, faultCounter, latencyHistogram, generator, delegate, resource);
  }

  private AwsSpanMetricsProcessor(
      LongCounter errorCounter,
      LongCounter faultCounter,
      DoubleHistogram latencyHistogram,
      MetricAttributeGenerator generator,
      SpanProcessor delegate,
      Resource resource) {
    this.errorCounter = errorCounter;
    this.faultCounter = faultCounter;
    this.latencyHistogram = latencyHistogram;
    this.generator = generator;
    this.delegate = delegate;
    this.resource = resource;
  }

  @Override
  public void onStart(Context parentContext, ReadWriteSpan span) {
    delegate.onStart(parentContext, span);
  }

  @Override
  public boolean isStartRequired() {
    return delegate.isStartRequired();
  }

  @Override
  public void onEnd(ReadableSpan span) {
    Attributes attributes = generator.generateMetricAttributesFromSpan(span, resource);

    // Only record metrics if non-empty attributes are returned.
    if (!attributes.isEmpty()) {
      recordErrorOrFault(span, attributes);
      recordLatency(span, attributes);
    }

    if (delegate.isEndRequired()) {
      // Only wrap the span if we need to (i.e. if it will be exported and if there is anything to
      // wrap it with).
      if (span.getSpanContext().isSampled() && !attributes.isEmpty()) {
        span = wrapSpanWithAttributes(span, attributes);
      }
      delegate.onEnd(span);
    }
  }

  @Override
  public boolean isEndRequired() {
    return true;
  }

  @Override
  public CompletableResultCode shutdown() {
    return delegate.shutdown();
  }

  @Override
  public CompletableResultCode forceFlush() {
    return delegate.forceFlush();
  }

  @Override
  public void close() {
    delegate.close();
  }

  private void recordErrorOrFault(ReadableSpan span, Attributes attributes) {
    Long httpStatusCode = span.getAttribute(HTTP_STATUS_CODE);
    if (httpStatusCode == null) {
      return;
    }

    if (httpStatusCode >= ERROR_CODE_LOWER_BOUND && httpStatusCode <= ERROR_CODE_UPPER_BOUND) {
      errorCounter.add(1, attributes);
    } else if (httpStatusCode >= FAULT_CODE_LOWER_BOUND
        && httpStatusCode <= FAULT_CODE_UPPER_BOUND) {
      faultCounter.add(1, attributes);
    }
  }

  private void recordLatency(ReadableSpan span, Attributes attributes) {
    long nanos = span.getLatencyNanos();
    double millis = nanos / NANOS_TO_MILLIS;
    latencyHistogram.record(millis, attributes);
  }

  /**
   * {@link #onEnd} works with a {@link ReadableSpan}, which does not permit modification. However,
   * we need to add derived metric attributes to the span for downstream metric to span correlation.
   * To work around this, we will wrap the ReadableSpan with a {@link DelegatingReadableSpan} that
   * simply passes through all API calls, except for those pertaining to Attributes, i.e {@link
   * ReadableSpan#toSpanData()} and {@link ReadableSpan#getAttribute} APIs.
   *
   * <p>Note that this approach relies on {@link DelegatingSpanData} to wrap toSpanData.
   * Unfortunately, no such wrapper appears to exist for ReadableSpan, so we use a new inner class,
   * {@link DelegatingReadableSpan}.
   *
   * <p>See https://github.com/open-telemetry/opentelemetry-specification/issues/1089 for more
   * context on this approach.
   */
  private ReadableSpan wrapSpanWithAttributes(ReadableSpan span, Attributes metricAttributes) {
    SpanData spanData = span.toSpanData();
    Attributes originalAttributes = spanData.getAttributes();
    Attributes replacementAttributes =
        originalAttributes.toBuilder().putAll(metricAttributes).build();

    int originalTotalAttributeCount = spanData.getTotalAttributeCount();
    int replacementTotalAttributeCount = originalTotalAttributeCount + metricAttributes.size();

    return new DelegatingReadableSpan(span) {
      @Override
      public SpanData toSpanData() {
        return new DelegatingSpanData(spanData) {
          @Override
          public Attributes getAttributes() {
            return replacementAttributes;
          }

          @Override
          public int getTotalAttributeCount() {
            return replacementTotalAttributeCount;
          }
        };
      }

      @Nullable
      @Override
      public <T> T getAttribute(AttributeKey<T> key) {
        T attributeValue = span.getAttribute(key);
        if (attributeValue != null) {
          return attributeValue;
        } else {
          return metricAttributes.get(key);
        }
      }
    };
  }

  /**
   * A {@link ReadableSpan} which delegates all methods to another {@link ReadableSpan}. We extend
   * this class to modify the {@link ReadableSpan} that will be processed by the {@link #delegate}.
   */
  private class DelegatingReadableSpan implements ReadableSpan {
    private final ReadableSpan delegate;

    protected DelegatingReadableSpan(ReadableSpan delegate) {
      this.delegate = requireNonNull(delegate, "delegate");
    }

    @Override
    public SpanContext getSpanContext() {
      return delegate.getSpanContext();
    }

    @Override
    public SpanContext getParentSpanContext() {
      return delegate.getParentSpanContext();
    }

    @Override
    public String getName() {
      return delegate.getName();
    }

    @Override
    public SpanData toSpanData() {
      return delegate.toSpanData();
    }

    @Override
    @Deprecated
    public io.opentelemetry.sdk.common.InstrumentationLibraryInfo getInstrumentationLibraryInfo() {
      return delegate.getInstrumentationLibraryInfo();
    }

    @Override
    public InstrumentationScopeInfo getInstrumentationScopeInfo() {
      return delegate.getInstrumentationScopeInfo();
    }

    @Override
    public boolean hasEnded() {
      return delegate.hasEnded();
    }

    @Override
    public long getLatencyNanos() {
      return delegate.getLatencyNanos();
    }

    @Override
    public SpanKind getKind() {
      return delegate.getKind();
    }

    @Nullable
    @Override
    public <T> T getAttribute(AttributeKey<T> key) {
      return delegate.getAttribute(key);
    }
  }
}
