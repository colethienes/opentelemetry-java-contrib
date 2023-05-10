/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.contrib.awsxray;

import static io.opentelemetry.contrib.awsxray.AwsAttributeKeys.AWS_LOCAL_OPERATION;
import static io.opentelemetry.contrib.awsxray.AwsAttributeKeys.AWS_REMOTE_OPERATION;
import static io.opentelemetry.contrib.awsxray.AwsAttributeKeys.AWS_REMOTE_SERVICE;
import static io.opentelemetry.semconv.resource.attributes.ResourceAttributes.SERVICE_NAME;
import static io.opentelemetry.semconv.trace.attributes.SemanticAttributes.DB_OPERATION;
import static io.opentelemetry.semconv.trace.attributes.SemanticAttributes.DB_SYSTEM;
import static io.opentelemetry.semconv.trace.attributes.SemanticAttributes.FAAS_INVOKED_NAME;
import static io.opentelemetry.semconv.trace.attributes.SemanticAttributes.FAAS_INVOKED_PROVIDER;
import static io.opentelemetry.semconv.trace.attributes.SemanticAttributes.GRAPHQL_OPERATION_TYPE;
import static io.opentelemetry.semconv.trace.attributes.SemanticAttributes.MESSAGING_OPERATION;
import static io.opentelemetry.semconv.trace.attributes.SemanticAttributes.MESSAGING_SYSTEM;
import static io.opentelemetry.semconv.trace.attributes.SemanticAttributes.PEER_SERVICE;
import static io.opentelemetry.semconv.trace.attributes.SemanticAttributes.RPC_METHOD;
import static io.opentelemetry.semconv.trace.attributes.SemanticAttributes.RPC_SERVICE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.ReadableSpan;
import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link AwsMetricAttributeGenerator}. */
class AwsMetricAttributeGeneratorTest {

  // String constants that are used many times in these tests.
  private static final AttributeKey<String> SERVICE = AttributeKey.stringKey("Service");
  private static final AttributeKey<String> OPERATION = AttributeKey.stringKey("Operation");
  private static final AttributeKey<String> REMOTE_SERVICE =
      AttributeKey.stringKey("RemoteService");
  private static final AttributeKey<String> REMOTE_OPERATION =
      AttributeKey.stringKey("RemoteOperation");
  private static final AttributeKey<String> ORIGIN_RESOURCE_TYPE =
      AttributeKey.stringKey("OriginResourceType");
  private static final AttributeKey<String> ORIGIN_RESOURCE_ARN =
      AttributeKey.stringKey("OriginResourceArn");
  private static final AttributeKey<String> SPAN_KIND = AttributeKey.stringKey("span.kind");
  private static final String AWS_LOCAL_OPERATION_VALUE = "AWS local operation";
  private static final String AWS_REMOTE_SERVICE_VALUE = "AWS remote service";
  private static final String AWS_REMOTE_OPERATION_VALUE = "AWS remote operation";
  private static final String SERVICE_NAME_VALUE = "Service name";
  private static final String SPAN_NAME_VALUE = "Span name";
  private static final String UNKNOWN_SERVICE = "UnknownService";
  private static final String UNKNOWN_OPERATION = "UnknownOperation";
  private static final String UNKNOWN_REMOTE_SERVICE = "UnknownRemoteService";
  private static final String UNKNOWN_REMOTE_OPERATION = "UnknownRemoteOperation";
  private static final String RESOURCE_TYPE = "AWS::Resource::Type";
  private static final String RESOURCE_ARN = "arn:aws:service:us-east-2:012345678910:resource/123";

  private ReadableSpan readableSpanMock;
  private Resource resource;
  private AwsOriginDetector originDetector;
  private MetricAttributeGenerator generator;

  @BeforeEach
  public void setUpMocks() {
    readableSpanMock = mock(ReadableSpan.class);
    when(readableSpanMock.getSpanContext()).thenReturn(mock(SpanContext.class));

    resource = Resource.empty();

    originDetector = mock(AwsOriginDetector.class);
    generator = AwsMetricAttributeGenerator.create(Collections.singletonList(originDetector));
  }

  @Test
  public void testConsumerSpanWithoutAttributes() {
    Attributes expectedAttributes =
        Attributes.of(
            SPAN_KIND, SpanKind.CONSUMER.name(),
            SERVICE, UNKNOWN_SERVICE,
            OPERATION, UNKNOWN_OPERATION);
    validateAttributesProducedForSpanOfKind(expectedAttributes, SpanKind.CONSUMER);
  }

  @Test
  public void testServerSpanWithoutAttributes() {
    Attributes expectedAttributes =
        Attributes.of(
            SPAN_KIND, SpanKind.SERVER.name(),
            SERVICE, UNKNOWN_SERVICE,
            OPERATION, UNKNOWN_OPERATION);
    validateAttributesProducedForSpanOfKind(expectedAttributes, SpanKind.SERVER);
  }

  @Test
  public void testProducerSpanWithoutAttributes() {
    Attributes expectedAttributes =
        Attributes.of(
            SPAN_KIND, SpanKind.PRODUCER.name(),
            SERVICE, UNKNOWN_SERVICE,
            OPERATION, UNKNOWN_OPERATION,
            REMOTE_SERVICE, UNKNOWN_REMOTE_SERVICE,
            REMOTE_OPERATION, UNKNOWN_REMOTE_OPERATION);
    validateAttributesProducedForSpanOfKind(expectedAttributes, SpanKind.PRODUCER);
  }

  @Test
  public void testClientSpanWithoutAttributes() {
    Attributes expectedAttributes =
        Attributes.of(
            SPAN_KIND, SpanKind.CLIENT.name(),
            SERVICE, UNKNOWN_SERVICE,
            OPERATION, UNKNOWN_OPERATION,
            REMOTE_SERVICE, UNKNOWN_REMOTE_SERVICE,
            REMOTE_OPERATION, UNKNOWN_REMOTE_OPERATION);
    validateAttributesProducedForSpanOfKind(expectedAttributes, SpanKind.CLIENT);
  }

  @Test
  public void testInternalSpan() {
    // Spans with internal span kind should not produce any attributes.
    validateAttributesProducedForSpanOfKind(Attributes.empty(), SpanKind.INTERNAL);
  }

  @Test
  public void testConsumerSpanWithAttributes() {
    updateResourceWithServiceName();
    when(readableSpanMock.getName()).thenReturn(SPAN_NAME_VALUE);

    Attributes expectedAttributes =
        Attributes.of(
            SPAN_KIND, SpanKind.CONSUMER.name(),
            SERVICE, SERVICE_NAME_VALUE,
            OPERATION, SPAN_NAME_VALUE);
    validateAttributesProducedForSpanOfKind(expectedAttributes, SpanKind.CONSUMER);
  }

  @Test
  public void testServerSpanWithAttributes() {
    updateResourceWithServiceName();
    when(readableSpanMock.getName()).thenReturn(SPAN_NAME_VALUE);

    Attributes expectedAttributes =
        Attributes.of(
            SPAN_KIND, SpanKind.SERVER.name(),
            SERVICE, SERVICE_NAME_VALUE,
            OPERATION, SPAN_NAME_VALUE);
    validateAttributesProducedForSpanOfKind(expectedAttributes, SpanKind.SERVER);
  }

  @Test
  public void testServerSpanWithAttributesOrigin() {
    updateResourceWithServiceName();
    when(readableSpanMock.getName()).thenReturn(SPAN_NAME_VALUE);
    AwsOrigin origin = new AwsOrigin(RESOURCE_TYPE, RESOURCE_ARN);
    when(originDetector.detectOrigin(readableSpanMock)).thenReturn(origin);

    Attributes expectedAttributes =
        Attributes.of(
            SPAN_KIND, SpanKind.SERVER.name(),
            SERVICE, SERVICE_NAME_VALUE,
            OPERATION, SPAN_NAME_VALUE,
            ORIGIN_RESOURCE_TYPE, RESOURCE_TYPE,
            ORIGIN_RESOURCE_ARN, RESOURCE_ARN);
    validateAttributesProducedForSpanOfKind(expectedAttributes, SpanKind.SERVER);
  }

  @Test
  public void testProducerSpanWithAttributes() {
    updateResourceWithServiceName();
    mockAttribute(AWS_LOCAL_OPERATION, AWS_LOCAL_OPERATION_VALUE);
    mockAttribute(AWS_REMOTE_SERVICE, AWS_REMOTE_SERVICE_VALUE);
    mockAttribute(AWS_REMOTE_OPERATION, AWS_REMOTE_OPERATION_VALUE);

    Attributes expectedAttributes =
        Attributes.of(
            SPAN_KIND, SpanKind.PRODUCER.name(),
            SERVICE, SERVICE_NAME_VALUE,
            OPERATION, AWS_LOCAL_OPERATION_VALUE,
            REMOTE_SERVICE, AWS_REMOTE_SERVICE_VALUE,
            REMOTE_OPERATION, AWS_REMOTE_OPERATION_VALUE);
    validateAttributesProducedForSpanOfKind(expectedAttributes, SpanKind.PRODUCER);
  }

  @Test
  public void testClientSpanWithAttributes() {
    updateResourceWithServiceName();
    mockAttribute(AWS_LOCAL_OPERATION, AWS_LOCAL_OPERATION_VALUE);
    mockAttribute(AWS_REMOTE_SERVICE, AWS_REMOTE_SERVICE_VALUE);
    mockAttribute(AWS_REMOTE_OPERATION, AWS_REMOTE_OPERATION_VALUE);

    Attributes expectedAttributes =
        Attributes.of(
            SPAN_KIND, SpanKind.CLIENT.name(),
            SERVICE, SERVICE_NAME_VALUE,
            OPERATION, AWS_LOCAL_OPERATION_VALUE,
            REMOTE_SERVICE, AWS_REMOTE_SERVICE_VALUE,
            REMOTE_OPERATION, AWS_REMOTE_OPERATION_VALUE);
    validateAttributesProducedForSpanOfKind(expectedAttributes, SpanKind.CLIENT);
  }

  @Test
  public void testRemoteAttributesCombinations() {
    // Set all expected fields to a test string, we will overwrite them in descending order to test
    // the priority-order logic in AwsMetricAttributeGenerator remote attribute methods.
    mockAttribute(AWS_REMOTE_SERVICE, "TestString");
    mockAttribute(AWS_REMOTE_OPERATION, "TestString");
    mockAttribute(RPC_SERVICE, "TestString");
    mockAttribute(RPC_METHOD, "TestString");
    mockAttribute(DB_SYSTEM, "TestString");
    mockAttribute(DB_OPERATION, "TestString");
    mockAttribute(FAAS_INVOKED_PROVIDER, "TestString");
    mockAttribute(FAAS_INVOKED_NAME, "TestString");
    mockAttribute(MESSAGING_SYSTEM, "TestString");
    mockAttribute(MESSAGING_OPERATION, "TestString");
    mockAttribute(GRAPHQL_OPERATION_TYPE, "TestString");
    // Do not set dummy value for PEER_SERVICE, since it has special behaviour.

    // Two unused attributes to show that we will not make use of unrecognized attributes
    mockAttribute(AttributeKey.stringKey("unknown.service.key"), "TestString");
    mockAttribute(AttributeKey.stringKey("unknown.operation.key"), "TestString");

    // Validate behaviour of various combinations of AWS remote attributes, then remove them.
    validateAndRemoveRemoteAttributes(
        AWS_REMOTE_SERVICE,
        AWS_REMOTE_SERVICE_VALUE,
        AWS_REMOTE_OPERATION,
        AWS_REMOTE_OPERATION_VALUE);

    // Validate behaviour of various combinations of RPC attributes, then remove them.
    validateAndRemoveRemoteAttributes(RPC_SERVICE, "RPC service", RPC_METHOD, "RPC method");

    // Validate behaviour of various combinations of DB attributes, then remove them.
    validateAndRemoveRemoteAttributes(DB_SYSTEM, "DB system", DB_OPERATION, "DB operation");

    // Validate behaviour of various combinations of FAAS attributes, then remove them.
    validateAndRemoveRemoteAttributes(
        FAAS_INVOKED_PROVIDER, "FAAS invoked provider", FAAS_INVOKED_NAME, "FAAS invoked name");

    // Validate behaviour of various combinations of Messaging attributes, then remove them.
    validateAndRemoveRemoteAttributes(
        MESSAGING_SYSTEM, "Messaging system", MESSAGING_OPERATION, "Messaging operation");

    // Validate behaviour of GraphQL operation type attribute, then remove it.
    mockAttribute(GRAPHQL_OPERATION_TYPE, "GraphQL operation type");
    validateExpectedRemoteAttributes("graphql", "GraphQL operation type");
    mockAttribute(GRAPHQL_OPERATION_TYPE, null);

    // Validate behaviour of Peer service attribute, then remove it.
    mockAttribute(PEER_SERVICE, "Peer service");
    validateExpectedRemoteAttributes("Peer service", UNKNOWN_REMOTE_OPERATION);
    mockAttribute(PEER_SERVICE, null);

    // Once we have removed all usable metrics, we only have "unknown" attributes, which are unused.
    validateExpectedRemoteAttributes(UNKNOWN_REMOTE_SERVICE, UNKNOWN_REMOTE_OPERATION);
  }

  @Test
  public void testPeerServiceDoesOverrideOtherRemoteServices() {
    validatePeerServiceDoesOverride(RPC_SERVICE);
    validatePeerServiceDoesOverride(DB_SYSTEM);
    validatePeerServiceDoesOverride(FAAS_INVOKED_PROVIDER);
    validatePeerServiceDoesOverride(MESSAGING_SYSTEM);
    validatePeerServiceDoesOverride(GRAPHQL_OPERATION_TYPE);
    // Actually testing that peer service overrides "UnknownRemoteService".
    validatePeerServiceDoesOverride(AttributeKey.stringKey("unknown.service.key"));
  }

  @Test
  public void testPeerServiceDoesNotOverrideAwsRemoteService() {
    mockAttribute(AWS_REMOTE_SERVICE, "TestString");
    mockAttribute(PEER_SERVICE, "PeerService");

    when(readableSpanMock.getKind()).thenReturn(SpanKind.CLIENT);
    Attributes actualAttributes =
        generator.generateMetricAttributesFromSpan(readableSpanMock, resource);
    assertThat(actualAttributes.get(REMOTE_SERVICE)).isEqualTo("TestString");
  }

  private void mockAttribute(AttributeKey<String> key, String value) {
    when(readableSpanMock.getAttribute(key)).thenReturn(value);
  }

  private void validateAttributesProducedForSpanOfKind(
      Attributes expectedAttributes, SpanKind kind) {
    when(readableSpanMock.getKind()).thenReturn(kind);
    Attributes actualAttributes =
        generator.generateMetricAttributesFromSpan(readableSpanMock, resource);
    assertThat(actualAttributes).isEqualTo(expectedAttributes);
  }

  private void updateResourceWithServiceName() {
    resource = Resource.builder().put(SERVICE_NAME, SERVICE_NAME_VALUE).build();
  }

  private void validateExpectedRemoteAttributes(
      String expectedRemoteService, String expectedRemoteOperation) {
    when(readableSpanMock.getKind()).thenReturn(SpanKind.CLIENT);
    Attributes actualAttributes =
        generator.generateMetricAttributesFromSpan(readableSpanMock, resource);
    assertThat(actualAttributes.get(REMOTE_SERVICE)).isEqualTo(expectedRemoteService);
    assertThat(actualAttributes.get(REMOTE_OPERATION)).isEqualTo(expectedRemoteOperation);

    when(readableSpanMock.getKind()).thenReturn(SpanKind.PRODUCER);
    actualAttributes = generator.generateMetricAttributesFromSpan(readableSpanMock, resource);
    assertThat(actualAttributes.get(REMOTE_SERVICE)).isEqualTo(expectedRemoteService);
    assertThat(actualAttributes.get(REMOTE_OPERATION)).isEqualTo(expectedRemoteOperation);
  }

  private void validateAndRemoveRemoteAttributes(
      AttributeKey<String> remoteServiceKey,
      String remoteServiceValue,
      AttributeKey<String> remoteOperationKey,
      String remoteOperationValue) {
    mockAttribute(remoteServiceKey, remoteServiceValue);
    mockAttribute(remoteOperationKey, remoteOperationValue);
    validateExpectedRemoteAttributes(remoteServiceValue, remoteOperationValue);

    mockAttribute(remoteServiceKey, null);
    mockAttribute(remoteOperationKey, remoteOperationValue);
    validateExpectedRemoteAttributes(UNKNOWN_REMOTE_SERVICE, remoteOperationValue);

    mockAttribute(remoteServiceKey, remoteServiceValue);
    mockAttribute(remoteOperationKey, null);
    validateExpectedRemoteAttributes(remoteServiceValue, UNKNOWN_REMOTE_OPERATION);

    mockAttribute(remoteServiceKey, null);
    mockAttribute(remoteOperationKey, null);
  }

  private void validatePeerServiceDoesOverride(AttributeKey<String> remoteServiceKey) {
    mockAttribute(remoteServiceKey, "TestString");
    mockAttribute(PEER_SERVICE, "PeerService");

    // Validate that peer service value takes precedence over whatever remoteServiceKey was set
    when(readableSpanMock.getKind()).thenReturn(SpanKind.CLIENT);
    Attributes actualAttributes =
        generator.generateMetricAttributesFromSpan(readableSpanMock, resource);
    assertThat(actualAttributes.get(REMOTE_SERVICE)).isEqualTo("PeerService");

    mockAttribute(remoteServiceKey, null);
    mockAttribute(PEER_SERVICE, null);
  }
}
