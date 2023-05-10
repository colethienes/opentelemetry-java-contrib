package io.opentelemetry.contrib.awsxray;


final class AwsOrigin {

  private final String resourceType;
  private final String resourceArn;

  AwsOrigin(String resourceType, String resourceArn) {
    this.resourceType = resourceType;
    this.resourceArn = resourceArn;
  }

  public String getResourceType() {
    return resourceType;
  }

  public String getResourceArn() {
    return resourceArn;
  }
}
