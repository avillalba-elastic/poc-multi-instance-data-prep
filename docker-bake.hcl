group "default" {
    targets = ["standard-image"]
}

# AWS Variables
variable "AWS_ACCOUNT_ID" {
    default = "879381254630"  # ml team account
}

variable "AWS_REGION" {
    default = "us-east-1"
}

# Docker variables
variable "USE_CASE" {
    default = "poc-multi-instance-data-prep"
}

variable "IMAGE_VERSION" {
    default = "latest"  # can be set dynamically
}

# Targets to build
target "standard-image" {
    description = "Standard image based on Python"
    context = "."
    dockerfile = "Dockerfile"
    tags = [
        "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${USE_CASE}/standard-image:${IMAGE_VERSION}",
    ]
    platforms = [
        "linux/amd64"
    ]
}

target "development-image" {
    description = "Development image based on Python"
    context = "."
    dockerfile = "ray_cluster/Dockerfile"
    tags = [
        "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${USE_CASE}/poc-multi-instance-data-prep-dev-image:${IMAGE_VERSION}",
    ]
    platforms = [
        "linux/amd64"
    ]
}
