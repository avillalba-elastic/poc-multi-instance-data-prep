group "default" {
    targets = ["standard-image", "pyspark-standard-image"]
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

target "pyspark-standard-image" {
    description = "Standard image based on Pyspark"
    context = "."
    dockerfile = "Dockerfile"
    args = {
        # See: https://github.com/aws/sagemaker-spark-container/blob/master/available_images.md
        base_image = "173754725891.dkr.ecr.us-east-1.amazonaws.com/sagemaker-spark-processing:3.5-cpu-py312-v1.0"
    }
    tags = [
        "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${USE_CASE}/pyspark-standard-image:${IMAGE_VERSION}",
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
