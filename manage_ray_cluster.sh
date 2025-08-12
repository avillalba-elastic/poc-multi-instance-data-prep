# Build and push to ECR the dev Docker image
docker buildx bake development-image --push

# Start the Ray cluster
AWS_PROFILE=ml poetry run ray up -y ray_cluster/config.yaml

# Rsync to sync your local project with the remote project


# Tear down the cluster
AWS_PROFILE=ml poetry run ray down -y ray_cluster/config.yaml
