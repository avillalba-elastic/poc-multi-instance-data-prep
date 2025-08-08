rm -r dist/
poetry build --format wheel

docker buildx bake --push
