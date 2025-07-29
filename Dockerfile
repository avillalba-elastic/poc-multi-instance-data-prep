# See Sagemaker DLC images: https://github.com/aws/deep-learning-containers/blob/master/available_images.md
ARG base_image=python:3.12-slim
FROM ${base_image}

RUN mkdir -p /opt/appgroup
WORKDIR /opt/app

COPY dist/*.whl .

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir ./*.whl && rm ./*.whl
