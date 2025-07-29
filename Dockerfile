# See Sagemaker DLC images: https://github.com/aws/deep-learning-containers/blob/master/available_images.md
ARG base_image=python:3.12-slim
FROM ${base_image}

ARG APP_USER_UID=1001
ARG APP_USER_GID=1001

RUN groupadd --gid ${APP_USER_GID} appgroup && \
    useradd --uid ${APP_USER_UID} --gid ${APP_USER_GID} --shell /sbin/nologin --create-home appuser

RUN mkdir -p /opt/app && chown -R ${APP_USER_UID}:${APP_USER_GID} /opt/app
WORKDIR /opt/app

COPY --chown=appuser:appgroup dist/*.whl .

RUN pip install --no-cache-dir --upgrade pip
RUN sh -c 'pip install --no-cache-dir \
        ./*.whl && rm ./*.whl'

USER appuser
