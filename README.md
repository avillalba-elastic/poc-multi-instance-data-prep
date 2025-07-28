# PoC multi instance data preparation

[![ci.yml](https://github.com/avillalba-elastic/poc-multi-instance-data-preparation/actions/workflows/ci.yml/badge.svg)](https://github.com/avillalba-elastic/poc-multi-instance-data-preparation/actions/workflows/ci.yml)

Testing processing data at scale in AWS.

# Installation

We use the package manager [Poetry](https://python-poetry.org/). Follow [these instructions](https://python-poetry.org/docs/#installation) to install it:
```bash
pipx install poetry==2.1.1
```

Install the virtual environment:
```bash
poetry install
```

:warning: To install internal dependencies, authentication to JFrog Artifactory is required:
```bash
poetry config http-basic.artifactory-virtual-python <USER> <PASSWORD/TOKEN>
```
[Here](https://jfrog.com/help/r/how-to-generate-an-access-token-video/artifactory-creating-access-tokens-in-artifactory) is the official documentation explaining how to create your user access token.

Install [pre-commit hooks](https://pre-commit.com/):

```bash
poetry run invoke installs.pre-commit
```

# Tests

We use [pytest](https://docs.pytest.org/en/stable/) and [nox](https://nox.thea.codes/en/stable/) to run the tests:

To run the tests with the default Python and dependency versions:

```bash
poetry run invoke testing.test-default-versions
```

To run the tests with the dependency versions matrix:

```bash
poetry run invoke testing.test-matrix-versions
```

# Relevant links
- [SonarQube project](https://sonar.elastic.dev/dashboard?id=)
