"""Testing tasks for pyinvoke."""

# %% IMPORTS

from invoke.context import Context
from invoke.tasks import task

# %% TASKS


@task
def test_default_versions(ctx: Context) -> None:
    """Run the tests with the default versions of the dependencies. Uses the existing virtual environment."""
    ctx.run("poetry run nox --no-venv -e test_default_versions")


@task
def test_matrix_versions(ctx: Context) -> None:
    """
    Run the nox tests that assess the compatibility matrix of the dependencies.
    Uses the existing virtual environment.
    """
    ctx.run("poetry run nox --no-venv -e test_multiple_versions")


@task(pre=[test_default_versions, test_matrix_versions], default=True)
def all(_: Context) -> None:
    """Run all testing tasks."""
