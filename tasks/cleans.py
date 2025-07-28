"""Clean tasks for pyinvoke."""

# %% IMPORTS

from invoke.context import Context
from invoke.tasks import task

# %% TASKS

# %% - Tools


@task
def mypy(ctx: Context) -> None:
    """Clean the mypy tool."""
    ctx.run("rm -rf .mypy_cache/")


@task
def ruff(ctx: Context) -> None:
    """Clean the ruff tool."""
    ctx.run("rm -rf .ruff_cache/")


@task
def pytest(ctx: Context) -> None:
    """Clean the pytest tool."""
    ctx.run("rm -rf .pytest_cache/")
    ctx.run("rm -f report.html")


@task
def coverage(ctx: Context) -> None:
    """Clean the coverage tool."""
    ctx.run("rm -f .coverage*")


# %% - Folders


@task
def python(ctx: Context) -> None:
    """Clean python caches and bytecodes."""
    ctx.run("find . -type f -name '*.py[co]' -delete")
    ctx.run(r"find . -type d -name __pycache__ -exec rm -r {} \+")


@task
def dist(ctx: Context) -> None:
    """Clean the dist folder."""
    ctx.run("rm -f dist/*")


@task
def docs(ctx: Context) -> None:
    """Clean the docs folder."""
    ctx.run("rm -rf docs/*")


@task
def cache(ctx: Context) -> None:
    """Clean the cache folder."""
    ctx.run("rm -rf .cache/")


@task
def outputs(ctx: Context) -> None:
    """Clean the outputs folder."""
    ctx.run("rm -rf outputs/*")


@task
def prof(ctx: Context) -> None:
    """Clean the profile folder."""
    ctx.run("rm -rf prof/")


# %% - Sources


@task
def venv(ctx: Context) -> None:
    """Clean the venv folder."""
    ctx.run("poetry env remove --all")


# %% - MLFlow


@task
def mlruns(ctx: Context) -> None:
    """Clean the mlruns folder."""
    ctx.run("rm -rf mlruns/*")


# %% - Combines


@task(pre=[mypy, ruff, pytest, coverage])
def tools(_: Context) -> None:
    """Run all tools tasks."""


@task(pre=[python, dist, docs, cache, outputs, prof])
def folders(_: Context) -> None:
    """Run all folders tasks."""


@task(pre=[tools, folders], default=True)
def all(_: Context) -> None:
    """Run all tools and folders tasks."""


@task(pre=[all, venv, mlruns])
def reset(_: Context) -> None:
    """Run all tools, folders and sources tasks."""
