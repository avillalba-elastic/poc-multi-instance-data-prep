"""
Assumes that we have an active virtual environment with default versions for
Python and dependencies. Run this function with `poetry run nox --no-venv`
to reuse them. This saves time and space.
"""

import nox


@nox.session
def test_default_versions(session: nox.Session) -> None:
    """Tests the project with the default versions of the dependencies."""
    session.run(
        "pytest",
        "--durations=0",
        "--cov=src",
        "--cov-report=xml:coverage.xml",
        "-n",
        "auto",
        "--profile",
        "--random-order",
        "tests/",
    )


# NOTE: Modify here the matrix to test, this is just an example:
@nox.session
@nox.parametrize("transformers", ["4.46.3", "4.39.3", "4.41.0", "4.37.0"])
@nox.parametrize("torch", ["2.4.1", "2.5.1"])
def test_multiple_versions(session: nox.Session, transformers: str, torch: str) -> None:
    """Tests the project with multiple versions of transformers and torch.

    Args:
        session (nox.Session): The Nox session object.
        transformers (str): Version of transformers.
        torch (str): Version of torch.
    """

    # Overwrite the virtual environment with the versions we want to test.
    session.run("pip", "install", f"transformers=={transformers}")
    session.run("pip", "install", f"torch=={torch}")

    session.run(
        "pytest",
        "--durations=0",
        "--cov=src",
        "--cov-report=xml:coverage.xml",
        "-n",
        "auto",
        "--profile",
        "--random-order",
        "tests/",
    )
