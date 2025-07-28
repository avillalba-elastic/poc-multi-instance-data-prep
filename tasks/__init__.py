"""Task collections for the project."""

# mypy: ignore-errors

# %% IMPORTS

from invoke.collection import Collection

from . import (
    checks,
    cleans,
    commits,
    containers,
    docs,
    formats,
    installs,
    mlflow,
    packages,
    projects,
    testing,
)

# %% NAMESPACES

ns = Collection()

# %% COLLECTIONS

ns.add_collection(Collection.from_module(checks))
ns.add_collection(Collection.from_module(cleans))
ns.add_collection(Collection.from_module(commits))
ns.add_collection(Collection.from_module(containers))
ns.add_collection(Collection.from_module(docs))
ns.add_collection(Collection.from_module(formats))
ns.add_collection(Collection.from_module(installs))
ns.add_collection(Collection.from_module(mlflow))
ns.add_collection(Collection.from_module(packages))
ns.add_collection(Collection.from_module(projects), default=True)
ns.add_collection(Collection.from_module(testing))
