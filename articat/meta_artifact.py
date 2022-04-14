from typing import Optional

from articat import Artifact
from articat.catalog import Catalog


class MetaArtifact(Artifact):
    """Metadata only Artifact. Primary use case is to aggregate information about multiple artifacts."""

    def deps(
        self, fetch: bool = False, catalog: Optional[type[Catalog]] = None
    ) -> list[Artifact]:
        """Returns list of dependencies of this MarkerArtifact"""
        deps = [
            Artifact(id=d["id"], partition=d["partition"], version=d["version"])
            for d in self.metadata.arbitrary["deps"]
        ]
        return deps if not fetch else [d.fetch(catalog=catalog) for d in deps]

    def browser_url(self) -> str:
        """MetaArtifact doesn't really have a proper URL website to open"""
        return ""

    def build(self) -> "Artifact":
        return self
