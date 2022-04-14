from articat.artifact import ID, Metadata
from articat.meta_artifact import MetaArtifact
from articat.tests.utils import TestCatalog, TestFSArtifact


def test_meta_artifact__happy_path(uid: ID) -> None:
    with TestFSArtifact.dummy_versioned_ctx(f"{uid}-dep", "0.1.0") as fsa:
        fsa.metadata.arbitrary["blah"] = 3.14
    with MetaArtifact.partitioned(uid) as ma:
        ma = ma.record_deps(fsa)
        ma.metadata.arbitrary["blah"] = 42

    a = TestCatalog.get(uid, model=MetaArtifact)
    assert a.metadata.arbitrary["blah"] == 42
    deps = a.deps()
    assert len(deps) == 1
    d0 = deps[0]
    assert d0.metadata == Metadata()
    deps = a.deps(fetch=True)
    assert len(deps) == 1
    d0 = deps[0]
    assert d0.metadata.arbitrary["blah"] == 3.14
    assert a.metadata.arbitrary["blah"] == 42


def test_meta_artifact__empty_browser_url(uid) -> None:
    a = MetaArtifact.partitioned(uid)
    assert a.browser_url() == ""
    assert not a.browser_url()
