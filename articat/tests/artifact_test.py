from datetime import date, datetime

import fsspec
import pytest
from _pytest.monkeypatch import MonkeyPatch
from dateutil.tz import UTC

from articat.artifact import ID, Artifact, Metadata
from articat.fs_artifact import FSArtifact
from articat.tests.utils import (
    TestCatalog,
    TestFSArtifact,
    write_a_couple_of_partitions,
)


def test_large_arbitrary_entry(uid: ID) -> None:
    more_than_1500 = "f" * 2000
    with TestFSArtifact.dummy_versioned_ctx(uid, "0.1.0") as a:
        a.metadata.arbitrary.update(dict(foo=more_than_1500))

    a = TestCatalog.get(uid, version="0.1.0", model=TestFSArtifact)
    assert a.metadata.arbitrary.get("foo") == more_than_1500


def test_artifact_parse_extra_prop(uid: ID) -> None:
    TestFSArtifact.write_dummy_partitioned(uid, date.today())
    a_back = TestCatalog.get(uid, partition=date.today(), model=Artifact)
    assert getattr(a_back, "files_pattern")


def test_write_to_prod_on_non_prod_env_fails(uid: ID, monkeypatch: MonkeyPatch) -> None:
    with monkeypatch.context() as m:
        m.delattr(TestFSArtifact, "__test__")
        with pytest.raises(
            ValueError, match="your environment is missing ARTICAT_PROD"
        ):
            TestFSArtifact.write_dummy_versioned(uid, "0.1.0")


def test_catalog_deps__happy_path(uid: ID) -> None:
    dep1 = TestFSArtifact.partitioned(
        "dep1", partition=datetime.today().replace(tzinfo=UTC)
    )
    dep2 = TestFSArtifact.versioned("dep2", version="0.1.1")
    with TestFSArtifact.dummy_versioned_ctx(uid, "0.1.0") as a:
        a.record_deps(dep1, dep2)

    a = TestCatalog.latest_partition(uid, model=TestFSArtifact)
    deps = a.metadata.arbitrary.get("deps")
    assert isinstance(deps, list)
    assert len(deps) == 2
    assert TestFSArtifact.spec(dep1) in deps
    assert TestFSArtifact.spec(dep2) in deps


def test_catalog_deps__dup_dep(uid: ID) -> None:
    dep1 = TestFSArtifact.partitioned(
        "dep1", partition=datetime.today().replace(tzinfo=UTC)
    )
    dep2 = TestFSArtifact.versioned("dep2", version="0.1.1")
    with TestFSArtifact.dummy_versioned_ctx(uid, "0.1.0") as a:
        a.record_deps(dep1)
        a.record_deps(dep2)
        # NOTE: if the user specifies the same dep multiple times
        #       it will end up in the deps multiple times
        a.record_deps(dep1)

    a = TestCatalog.latest_partition(uid, model=TestFSArtifact)
    deps = a.metadata.arbitrary.get("deps")
    assert isinstance(deps, list)
    assert len(deps) == 3
    assert TestFSArtifact.spec(dep1) in deps
    assert TestFSArtifact.spec(dep2) in deps


def test_catalog_fails_on_preexisting_manifest_file(uid: ID) -> None:
    with pytest.raises(
        ValueError, match="Manifest file `_MANIFEST.json` already present"
    ):
        with TestFSArtifact.dummy_versioned_ctx(uid, "0.1.0") as a:
            with fsspec.open(f"{a.staging_file_prefix}/_MANIFEST.json", "w") as f:
                f.write("foobar")
            a.files_pattern = None


def test_dev_mode(uid: ID) -> None:
    write_a_couple_of_partitions(f"_dev_{uid}", 1)
    r = TestCatalog.get(uid, partition=date.today(), dev=True, model=FSArtifact)
    assert TestFSArtifact.config().fs_dev_prefix() in (r.files_pattern or "")
    assert "final_output" not in (r.files_pattern or "final_output")


def test_dev_mode_in_out(uid: ID) -> None:
    TestFSArtifact.write_dummy_versioned(uid, "0.1.0", dev=True)
    with pytest.raises(ValueError, match="Can't find requested artifact"):
        TestCatalog.get(uid, version="0.1.0")
    assert TestCatalog.get(uid, version="0.1.0", dev=True) is not None


def test_dev_mode_dont_dup_prefix(uid: ID) -> None:
    TestFSArtifact.write_dummy_versioned(f"_dev_{uid}", "0.1.0", dev=True)
    assert TestCatalog.get(f"_dev_{uid}", version="0.1.0") is not None
    assert TestCatalog.get(uid, version="0.1.0", dev=True) is not None
    assert TestCatalog.get(f"_dev_{uid}", version="0.1.0", dev=True) is not None


def test_fails_before_file_copy_on_invalid_types_in_metadata(uid: ID) -> None:
    with pytest.raises(ValueError, match="Unknown protobuf attr"):
        with TestFSArtifact.partitioned(uid) as a:
            a.metadata = Metadata(arbitrary={"a": date.today()})


def test_metadata_supports_embedded_dicts_lists(uid: ID) -> None:
    today = date.today()
    nested_metadata = {"a": {"foo": "bar", "baz": [1, 2, 3]}}
    with TestFSArtifact.dummy_partitioned_ctx(uid, today) as a:
        a.metadata = Metadata(arbitrary={"a": {"foo": "bar", "baz": [1, 2, 3]}})

    roundback_metadata = TestCatalog.get(uid, partition=today).metadata
    assert roundback_metadata and roundback_metadata.arbitrary == nested_metadata
