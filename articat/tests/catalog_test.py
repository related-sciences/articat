import inspect
import re
import tempfile
from datetime import date, datetime, time, timedelta
from pathlib import Path
from random import shuffle
from typing import List, cast

import fsspec
import pytest
from _pytest.monkeypatch import MonkeyPatch
from dateutil.tz import UTC

# Tests in this module require Datastore emulator, and by default won't run
# unless pytest has been told that the emulator is in fact available
# via `--datastore_emulated`. Out Github CI has Datastore emulator enabled.
# To run these tests locally:
# docker run --rm -p 8099:8099 ravwojdyla/datastore_emulator:0.1.0
# pytest --datastore_emulated
from articat.artifact import ID, Artifact, Metadata
from articat.fs_artifact import FSArtifact
from articat.tests.utils import TestCatalog, TestFSArtifact
from articat.utils.path_utils import get_root_path
from articat.utils.utils import get_relative_call_site

pytestmark = pytest.mark.datastore_emulated


def test_simple_happy_path_catalog(uid: ID) -> None:
    yesterday = datetime.utcnow() - timedelta(days=1)
    today = datetime.utcnow()
    TestFSArtifact.write_dummy_partitioned(uid, yesterday)
    TestFSArtifact.write_dummy_partitioned(uid, today)

    r = TestCatalog.get(uid, partition=yesterday)
    if r.files_pattern is None:
        pytest.fail("files_pattern should be set")
    else:
        assert TestFSArtifact.config.fs_prod_prefix in r.files_pattern
        assert "ala" in Path(r.files_pattern).read_text()
        assert Path(r.files_pattern).parent.joinpath("_SUCCESS").exists()
        manifest_path = Path(r.files_pattern).parent.joinpath("_MANIFEST.json")
        assert manifest_path.exists()
        manifest = FSArtifact.parse_file(manifest_path)
        # Datastore adds tzinfo, which we don't have
        manifest.partition = manifest.partition.replace(tzinfo=UTC)
        manifest.created = manifest.created.replace(tzinfo=UTC)
        assert manifest == r
        assert (
            manifest.metadata.arbitrary.get("call_site_relfname")
            == Path(inspect.getfile(TestFSArtifact))
            .relative_to(get_root_path())
            .as_posix()
        )
        assert manifest.metadata.arbitrary.get("call_site_lineno") > 1

    assert len(list(TestCatalog.lookup(uid))) == 2
    assert len(list(TestCatalog.lookup(uid, partition_dt_start=today))) == 1
    assert len(list(TestCatalog.lookup(uid, partition_dt_start=yesterday))) == 2
    # end is not inclusive
    assert len(list(TestCatalog.lookup(uid, partition_dt_end=today))) == 1


def test_catalog_date_partition(uid: ID) -> None:
    dt = date.today()
    TestFSArtifact.write_dummy_partitioned(uid, dt)
    assert TestCatalog.get(uid, partition=dt).id == uid


def test_fail_on__missing_output(uid: ID) -> None:
    with pytest.raises(ValueError, match="There are no files in the output"):
        with TestFSArtifact.partitioned(uid) as art:
            out_dir = Path(tempfile.mkdtemp())
            art.files_pattern = f"{out_dir.as_posix()}/*"


def test_publish_file_works_for_local() -> None:
    fs = fsspec.filesystem("file")
    prefix = f"{tempfile.mkdtemp()}"
    fs.touch(f"{prefix}/sth.txt")
    dst_prefix = f"{tempfile.mkdtemp()}"
    TestFSArtifact._publish_file(fs, f"{prefix}/sth.txt", prefix, dst_prefix)
    assert Path(f"{dst_prefix}/sth.txt").exists()


def test_fs_scheme_regex() -> None:
    assert (
        re.sub(TestFSArtifact._fs_scheme_regex, "", "gs://test-sth/foobar")
        == "test-sth/foobar"
    )
    assert (
        re.sub(TestFSArtifact._fs_scheme_regex, "", "gs://test-sth/foobar://fds[.*]")
        == "test-sth/foobar://fds[.*]"
    )


def write_a_couple_of_partitions(uid: ID, n: int) -> None:
    # we shuffle the partitions to further tests ordering etc
    ns = list(range(n))
    shuffle(ns)
    for i in ns:
        dt = date.today() - timedelta(days=i)
        TestFSArtifact.write_dummy_partitioned(uid, dt)
        # write some unrelated dataset
        TestFSArtifact.write_dummy_partitioned(f"{uid}1234", dt)


def test_catalog_lookup__ordered_partitions(uid: ID) -> None:
    write_a_couple_of_partitions(uid, 10)
    # should return exactly 3 results ordered desc
    _1, _2, _3 = tuple(TestCatalog.lookup(uid, partition_dt_start=date.min, limit=3))
    today = datetime.combine(date.today(), time.min).replace(tzinfo=UTC)
    assert _1.partition == today
    assert _2.partition == today - timedelta(days=1)
    assert _3.partition == today - timedelta(days=2)

    all_parts = tuple(TestCatalog.lookup(uid, partition_dt_start=date.min))
    p = [x.partition for x in all_parts]
    assert len(p) == 10
    assert sorted(cast(List[date], p), reverse=True) == p


def test_catalog_lookup__limit_works(uid: ID) -> None:
    write_a_couple_of_partitions(uid, 3)
    # should return exactly 3 results ordered desc
    (_1,) = tuple(TestCatalog.lookup(uid, limit=1, partition_dt_start=date.min))
    today = datetime.combine(date.today(), time.min).replace(tzinfo=UTC)
    assert _1.partition == today


def test_catalog_lookup__partition_limits(uid: ID) -> None:
    write_a_couple_of_partitions(uid, 5)
    today = datetime.combine(date.today(), time.min).replace(tzinfo=UTC)
    today_m_2 = today - timedelta(days=2)
    today_m_4 = today - timedelta(days=4)
    _1, _2 = tuple(
        TestCatalog.lookup(
            uid, partition_dt_start=today_m_4, partition_dt_end=today_m_2
        )
    )
    assert _1.partition == today_m_4 + timedelta(days=1)
    assert _2.partition == today_m_4

    assert (
        len(
            list(
                TestCatalog.lookup(
                    uid, partition_dt_start=today_m_2, partition_dt_end=today_m_4
                )
            )
        )
        == 0
    )
    assert len(list(TestCatalog.lookup(uid, partition_dt_start=date.min))) == 5
    assert (
        len(
            list(
                TestCatalog.lookup(
                    uid, partition_dt_start=date.min, partition_dt_end=today_m_2
                )
            )
        )
        == 2
    )
    assert len(list(TestCatalog.lookup(uid, partition_dt_end=today_m_2))) == 2
    assert len(list(TestCatalog.lookup(uid, partition_dt_end=today_m_4))) == 0


def test_catalog_lookup__all(uid: ID) -> None:
    write_a_couple_of_partitions(uid, 2)
    assert len(list(TestCatalog.lookup())) >= 2


def test_catalog_lookup__metadata(uid: ID) -> None:
    write_a_couple_of_partitions(f"{uid}foobar", 2)
    with TestFSArtifact.dummy_versioned_ctx(uid, "0.1.2") as a:
        a.metadata = Metadata(
            arbitrary={"a": "foo"}, schema_fields=["foo", "bar", "baz"]
        )

    with TestFSArtifact.dummy_versioned_ctx(uid, "0.1.1") as a:
        a.metadata = Metadata(
            arbitrary={"a": "foo", "b": "bar"},
            schema_fields=["foo", "bar"],
        )

    with TestFSArtifact.dummy_versioned_ctx(uid, "0.1.0") as a:
        a.metadata.arbitrary.update({"a": "foo"})
        a.metadata.schema_fields = ["foo", "bar"]

    assert len(list(TestCatalog.lookup(uid))) == 3
    (_1,) = tuple(TestCatalog.lookup(uid, version="0.1.0"))
    assert _1.metadata and _1.version == "0.1.0"
    assert "git_repo_url" in _1.metadata.arbitrary
    assert "git_head_hash" in _1.metadata.arbitrary
    assert (
        len(tuple(TestCatalog.lookup(uid, metadata=Metadata(schema_fields=["bar"]))))
        == 3
    )
    (_1,) = tuple(TestCatalog.lookup(uid, metadata=Metadata(schema_fields=["baz"])))
    assert _1.metadata and _1.metadata.schema_fields == ["foo", "bar", "baz"]
    assert _1.metadata and _1.version == "0.1.2"
    # there is no version 0.1.0 with baz field
    assert (
        len(
            tuple(
                TestCatalog.lookup(
                    uid, version="0.1.0", metadata=Metadata(schema_fields=["baz"])
                )
            )
        )
        == 0
    )
    assert len(tuple(TestCatalog.lookup(uid, version="0.1.3"))) == 0
    with pytest.raises(ValueError, match="Arbitrary lookup not supported"):
        tuple(TestCatalog.lookup(uid, metadata=Metadata(arbitrary={"a": "foo"})))


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


def test_catalog_lookup__unordered_when_no_partition_requested(uid: ID) -> None:
    write_a_couple_of_partitions(uid, 10)
    # no partition requested:
    all_parts = tuple(TestCatalog.lookup(uid))
    p = [x.partition for x in all_parts]
    assert len(p) == 10
    assert sorted(cast(List[date], p), reverse=True) != p


def test_catalog_lookup__cant_lookup_by_both_partition_and_metadata(uid: ID) -> None:
    with pytest.raises(
        ValueError, match="Can't lookup based on both metadata and partition"
    ):
        list(
            TestCatalog.lookup(
                id=uid, partition_dt_start=date.today(), metadata=Metadata()
            )
        )


def test_catalog_get__latest_no_part_ver(uid: ID) -> None:
    write_a_couple_of_partitions(uid, 3)
    r = TestCatalog.get(uid)
    assert isinstance(r.partition, datetime)
    assert r.partition.date() == date.today()


def test_catalog_lookup__cant_lookup_by_description_or_spark_schema(uid: ID) -> None:
    with pytest.raises(ValueError, match="Description lookup not supported"):
        list(TestCatalog.lookup(id=uid, metadata=Metadata(description="blah")))
    with pytest.raises(ValueError, match="Spark schema lookup not supported"):
        list(TestCatalog.lookup(id=uid, metadata=Metadata(spark_schema="blah")))


def test_dev_mode(uid: ID) -> None:
    write_a_couple_of_partitions(f"_dev_{uid}", 1)
    r = TestCatalog.get(uid, partition=date.today(), dev=True)
    assert TestFSArtifact.config.fs_dev_prefix in (r.files_pattern or "")
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


def test_catalog_no_files_pattern_includes_everything(uid: ID) -> None:
    with TestFSArtifact.dummy_versioned_ctx(uid, "0.1.0") as a:
        a.files_pattern = None
    output = TestCatalog.get(uid, version="0.1.0").files_pattern
    assert output is not None
    assert output.endswith("/**")


def test_dev_mode_can_overwrite(uid: ID) -> None:
    TestFSArtifact.write_dummy_versioned(uid, "0.1.0")
    with pytest.raises(
        ValueError, match="Catalog already has an entry for this artifact!"
    ):
        TestFSArtifact.write_dummy_versioned(uid, "0.1.0")

    dev_id = f"_dev_{uid}"
    retired = list(
        TestCatalog._lookup(TestCatalog._retired_client(), dev_id, version="0.1.0")
    )
    dev = list(TestCatalog.lookup(dev_id, version="0.1.0"))
    assert len(dev) == 0
    assert len(retired) == 0

    TestFSArtifact.write_dummy_versioned(dev_id, "0.1.0")
    TestFSArtifact.write_dummy_versioned(dev_id, "0.1.0")

    retired = list(
        TestCatalog._lookup(TestCatalog._retired_client(), dev_id, version="0.1.0")
    )
    dev = list(TestCatalog.lookup(dev_id, version="0.1.0"))
    assert len(dev) == 1
    assert len(retired) == 1


def test_catalog_latest_partition(uid: ID) -> None:
    write_a_couple_of_partitions(uid, 10)
    latest = TestCatalog.latest_partition(uid)
    assert (
        max(x.partition for x in TestCatalog.lookup(uid) if x.partition is not None)
        == latest.partition
    )


def test_catalog_get_nice_error_on_missing_get(uid: ID) -> None:
    with pytest.raises(ValueError, match="Can't find requested artifact"):
        TestCatalog.get(uid, version="0.1.1")


def test_catalog_get_nice_error_on_missing_latest(uid: ID) -> None:
    with pytest.raises(ValueError, match="Can't find requested artifact"):
        TestCatalog.latest_partition(uid)


def test_catalog_fails_on_preexisting_manifest_file(uid: ID) -> None:
    with pytest.raises(
        ValueError, match="Manifest file `_MANIFEST.json` already present"
    ):
        with TestFSArtifact.dummy_versioned_ctx(uid, "0.1.0") as a:
            with fsspec.open(f"{a.staging_file_prefix}/_MANIFEST.json", "w") as f:
                f.write("foobar")
            a.files_pattern = None


def test_catalog_main_path_is_valid(uid: ID) -> None:
    with TestFSArtifact.dummy_versioned_ctx(uid, "0.1.0") as a:
        a.files_pattern = None
    a = TestCatalog.latest_partition(uid, model=TestFSArtifact)
    assert "**" not in a.main_dir
    assert a.files_pattern
    assert a.files_pattern.endswith("/**")
    assert a.files_pattern.startswith(a.main_dir)
    assert a.main_dir == a.files_pattern.replace("/**", "")


def test_catalog_main_path_is_valid_in_dev_mode(uid: ID) -> None:
    TestFSArtifact.write_dummy_versioned(f"_dev_{uid}", "0.1.0")
    a = TestCatalog.latest_partition(f"_dev_{uid}", model=TestFSArtifact)
    assert "**" not in a.main_dir
    assert a.files_pattern
    assert a.files_pattern.endswith("/output.txt")
    assert a.files_pattern.startswith(a.main_dir)
    assert a.main_dir == a.files_pattern.replace("/output.txt", "")


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


def test_catalog_record_loc(uid: ID) -> None:
    k = "call_site_relfname"
    with TestFSArtifact.dummy_versioned_ctx(uid, "0.1.0") as a:
        assert k in a.metadata.arbitrary
        assert a.metadata.arbitrary.pop(k)
        assert k not in a.metadata.arbitrary
        call_site = get_relative_call_site(1)
        assert call_site
        this_file, this_line = call_site
        assert this_file in __file__
        a.record_loc()

    a = TestCatalog.get(uid, version="0.1.0")
    assert a.metadata.arbitrary.get(k) == this_file
    assert a.metadata.arbitrary.get("call_site_lineno") == this_line + 4


def test_write_to_prod_on_non_prod_env_fails(uid: ID, monkeypatch: MonkeyPatch) -> None:
    with monkeypatch.context() as m:
        m.delattr(TestFSArtifact, "__test__")
        with pytest.raises(ValueError, match="your environment is missing RS_PROD"):
            TestFSArtifact.write_dummy_versioned(uid, "0.1.0")


def test_fs_artifact_join_paths_works(uid: ID) -> None:
    with TestFSArtifact.dummy_versioned_ctx(uid, "0.1.0") as a:
        assert a._file_prefix is not None
        p = a.joinpath("data.dat")
        with fsspec.open(p, "w") as f:
            f.write("ala ma kota")
        a.files_pattern = None
        assert p == f"{a.staging_file_prefix}/data.dat"
    assert a._file_prefix is None
    p = a.joinpath("data.dat")
    assert p == f"{a.main_dir}/data.dat"
    assert Path(p).read_text() == "ala ma kota"


def test_large_arbitrary_entry(uid: ID) -> None:
    more_than_1500 = "f" * 2000
    with TestFSArtifact.dummy_versioned_ctx(uid, "0.1.0") as a:
        a.metadata.arbitrary.update(dict(foo=more_than_1500))

    a = TestCatalog.get(uid, version="0.1.0")
    assert a.metadata.arbitrary.get("foo") == more_than_1500


def test_artifact_parse_extra_prop(uid: ID) -> None:
    TestFSArtifact.write_dummy_partitioned(uid, date.today())
    a_back = TestCatalog.get(uid, partition=date.today(), model=Artifact)
    assert getattr(a_back, "files_pattern")


def test_catalog_to_df(uid: ID) -> None:
    TestFSArtifact.write_dummy_partitioned(uid, date.today())
    df = TestCatalog.to_dataframe()
    assert df.id.str.contains(uid).any()
    assert any([c.startswith("metadata_arbitrary_") for c in df.columns])

    df_no_arb = TestCatalog.to_dataframe(include_arbitrary=False)
    assert not any([c.startswith("metadata_arbitrary_") for c in df_no_arb.columns])


def test_fs_stage(uid: ID, tmpdir: str) -> None:
    tmpdir_path = Path(tmpdir).joinpath("foo")
    tmpdir_path.write_text("foo")

    with TestFSArtifact.versioned(uid, version="foo") as a:
        a.stage(tmpdir_path)
    assert Path(a.joinpath("foo")).read_text() == "foo"
