from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import List, cast

import pytest
from _pytest.monkeypatch import MonkeyPatch
from dateutil.tz import UTC

from articat.artifact import EXECUTION_URL_ENV_NAME, ID, Metadata
from articat.fs_artifact import FSArtifact
from articat.tests.utils import (
    TestCatalog,
    TestFSArtifact,
    write_a_couple_of_partitions,
)

# Tests in this module require Datastore emulator, and by default won't run
# unless pytest has been told that the emulator is in fact available
# via `--datastore_emulated`. Out Github CI has Datastore emulator enabled.
# To run these tests locally:
# docker run --rm -p 8099:8099 ravwojdyla/datastore_emulator:0.1.0
# pytest --datastore_emulated
pytestmark = pytest.mark.datastore_emulated


def test_simple_happy_path_catalog(uid: ID) -> None:
    yesterday = datetime.utcnow() - timedelta(days=1)
    today = datetime.utcnow()
    TestFSArtifact.write_dummy_partitioned(uid, yesterday)
    TestFSArtifact.write_dummy_partitioned(uid, today)

    r = TestCatalog.get(uid, partition=yesterday, model=FSArtifact)
    if r.files_pattern is None:
        pytest.fail("files_pattern should be set")
    else:
        assert TestFSArtifact.config().fs_prod_prefix() in r.files_pattern
        assert "ala" in Path(r.files_pattern).read_text()
        assert Path(r.files_pattern).parent.joinpath("_SUCCESS").exists()
        manifest_path = Path(r.files_pattern).parent.joinpath("_MANIFEST.json")
        assert manifest_path.exists()
        manifest = FSArtifact.parse_file(manifest_path)
        # Datastore adds tzinfo, which we don't have
        manifest.partition = cast(datetime, manifest.partition).replace(tzinfo=UTC)
        manifest.created = cast(datetime, manifest.created).replace(tzinfo=UTC)
        assert manifest == r

    assert len(list(TestCatalog.lookup(uid))) == 2
    assert len(list(TestCatalog.lookup(uid, partition_dt_start=today))) == 1
    assert len(list(TestCatalog.lookup(uid, partition_dt_start=yesterday))) == 2
    # end is not inclusive
    assert len(list(TestCatalog.lookup(uid, partition_dt_end=today))) == 1


def test_catalog_date_partition(uid: ID) -> None:
    dt = date.today()
    TestFSArtifact.write_dummy_partitioned(uid, dt)
    assert TestCatalog.get(uid, partition=dt).id == uid


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


def test_catalog_lookup__metadata(uid: ID, monkeypatch: MonkeyPatch) -> None:
    with monkeypatch.context() as m:
        m.setenv(EXECUTION_URL_ENV_NAME, "http://wwww.url-to-articat-exe.com")
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
    assert "execution_url" in _1.metadata.arbitrary
    assert (
        _1.metadata.arbitrary["execution_url"] == "http://wwww.url-to-articat-exe.com"
    )
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


def test_dev_mode_can_overwrite(uid: ID) -> None:
    TestFSArtifact.write_dummy_versioned(uid, "0.1.0")
    with pytest.raises(
        ValueError, match="Catalog already has an entry for this artifact!"
    ):
        TestFSArtifact.write_dummy_versioned(uid, "0.1.0")

    dev_id = f"_dev_{uid}"
    retired = list(
        TestCatalog._lookup(
            dev_id, version="0.1.0", client=TestCatalog._retired_client()
        )
    )
    dev = list(TestCatalog.lookup(dev_id, version="0.1.0"))
    assert len(dev) == 0
    assert len(retired) == 0

    TestFSArtifact.write_dummy_versioned(dev_id, "0.1.0")
    TestFSArtifact.write_dummy_versioned(dev_id, "0.1.0")

    retired = list(
        TestCatalog._lookup(
            dev_id, version="0.1.0", client=TestCatalog._retired_client()
        )
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


def test_catalog_to_df(uid: ID) -> None:
    TestFSArtifact.write_dummy_partitioned(uid, date.today())
    df = TestCatalog.to_dataframe()
    assert df.id.str.contains(uid).any()
    assert any([c.startswith("metadata_arbitrary_") for c in df.columns])

    df_no_arb = TestCatalog.to_dataframe(include_arbitrary=False)
    assert not any([c.startswith("metadata_arbitrary_") for c in df_no_arb.columns])
