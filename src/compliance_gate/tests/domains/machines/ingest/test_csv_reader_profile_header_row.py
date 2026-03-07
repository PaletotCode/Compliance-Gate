from __future__ import annotations

from compliance_gate.domains.machines.ingest.mapping_profile import CsvTabConfig
from compliance_gate.infra.storage.csv_reader import read_csv_for_source


def _write_ad_csv(tmp_path) -> None:
    content = """metadata line 1\nmetadata line 2\nmetadata line 3\nMachine Name;User\nPC-01;alice\nPC-02;bob\n"""
    (tmp_path / "AD.csv").write_text(content, encoding="utf-8")


def test_read_csv_for_source_honors_header_row_from_profile(tmp_path) -> None:
    _write_ad_csv(tmp_path)
    cfg = CsvTabConfig(
        header_row=3,
        delimiter=";",
        encoding="utf-8",
        sic_column="Machine Name",
    )

    result = read_csv_for_source(
        source="AD",
        data_dir=tmp_path,
        filename_candidates=["AD.csv"],
        config=cfg,
    )

    assert result.ok
    assert result.header_row_index == 3
    assert result.rows_read == 2
    assert result.df is not None
    assert result.df.columns == ["Machine Name", "User"]


def test_read_csv_for_source_accepts_header_row_index_alias(tmp_path) -> None:
    _write_ad_csv(tmp_path)
    cfg = CsvTabConfig.model_validate(
        {
            "header_row_index": 3,
            "delimiter": ";",
            "encoding": "utf-8",
            "sic_column": "Machine Name",
        }
    )

    result = read_csv_for_source(
        source="AD",
        data_dir=tmp_path,
        filename_candidates=["AD.csv"],
        config=cfg,
    )

    assert result.ok
    assert cfg.header_row == 3
    assert result.header_row_index == 3
    assert result.rows_read == 2
