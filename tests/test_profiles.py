import pytest
from pydantic import ValidationError
from compliance_gate.domains.machines.ingest.mapping_profile import CsvTabConfig

def test_csv_tab_config_default_values():
    config = CsvTabConfig(
        header_row=0,
        sic_column="Hostname",
        selected_columns=["Hostname", "OS"]
    )
    assert config.header_row == 0
    assert config.delimiter is None
    assert config.encoding is None
    assert config.sic_column == "Hostname"
    assert config.selected_columns == ["Hostname", "OS"]
    assert config.alias_map == {}

def test_csv_tab_config_invalid_header():
    with pytest.raises(ValidationError):
        CsvTabConfig(
            header_row=-1,
            sic_column="Host",
            selected_columns=[]
        )


