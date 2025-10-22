from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable


DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
QUARANTINE_DIR = DATA_DIR / "quarantine"
RUN_DIR = DATA_DIR / "runs"
DB_PATH = DATA_DIR / "txrrc.sqlite"
LAYOUT_DIR = Path(__file__).parent / "parse" / "layouts"


@dataclass(slots=True)
class DatasetConfig:
    dataset_key: str
    title: str
    source_patterns: Iterable[str]
    format: str
    layout: str | None
    target_table: str
    mapping: dict[str, str]
    keys: Iterable[str]
    validators: Iterable[str] = field(default_factory=tuple)


def ensure_data_dirs() -> None:
    for path in (DATA_DIR, RAW_DIR, QUARANTINE_DIR, RUN_DIR):
        path.mkdir(parents=True, exist_ok=True)


def get_default_datasets() -> dict[str, DatasetConfig]:
    return {
        "wellbore": DatasetConfig(
            dataset_key="wellbore",
            title="Wellbore Database",
            source_patterns=("wellbore", "dbf900"),
            format="ebcdic_fixed",
            layout="wellbore_dbf900.yaml",
            target_table="well",
            mapping={
                "api14": "api14",
                "api10": "api10",
                "district": "district",
                "county_code": "county_code",
                "field_code": "field_code",
                "field_name": "field_name",
                "well_name": "well_name",
                "operator_number": "operator_number",
                "operator_name": "operator_name",
                "orig_completion_date": "orig_completion_date",
                "total_depth": "total_depth",
            },
            keys=("api14",),
            validators=("api14", "api10"),
        ),
        "production": DatasetConfig(
            dataset_key="production",
            title="Production Ledger",
            source_patterns=("production", "ledger"),
            format="csv",
            layout=None,
            target_table="production_monthly",
            mapping={
                "api10": "api10",
                "prod_month": "prod_month",
                "oil_bbl": "oil_bbl",
                "gas_mcf": "gas_mcf",
                "water_bbl": "water_bbl",
            },
            keys=("api10", "prod_month"),
        ),
        "drilling_permits": DatasetConfig(
            dataset_key="drilling_permits",
            title="Drilling Permits",
            source_patterns=("drilling", "permit"),
            format="ascii_fixed",
            layout="drilling_permits.yaml",
            target_table="drilling_permit",
            mapping={
                "permit_number": "permit_number",
                "api10": "api10",
                "submit_date": "submit_date",
                "approve_date": "approve_date",
                "well_purpose": "well_purpose",
                "lateral_length": "lateral_length",
                "is_horizontal": "is_horizontal",
            },
            keys=("permit_number",),
        ),
        "organizations": DatasetConfig(
            dataset_key="organizations",
            title="P-5 Organizations",
            source_patterns=("organization", "p-5"),
            format="csv",
            layout=None,
            target_table="organization",
            mapping={
                "org_number": "org_number",
                "name": "name",
                "status": "status",
                "address": "address",
            },
            keys=("org_number",),
        ),
        "fields": DatasetConfig(
            dataset_key="fields",
            title="Fields",
            source_patterns=("field", "field name"),
            format="csv",
            layout=None,
            target_table="field",
            mapping={
                "field_code": "field_code",
                "field_name": "field_name",
                "district": "district",
                "oil_or_gas": "oil_or_gas",
            },
            keys=("field_code",),
        ),
    }


def resolve_layout_path(name: str) -> Path:
    return LAYOUT_DIR / name


def load_layout(name: str) -> Any:
    import yaml

    with resolve_layout_path(name).open("r", encoding="utf8") as fh:
        return yaml.safe_load(fh)
