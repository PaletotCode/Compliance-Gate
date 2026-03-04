import logging
import os
from pathlib import Path
from typing import List, Tuple

from compliance_gate.domains.machines.schemas import (
    FilterDefinitionSchema, MachineFilterSchema, MachineItemSchema, MachineSummarySchema,
)
from compliance_gate.domains.machines.classification.orchestrator import (
    load_rule, PRIMARY_FILTERS_ORDER, FLAG_FILTERS, SPECIAL_FILTERS,
)
from compliance_gate.domains.machines.engine import MachinesEngine

log = logging.getLogger(__name__)


class MachinesService:
    @staticmethod
    def get_available_filters() -> List[FilterDefinitionSchema]:
        """
        Dynamically load all status definitions from the isolated rules.
        """
        filters = []
        all_modules = PRIMARY_FILTERS_ORDER + FLAG_FILTERS + SPECIAL_FILTERS
        for module_name in all_modules:
            rule = load_rule(module_name)
            filters.append(
                FilterDefinitionSchema(
                    key=rule.STATUS_DEF.key,
                    label=rule.STATUS_DEF.label,
                    severity=rule.STATUS_DEF.severity,
                    description=rule.STATUS_DEF.description,
                    is_flag=rule.STATUS_DEF.is_flag,
                )
            )
        return filters

    @staticmethod
    def _get_engine() -> MachinesEngine:
        """
        Build a MachinesEngine loaded with real CSV data.

        Data directory resolution (in order):
          1. CG_DATA_DIR environment variable
          2. /workspace (Docker mount)
          3. Project root (detected heuristically)

        If CSVs are not found, falls back to empty engine and logs a warning.
        The join logic mirrors dashboard_fixed.ts:
          - Universe = AD + UEM + EDR only
          - ASSET = lookup-only (marks has_asset, never creates new entries)
        """
        from compliance_gate.infra.storage.csv_loader import load_machines_sources
        from compliance_gate.domains.machines.master_map_builder import build_master_records

        # Resolve data directory
        data_dir_env = os.environ.get("CG_DATA_DIR", "")
        if data_dir_env and Path(data_dir_env).exists():
            data_dir = Path(data_dir_env)
        elif Path("/workspace").exists() and any(
            (Path("/workspace") / f).exists() for f in ["AD.csv", "AD.CSV"]
        ):
            data_dir = Path("/workspace")
        else:
            # Heuristic: walk up from this file to project root
            here = Path(__file__).resolve()
            data_dir = None
            for parent in here.parents:
                if any((parent / f).exists() for f in ["AD.csv", "AD.CSV"]):
                    data_dir = parent
                    break

        if data_dir is None:
            log.warning(
                "MachinesService: CSV data directory not found. "
                "Set CG_DATA_DIR env var or mount CSVs at /workspace. "
                "Serving empty engine."
            )
            return MachinesEngine(data=[])

        log.info("MachinesService: loading CSVs from %s", data_dir)

        try:
            sources = load_machines_sources(data_dir)
        except Exception as exc:
            log.error("MachinesService: CSV load failed — %s", exc)
            return MachinesEngine(data=[])

        if sources.load_errors:
            for err in sources.load_errors:
                log.warning("MachinesService CSV load warning: %s", err)

        try:
            records = build_master_records(sources)
        except Exception as exc:
            log.error("MachinesService: master map build failed — %s", exc)
            return MachinesEngine(data=[])

        log.info("MachinesService: Universe size=%d records — engine ready", len(records))
        return MachinesEngine(data=records)

    @staticmethod
    def get_table_data(
        filters: MachineFilterSchema, page: int, size: int
    ) -> Tuple[List[MachineItemSchema], int]:
        engine = MachinesService._get_engine()
        return engine.get_table(filters, page, size)

    @staticmethod
    def get_summary_data(filters: MachineFilterSchema) -> MachineSummarySchema:
        engine = MachinesService._get_engine()
        return engine.get_summary(filters)

