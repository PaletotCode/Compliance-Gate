import json
import logging
import os
from pathlib import Path
from typing import List, Optional, Tuple, Dict

from sqlalchemy.orm import Session

from compliance_gate.domains.machines.schemas import (
    FilterDefinitionSchema, MachineFilterSchema, MachineItemSchema, MachineSummarySchema,
)
from compliance_gate.domains.machines.classification.orchestrator import (
    load_rule, PRIMARY_FILTERS_ORDER, FLAG_FILTERS, SPECIAL_FILTERS,
)
from compliance_gate.domains.machines.engine import MachinesEngine
from compliance_gate.infra.storage import datasets_store, profiles_store
from compliance_gate.domains.machines.ingest.mapping_profile import CsvTabConfig

log = logging.getLogger(__name__)


class MachinesService:
    class NoDatasetError(Exception):
        """Raised when no dataset_version is found and raw CSVs are also unavailable."""

    @staticmethod
    def get_available_filters() -> List[FilterDefinitionSchema]:
        """Dynamically load all status definitions from the isolated rules."""
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
    def _get_engine(
        db: Session,
        tenant_id: str,
        dataset_version_id: Optional[str] = None,
    ) -> MachinesEngine:
        """
        Build a MachinesEngine loaded with data.

        Resolution order:
          1. dataset_version_id → load by ID from DB (not yet implemented, Chat 2)
          2. CG_DATA_DIR env var or /workspace → load from raw CSV files
          3. Heuristic walk-up from __file__ → project root
          4. If nothing found: raise NoDatasetError

        Note: In Chat 1, dataset_version storage means metadata + metrics only.
        Record re-loading from a specific version will be implemented in Chat 2.
        For now, if dataset_version_id is provided but DB record loading is not yet
        implemented, we fall through to CSV loading with a warning.
        """
        from compliance_gate.infra.storage.csv_loader import load_machines_sources
        from compliance_gate.domains.machines.master_map_builder import build_master_records

        # Chat 2: Resolve dataset version and used_profile_ids
        configs: Dict[str, CsvTabConfig] = {}
        if dataset_version_id:
            version = datasets_store.get_version_by_id(db, dataset_version_id)
            if version and version.tenant_id != tenant_id:
                raise MachinesService.NoDatasetError("dataset_version not found for tenant")
            if version and version.used_profile_ids:
                try:
                    pids = json.loads(version.used_profile_ids)
                    for src, pid in pids.items():
                        payload = profiles_store.get_active_payload(db, pid)
                        if payload:
                            configs[src] = payload
                except Exception as e:
                    log.warning("Failed to parse used_profile_ids for version %s: %s", dataset_version_id, e)

        # Resolve data directory
        data_dir_env = os.environ.get("CG_DATA_DIR", "")
        if data_dir_env and Path(data_dir_env).exists():
            data_dir = Path(data_dir_env)
        elif Path("/workspace").exists() and any(
            (Path("/workspace") / f).exists() for f in ["AD.csv", "AD.CSV"]
        ):
            data_dir = Path("/workspace")
        else:
            here = Path(__file__).resolve()
            data_dir = None
            for parent in here.parents:
                if any((parent / f).exists() for f in ["AD.csv", "AD.CSV"]):
                    data_dir = parent
                    break

        if data_dir is None:
            raise MachinesService.NoDatasetError(
                "CSV data directory not found. "
                "Run POST /api/v1/datasets/machines/ingest or set CG_DATA_DIR."
            )

        log.info("MachinesService: loading CSVs from %s", data_dir)

        try:
            sources = load_machines_sources(data_dir)
        except Exception as exc:
            log.error("MachinesService: CSV load failed — %s", exc)
            raise MachinesService.NoDatasetError(str(exc)) from exc

        if sources.load_errors:
            for err in sources.load_errors:
                log.warning("MachinesService CSV load warning: %s", err)

        try:
            records = build_master_records(sources)
        except Exception as exc:
            log.error("MachinesService: master map build failed — %s", exc)
            raise MachinesService.NoDatasetError(str(exc)) from exc

        log.info("MachinesService: Universe size=%d records — engine ready", len(records))
        return MachinesEngine(data=records, configs=configs)

    @staticmethod
    def get_table_data(
        db: Session,
        filters: MachineFilterSchema, page: int, size: int,
        tenant_id: str,
        dataset_version_id: Optional[str] = None,
    ) -> Tuple[List[MachineItemSchema], int]:
        engine = MachinesService._get_engine(db, tenant_id=tenant_id, dataset_version_id=dataset_version_id)
        return engine.get_table(filters, page, size)

    @staticmethod
    def get_summary_data(
        db: Session,
        filters: MachineFilterSchema,
        tenant_id: str,
        dataset_version_id: Optional[str] = None,
    ) -> MachineSummarySchema:
        engine = MachinesService._get_engine(db, tenant_id=tenant_id, dataset_version_id=dataset_version_id)
        return engine.get_summary(filters)
