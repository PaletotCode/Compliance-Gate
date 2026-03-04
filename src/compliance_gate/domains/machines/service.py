from typing import List, Tuple
from compliance_gate.domains.machines.schemas import FilterDefinitionSchema, MachineFilterSchema, MachineItemSchema, MachineSummarySchema
from compliance_gate.domains.machines.classification.orchestrator import load_rule, PRIMARY_FILTERS_ORDER, FLAG_FILTERS, SPECIAL_FILTERS
from compliance_gate.domains.machines.engine import MachinesEngine

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
                    is_flag=rule.STATUS_DEF.is_flag
                )
            )
        return filters
        
    @staticmethod
    def _get_engine() -> MachinesEngine:
        # For Phase 1, using empty in-memory engine (will simulate ingestion if passed real data)
        # Ultimately, this will be connected to DuckDB/Local Polars storage.
        return MachinesEngine(data=[])

    @staticmethod
    def get_table_data(filters: MachineFilterSchema, page: int, size: int) -> Tuple[List[MachineItemSchema], int]:
        engine = MachinesService._get_engine()
        return engine.get_table(filters, page, size)

    @staticmethod
    def get_summary_data(filters: MachineFilterSchema) -> MachineSummarySchema:
        engine = MachinesService._get_engine()
        return engine.get_summary(filters)
