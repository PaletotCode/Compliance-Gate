import json
import polars as pl
from typing import List, Dict, Any, Optional

from compliance_gate.domains.machines.ingest.mapping_profile import CsvTabConfig
from compliance_gate.domains.machines.classification.orchestrator import evaluate_machine
from compliance_gate.domains.machines.classification.models import MachineRecord
from compliance_gate.domains.machines.schemas import MachineFilterSchema, MachineItemSchema, MachineSummarySchema

class MachinesEngine:
    """
    Data engine minimum implementation for the Machine domain based on Polars.
    Provides table filtering and summary generation on processed datasets.
    """
    
    def __init__(self, data: List[Dict[str, Any]] = None, configs: Optional[Dict[str, CsvTabConfig]] = None):
        # Stub the in-memory data if not provided (Golden Test / Data engine phase 1)
        self.raw_data = data or []
        self.configs = configs or {}
        self._df = None
        self._processed = False
        
    def _ingest_and_classify(self) -> pl.DataFrame:
        """
        Simulates the ingestion, normalization, and classification process.
        """
        if self._processed and self._df is not None:
            return self._df
            
        processed_rows = []
        for raw in self.raw_data:
            # 1. Normalize
            record = MachineRecord(**raw)
            
            # 2. Classify (Orchestrator)
            result = evaluate_machine(record)
            
            # 2.5 Extract Selected Data from configs
            sel_data = {}
            for src, src_raw in record.raw_sources.items():
                if src in self.configs:
                    for col in self.configs[src].selected_columns:
                        if col in src_raw:
                            sel_data[f"{src}.{col}"] = src_raw[col]
            
            # 3. Output
            processed_rows.append({
                "id": record.hostname,
                "hostname": record.hostname,
                "pa_code": record.pa_code,
                "primary_status": result.primary_status,
                "primary_status_label": result.primary_status_label,
                "flags": result.flags,
                "has_ad": record.has_ad,
                "has_uem": record.has_uem,
                "has_edr": record.has_edr,
                "has_asset": record.has_asset,
                "model": "StubModel",
                "ip": "127.0.0.1",
                "tags": "StubTag",
                "main_user": record.main_user,
                "ad_os": record.ad_os,
                "us_ad": record.us_ad,
                "us_uem": record.us_uem,
                "us_edr": record.us_edr,
                "uem_extra_user_logado": record.uem_extra_user_logado,
                "edr_os": record.edr_os,
                "status_check_win11": record.status_check_win11,
                "uem_serial": record.uem_serial,
                "edr_serial": record.edr_serial,
                "chassis": record.chassis,
                "selected_data_json": json.dumps(sel_data, ensure_ascii=False)
            })
            
        if not processed_rows:
             # Create empty DF with correct schema
            self._df = pl.DataFrame(schema={
                "id": pl.Utf8, "hostname": pl.Utf8, "pa_code": pl.Utf8, 
                "primary_status": pl.Utf8, "primary_status_label": pl.Utf8,
                "flags": pl.List(pl.Utf8), "has_ad": pl.Boolean, "has_uem": pl.Boolean,
                "has_edr": pl.Boolean, "has_asset": pl.Boolean, "model": pl.Utf8,
                "ip": pl.Utf8, "tags": pl.Utf8, "main_user": pl.Utf8, "ad_os": pl.Utf8,
                "us_ad": pl.Utf8, "us_uem": pl.Utf8, "us_edr": pl.Utf8, 
                "uem_extra_user_logado": pl.Utf8, "edr_os": pl.Utf8, 
                "status_check_win11": pl.Utf8, "uem_serial": pl.Utf8, 
                "edr_serial": pl.Utf8, "chassis": pl.Utf8,
                "selected_data_json": pl.Utf8
            })
        else:
            self._df = pl.DataFrame(processed_rows)
            
        self._processed = True
        return self._df
        
    def _apply_filters(self, df: pl.DataFrame, filters: MachineFilterSchema) -> pl.DataFrame:
        if df.is_empty():
            return df
            
        if filters.search:
            # Example search logic: filters hostname based on uppercase match
            search_upper = filters.search.upper()
            df = df.filter(pl.col("hostname").str.to_uppercase().str.contains(search_upper))
            
        if filters.pa_code:
            df = df.filter(pl.col("pa_code") == filters.pa_code)
            
        if filters.statuses and len(filters.statuses) > 0:
            df = df.filter(pl.col("primary_status").is_in(filters.statuses))
            
        if filters.flags and len(filters.flags) > 0:
            # For each required flag, check if it's in the list column
            for flag in filters.flags:
                 df = df.filter(pl.col("flags").list.contains(flag))
                 
        return df

    def get_table(self, filters: MachineFilterSchema, page: int, size: int) -> tuple[List[MachineItemSchema], int]:
        df = self._ingest_and_classify()
        df = self._apply_filters(df, filters)
        
        total = df.height
        if total == 0:
            return [], 0
            
        offset = (page - 1) * size
        paginated_df = df.slice(offset, size)
        
        items = []
        for row in paginated_df.to_dicts():
            if "selected_data_json" in row:
                row["selected_data"] = json.loads(row.get("selected_data_json") or "{}")
            items.append(MachineItemSchema(**row))
            
        return items, total

    def get_summary(self, filters: MachineFilterSchema) -> MachineSummarySchema:
        df = self._ingest_and_classify()
        df = self._apply_filters(df, filters)
        
        total = df.height
        if total == 0:
            return MachineSummarySchema(total=0, by_status={}, by_flag={})
            
        by_status = df.group_by("primary_status").len().to_dict(as_series=False)
        status_counts = dict(zip(by_status["primary_status"], by_status["len"]))
        
        # Calculate flags counts (explode the list column, then group by)
        flag_counts = {}
        flags_df = df.select(pl.col("flags").explode().drop_nulls())
        if not flags_df.is_empty():
            by_flags = flags_df.group_by("flags").len().to_dict(as_series=False)
            flag_counts = dict(zip(by_flags["flags"], by_flags["len"]))
            
        return MachineSummarySchema(
            total=total,
            by_status=status_counts,
            by_flag=flag_counts
        )
