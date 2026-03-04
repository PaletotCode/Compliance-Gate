from typing import Optional, List
from pydantic import BaseModel, Field
from enum import Enum

class StatusSeverity(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    DANGER = "DANGER"
    SUCCESS = "SUCCESS"

class MachineStatusDef(BaseModel):
    key: str
    label: str
    severity: StatusSeverity
    description: str
    is_flag: bool = False

class MachineRecord(BaseModel):
    """
    Representation of a single machine row with normalized data from the
    ingestion pipeline. Evaluated by filters to determine final status.
    """
    hostname: str
    pa_code: str
    
    # Pre-calculated sources from Join stage
    has_ad: bool = False
    has_uem: bool = False
    has_edr: bool = False
    has_asset: bool = False

    # Normalized fields
    ad_os: Optional[str] = None
    uem_serial: Optional[str] = None
    edr_serial: Optional[str] = None
    
    # For matching login user and hostname suffix (PA mismatch)
    main_user: Optional[str] = None
    uem_extra_user_logado: Optional[str] = None

    # Stale/Offline calculations
    last_seen_date_ms: Optional[int] = None
    
    # Special calculation context properties
    serial_is_cloned: bool = False
    is_virtual_gap: bool = False
    is_available_in_asset: bool = False

class MachineStatusResult(BaseModel):
    """
    The output of the orchestrator after evaluating all rules on a MachineRecord.
    """
    primary_status: str
    primary_status_label: str
    flags: List[str] = Field(default_factory=list)
