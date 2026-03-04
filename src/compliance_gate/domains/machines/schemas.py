from typing import List, Dict, Optional
from pydantic import BaseModel, ConfigDict
from compliance_gate.domains.machines.classification.models import StatusSeverity

# Models for the /filters endpoint

class FilterDefinitionSchema(BaseModel):
    key: str
    label: str
    severity: StatusSeverity
    description: str
    is_flag: bool

# Models for the /table endpoint

class MachineFilterSchema(BaseModel):
    # This schema will be used to receive query params for filtering
    search: Optional[str] = None
    pa_code: Optional[str] = None
    statuses: Optional[List[str]] = None
    flags: Optional[List[str]] = None

class MachineItemSchema(BaseModel):
    id: str  # e.g., Hostname
    hostname: str
    pa_code: str
    primary_status: str
    primary_status_label: str
    flags: List[str]
    has_ad: bool
    has_uem: bool
    has_edr: bool
    has_asset: bool
    model: Optional[str] = None
    ip: Optional[str] = None
    tags: Optional[str] = None
    
    # Extra fields for details
    main_user: Optional[str] = None
    ad_os: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)

# Models for the /summary endpoint

class MachineSummarySchema(BaseModel):
    total: int
    by_status: Dict[str, int]
    by_flag: Dict[str, int]
