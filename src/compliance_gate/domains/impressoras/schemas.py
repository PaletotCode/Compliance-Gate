from pydantic import BaseModel, ConfigDict

class ImpressorasFilterSchema(BaseModel):
    compliant: bool = True
    offline: bool = False

class ImpressorasItemSchema(BaseModel):
    id: str
    name: str
    pa_code: str
    is_compliant: bool

    model_config = ConfigDict(from_attributes=True)

class ImpressorasSummarySchema(BaseModel):
    total: int
    compliant: int
    offline: int
