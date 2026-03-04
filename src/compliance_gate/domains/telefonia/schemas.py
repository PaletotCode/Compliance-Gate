from pydantic import BaseModel, ConfigDict

class TelefoniaFilterSchema(BaseModel):
    compliant: bool = True
    inconsistency: bool = False

class TelefoniaItemSchema(BaseModel):
    id: str
    number: str
    pa_code: str
    is_compliant: bool

    model_config = ConfigDict(from_attributes=True)

class TelefoniaSummarySchema(BaseModel):
    total: int
    compliant: int
    inconsistency: int
