"""
person_dim.py - Placeholder for Person dimensional tracking.
"""

from pydantic import BaseModel

from .models import SpineTable


class PersonDimRow(BaseModel):
    cpf: str
    name: str | None = None
    role: str | None = None


class PersonDimSpine(SpineTable):
    domain: str = "person"
    spine_name: str = "person_dim"
