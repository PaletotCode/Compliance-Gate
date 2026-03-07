"""
pa_dim.py - Placeholder for Attendance Point (PA) dimensional tracking.
"""

from pydantic import BaseModel

from .models import SpineTable


class PADimRow(BaseModel):
    pa_code: str
    name: str | None = None
    region: str | None = None

class PADimSpine(SpineTable):
    domain: str = "pa"
    spine_name: str = "pa_dim"
