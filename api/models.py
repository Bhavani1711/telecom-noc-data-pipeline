from pydantic import BaseModel
from typing import Optional


class NetworkSummaryResponse(BaseModel):
    total_activity: float
    active_grids: int
    peak_hour: str
    top_grid: int
    as_of: str
