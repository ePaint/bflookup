from dataclasses import dataclass


@dataclass
class OutputEntry:
    upc: str
    qty_input: int
    qty_database: int = 0
    variance: int = 0
    unit_cost: float = 0
    total_dollar_variance: float = 0
    name: str = ""
