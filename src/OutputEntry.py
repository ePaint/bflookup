from typing import Optional

from pydantic import BaseModel, Field


class OutputEntry(BaseModel):
    upc: Optional[str] = Field(default="", alias="UPC")
    stock_code: Optional[str] = Field(default="", alias="Stock Code")
    qty_input: Optional[int] = Field(default=0, alias="Qty Counted")
    name: Optional[str] = Field(default="", alias="Item Name")
    unit_cost: Optional[float] = Field(default=0, alias="Unit Cost")
    category: Optional[str] = Field(default="", alias="Category Name")
    category_group: Optional[str] = Field(default="", alias="Category Group Name")
    qty_database: Optional[int] = Field(default=0, alias="Expected Qty in POS")
    unit_variance: Optional[int] = Field(default=0, alias="Variance in Units")
    dollar_variance: Optional[float] = Field(default=0, alias="Variance in Dollars")
