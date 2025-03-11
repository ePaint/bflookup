from typing import Optional

from pydantic import BaseModel, Field


class OutputEntry(BaseModel):
    upc: Optional[str] = Field(default="", alias="UPC")
    qty_input: Optional[int] = Field(default=0, alias="Quantity Input")
    qty_database: Optional[int] = Field(default=0, alias="Quantity Database")
    unit_cost: Optional[float] = Field(default=0, alias="Unit Cost", description="Currency")
    stock_code: Optional[str] = Field(default="", alias="Stock Code")
    name: Optional[str] = Field(default="", alias="Name")
    category: Optional[str] = Field(default="", alias="Category")
    category_group: Optional[str] = Field(default="", alias="Category Group")
    unit_variance: Optional[int] = Field(default=0, alias="Unit Variance")
    dollar_variance: Optional[float] = Field(default=0, alias="Dollar Variance", description="Currency")
    found: Optional[bool] = Field(default=False, alias="Found")

    @staticmethod
    def get_currency_fields() -> list[str]:
        return [
            field_info.alias
            for field_name, field_info in OutputEntry.model_fields.items()
            if field_info.description == "Currency"
        ]
