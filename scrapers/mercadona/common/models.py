# scrapers/mercadona/common/models.py

from typing import TypedDict

class ProductRow(TypedDict):
    date: str
    product: str
    brand: str
    price: str
    price_per_unit: str
    offer: str
    category: str
    subcategory: str
    product_url: str

class CategoryTarget(TypedDict):
    group: str
    category: str
    subcategory: str
    url: str
