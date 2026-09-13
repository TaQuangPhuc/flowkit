"""
Product Data Normalizer for FlowKit Crawler Engine.
Standardizes product data from Shopee and TikTok Shop into a unified NormalizedProduct schema.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
import time


def format_currency_vnd(amount: Optional[float]) -> str:
    """Format numeric amount into Vietnamese currency string (e.g. 25000 -> 25.000₫)."""
    if amount is None or amount <= 0:
        return ""
    try:
        return f"{int(amount):,}".replace(",", ".") + "₫"
    except Exception:
        return f"{amount}₫"


@dataclass
class NormalizedProduct:
    platform: str                          # "shopee" | "tiktok"
    product_id: str                        # Item ID or Video ID
    title: str = ""                        # Sanitized product title
    url: str = ""                          # Raw input URL
    canonical_url: str = ""                # Canonical resolved URL
    shop_id: Optional[str] = None          # Seller or shop ID
    shop_name: Optional[str] = None        # Seller or shop display name
    shop_url: Optional[str] = None         # Shop profile URL
    price: Optional[float] = None          # Current selling price
    original_price: Optional[float] = None # Original / strike-through price
    discount_percent: Optional[int] = 0    # Discount percentage (e.g. 50 for 50%)
    discount_percentage: Optional[int] = 0 # Alias for compatibility
    currency: str = "VND"                  # Currency code (default VND)
    formatted_price: str = ""              # Formatted e.g. "25.000₫"
    formatted_original_price: str = ""     # Formatted e.g. "50.000₫"
    rating_star: Optional[float] = 5.0     # Rating stars (1.0 - 5.0)
    rating_count: Optional[int] = 0        # Total review count
    historical_sold: Optional[int] = 0     # Numeric sold count
    sold_count: Optional[str] = ""         # Human-readable sold count (e.g. "Đã bán 5.4k")
    description: str = ""                  # Full product description
    highlights: List[str] = field(default_factory=list) # USP highlights chips
    specs: Dict[str, str] = field(default_factory=dict) # Key-value product attributes
    images: List[str] = field(default_factory=list)     # All HD product image URLs
    primary_image: Optional[str] = None    # Primary keyframe image URL
    cover_image: Optional[str] = None      # Alias to primary_image
    video_url: Optional[str] = None        # Demo video MP4 URL
    demo_video_url: Optional[str] = None   # Alias to video_url
    is_price_estimated: bool = False       # True if price was estimated due to shield omission
    crawled_at: float = field(default_factory=time.time)

    def __post_init__(self):
        # Sync aliases
        if not self.discount_percentage and self.discount_percent:
            self.discount_percentage = self.discount_percent
        elif not self.discount_percent and self.discount_percentage:
            self.discount_percent = self.discount_percentage

        if not self.cover_image and self.primary_image:
            self.cover_image = self.primary_image
        elif not self.primary_image and self.cover_image:
            self.primary_image = self.cover_image
        elif not self.primary_image and self.images:
            self.primary_image = self.images[0]
            self.cover_image = self.images[0]

        if not self.demo_video_url and self.video_url:
            self.demo_video_url = self.video_url
        elif not self.video_url and self.demo_video_url:
            self.video_url = self.demo_video_url

        if not self.formatted_price and self.price is not None:
            self.formatted_price = format_currency_vnd(self.price)
        if not self.formatted_original_price and self.original_price is not None:
            self.formatted_original_price = format_currency_vnd(self.original_price)

        if not self.sold_count and self.historical_sold:
            if self.historical_sold >= 1000:
                self.sold_count = f"Đã bán {self.historical_sold / 1000:.1f}k".replace(".0k", "k")
            else:
                self.sold_count = f"Đã bán {self.historical_sold}"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "platform": self.platform,
            "product_id": self.product_id,
            "shop_id": self.shop_id,
            "shop_name": self.shop_name,
            "shop_url": self.shop_url,
            "title": self.title,
            "url": self.url,
            "canonical_url": self.canonical_url,
            "price": self.price,
            "original_price": self.original_price,
            "discount_percent": self.discount_percent,
            "discount_percentage": self.discount_percentage,
            "currency": self.currency,
            "formatted_price": self.formatted_price,
            "formatted_original_price": self.formatted_original_price,
            "rating_star": self.rating_star,
            "rating_count": self.rating_count,
            "historical_sold": self.historical_sold,
            "sold_count": self.sold_count,
            "description": self.description,
            "highlights": self.highlights,
            "specs": self.specs,
            "images": self.images,
            "primary_image": self.primary_image,
            "cover_image": self.cover_image,
            "video_url": self.video_url,
            "demo_video_url": self.demo_video_url,
            "is_price_estimated": self.is_price_estimated,
            "crawled_at": self.crawled_at,
        }
