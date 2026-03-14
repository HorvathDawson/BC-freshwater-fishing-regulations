"""
Extraction data models for the BC Fishing Regulations Synopsis Pipeline (v2).

Contains the data classes used during PDF extraction: WaterbodyRow,
PageMetadata, PageResult, and ExtractionResults.  These are standalone
copies (not re-exports) so that the v2 package is fully self-contained.
"""

from typing import Any, Dict, Iterator, List, Optional
from attrs import define, asdict


# ==========================================
#       EXTRACTION DATA MODELS
# ==========================================


@define(frozen=True, cache_hash=True)
class WaterbodyRow:
    """Represents a single waterbody row extracted from the PDF."""

    water: str
    mu: List[str]
    raw_regs: str
    symbols: List[str]
    page: int
    image: str
    region: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "WaterbodyRow":
        return cls(**data)


@define(frozen=True, cache_hash=True)
class PageMetadata:
    """Metadata for a single page."""

    page_number: int
    region: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PageMetadata":
        return cls(**data)


@define(frozen=True, cache_hash=True)
class PageResult:
    """Result of extracting a single page."""

    metadata: PageMetadata
    rows: List[WaterbodyRow]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "context": self.metadata.to_dict(),
            "rows": [row.to_dict() for row in self.rows],
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PageResult":
        return cls(
            metadata=PageMetadata.from_dict(data["context"]),
            rows=[WaterbodyRow.from_dict(row) for row in data["rows"]],
        )


@define(frozen=True, cache_hash=True)
class ExtractionResults:
    """Results from extracting all pages from the PDF."""

    pages: List[PageResult]

    def to_dict(self) -> List[Dict[str, Any]]:
        return [page.to_dict() for page in self.pages]

    @classmethod
    def from_dict(cls, data: List[Dict[str, Any]]) -> "ExtractionResults":
        return cls(pages=[PageResult.from_dict(page) for page in data])

    def __len__(self) -> int:
        return len(self.pages)

    def __iter__(self) -> Iterator[PageResult]:
        return iter(self.pages)

    def __getitem__(self, index: int) -> PageResult:
        return self.pages[index]


__all__ = [
    "ExtractionResults",
    "PageMetadata",
    "PageResult",
    "WaterbodyRow",
]
