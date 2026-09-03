"""Matching output (07/08): regulations resolved onto a section."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SectionRegs:
    """Regulations resolved onto a section. Base/zone regs are an MU overlay, not stored here."""
    section_id: str
    reg_set_index: int                      # single reg set per section (07)
    named_reg_ids: tuple[str, ...] = ()     # direct named/override matches (provenance)
    tributary_reg_ids: tuple[str, ...] = ()  # inherited via tributary_section_ids (provenance)
