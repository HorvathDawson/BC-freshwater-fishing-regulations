# Regulation Cascading & Precedence

## Overview

Every FWA segment in BC receives fishing regulations from **multiple sources** that form a cascading precedence hierarchy. More specific regulations override more general ones. Understanding this hierarchy is critical for correctly presenting rules to users and resolving conflicts.

**Key Principle**: When multiple regulations define the same rule type for the same species at the same location, the most specific regulation wins.

---

## The Hierarchy

### Five Levels (General → Specific)

```
┌─────────────────────────────────────────────────────────────┐
│ 1. PROVINCIAL REGULATIONS (applies to all)                  │
│    - Applies to every waterbody in BC                       │
│    - Example: "Daily limit for any trout species = 5"       │
│    - Scope: ALL segments in province                        │
│    - Override: Can be overridden by any more specific level │
└─────────────────────────────────────────────────────────────┘
                            ↓ Overridden by ↓
┌─────────────────────────────────────────────────────────────┐
│ 2. ZONAL REGULATIONS (applies to all in zone)               │
│    - Applies to all waterbodies in a management zone        │
│    - Example: "Region 4 daily trout limit = 3"              │
│    - Scope: ALL segments within zone boundary               │
│    - Override: Overrides provincial, overridden by below    │
└─────────────────────────────────────────────────────────────┘
                            ↓ Overridden by ↓
┌─────────────────────────────────────────────────────────────┐
│ 3. INHERITED WATERBODY REGULATIONS                          │
│    - Inherited from parent waterbody that includes          │
│      tributaries                                            │
│    - Example: Thompson River inherits Fraser River rules    │
│      because Fraser regulation includes tributaries         │
│    - Scope: Applies when this waterbody is a tributary of   │
│      another regulated waterbody                            │
│    - Override: Overrides zonal and provincial               │
└─────────────────────────────────────────────────────────────┘
                            ↓ Overridden by ↓
┌─────────────────────────────────────────────────────────────┐
│ 4. WATERBODY-SPECIFIC REGULATIONS (global scope)            │
│    - Applies to a specific waterbody's global scope         │
│    - Example: "THOMPSON RIVER daily trout limit = 2"        │
│    - Scope: Entire waterbody per global scope (whole        │
│      system, tributaries only, etc.)                        │
│    - Override: Overrides inherited, zonal, and provincial   │
└─────────────────────────────────────────────────────────────┘
                            ↓ Overridden by ↓
┌─────────────────────────────────────────────────────────────┐
│ 5. RULE-LEVEL SCOPED REGULATIONS                            │
│    - Individual rules within a waterbody regulation with    │
│      specific location scope                                │
│    - Example: "Trout limit = 1 downstream of Kamloops Lake" │
│    - Scope: Subset of waterbody regulation (directional,    │
│      segment, etc.)                                         │
│    - Override: Most specific - overrides all above          │
│    - Note: Part of same regulation as level 4, just more    │
│      specific location                                      │
└─────────────────────────────────────────────────────────────┘
```

### Important Notes

- **Lower levels override higher levels**: Waterbody-specific overrides inherited waterbody, inherited overrides zonal, zonal overrides provincial
- **Inheritance through tributary relationships**: A waterbody inherits regulations from any parent waterbody that includes tributaries in its scope
- **Rule-scoped vs waterbody-scoped**: Both come from the same regulation, rule-scoped just has more specific location within the waterbody
- **Same level, different rule types**: No conflict - both apply (e.g., trout quota + bait ban)
- **Same level, different species**: No conflict - both apply (e.g., trout quota + salmon quota)
- **Same level, same rule type + species**: Error - should not happen, requires manual review

---

## Precedence Resolution Example

### Scenario

User queries segment `fwa_thompson_downstream_001` (Thompson River downstream of Kamloops Lake).

Assume Thompson River is a tributary of Fraser River, and Fraser River has a regulation with "includes tributaries".

**Step 1: Collect All Applicable Regulations**

```python
applicable_regulations = {
    "provincial": {
        "scope": "ALL",
        "rules": [
            {"type": "QUOTA", "species": "Trout", "limit": 5}
        ]
    },
    "zonal": {
        "scope": "Region 3",
        "rules": [
            {"type": "QUOTA", "species": "Trout", "limit": 3},
            {"type": "BAIT_BAN", "species": "ALL"}
        ]
    },
    "inherited": {
        "scope": "FRASER RIVER [including tributaries]",
        "source": "Fraser River regulation",
        "rules": [
            {"type": "QUOTA", "species": "Trout", "limit": 4},
            {"type": "SIZE_LIMIT", "species": "Trout", "min": 30}
        ]
    },
    "waterbody": {
        "scope": "THOMPSON RIVER [including tributaries]",
        "source": "Thompson River regulation",
        "rules": [
            {"type": "QUOTA", "species": "Trout", "limit": 2},
            {"type": "QUOTA", "species": "Salmon", "limit": 1}
        ]
    },
    "rule_scoped": {
        "scope": "THOMPSON RIVER downstream of Kamloops Lake",
        "source": "Thompson River regulation (specific rule)",
        "rules": [
            {"type": "QUOTA", "species": "Trout", "limit": 1}
        ]
    }
}
```

**Step 2: Apply Precedence (Most Specific Wins)**

```python
final_rules = {
    "Trout quota": 1,           # rule_scoped (level 5) overrides waterbody (4) overrides inherited (3) overrides zonal (2) overrides provincial (1)
    "Trout size limit": 30,     # inherited from Fraser (only one defined)
    "Salmon quota": 1,          # waterbody (only one defined at this level)
    "Bait ban": True            # zonal (only one defined at this level)
}
```

**Precedence Chain for Trout Quota**:
- Provincial: 5 (base rule) - **OVERRIDDEN**
- Zonal: 3 (overrides provincial) - **OVERRIDDEN**
- Inherited (Fraser River): 4 (overrides zonal) - **OVERRIDDEN**
- Waterbody (Thompson River): 2 (overrides inherited) - **OVERRIDDEN**
- Rule-scoped (Thompson downstream): 1 (overrides waterbody) ← **ACTIVE**

**Precedence Chain for Trout Size Limit**:
- Inherited (Fraser R (tributary of Fraser River):

Provincial Scope (entire BC):
├── Region 3 Zonal Scope:
│   ├── FRASER RIVER [including tributaries]:
│   │   └── Applies to Thompson River as a tributary:
│   │       → Trout limit = 4 (inherited from Fraser)
│   │       → Trout min size = 30cm (inherited from Fraser)
│   │
│   ├── THOMPSON RIVER waterbody scope [including tributaries]:
│   │   ├── Upstream segments (waterbody global scope):
│   │   │   → Trout limit = 2 (from Thompson regulation - overrides Fraser)
│   │   │   → Trout min size = 30cm (from Fraser - inherited, not overridden)
│   │   │   → Salmon limit = 1 (from Thompson regulation)
│   │   │   → Bait ban = True (from zonal regulation)
│   │   │
│   │   └── Downstream of Kamloops Lake (rule-scoped within Thompson):
│   │       → Trout limit = 1 (from Thompson rule scope - MOST SPECIFIC)
│   │       → Trout min size = 30cm (from Fraser - inherited, not overridden)
│   │       → Salmon limit = 1 (from Thompson only):
│   │   │   → Trout limit = 2 (from waterbody regulation)
│   │   │   → Salmon limit = 1 (from waterbody regulation)
│   │   │   → Bait ban = True (from zonal regulation)
│   │   │
│   │   └── Downstream of Kamloops Lake (rule-scoped):
│   │       → Trout limit = 1 (from rule scope - MOST SPECIFIC)
│   │       → Salmon limit = 1 (from waterbody regulation)
│   │       → Bait ban = True (from zonal regulation)
│   │
│   └── Other Region 3 waterbodies (zonal rules only):
│       → Trout limit = 3 (from zonal regulation)
│       → Bait ban = True (from zonal regulation)
│
└── Other regions (provincial rules only):
    → Trout limit = 5 (from provincial regulation)
```

---

## ImTributary lookup**: Determine which waterbodies include this segment via tributary relationships
3. **Collect regulations**: Gather provincial + zonal + inherited (from parent waterbodies) + waterbody-specific + rule-scoped regulations
4. **Deduplicate by rule type + species**: Group rules that regulate the same thing
5. **Apply precedence**: Keep most specific rule for each type+species combination
6
The query system must:

1. **Spatial lookup**: Determine which zone the segment is in
2. **Collect regulations**: Gather provincial + zonal + waterbody + rule-scoped regulations
3. **Deduplicate by rule type + species**: Group rules that regulate the same thing
4. **Apply precedence**: Keep most specific rule for each type+species combination
5. **Return final ruleset**: Present consolidated rules to user

### Storage Model

```python
segment_query_result = {
    "segment_id": "fwa_thompson_downstream_001",
    "location": "Thompson River downstream of Kamloops Lake",
    "parent_waterbodies": ["Fraser River"],  # Inherits from these
    "final_rules": [
        {
            "type": "QUOTA",
            "species": "Trout",
            "limit": 1,
            "source": "THOMPSON RIVER downstream of Kamloops Lake (rule-scoped)",
            "precedence_chain": [
                {"source": "Provincial", "limit": 5, "level": 1, "overridden": True},
                {"source": "Region 3", "limit": 3, "level": 2, "overridden": True},
                {"source": "FRASER RIVER [inherited]", "limit": 4, "level": 3, "overridden": True},
                {"source": "THOMPSON RIVER", "limit": 2, "level": 4, "overridden": True},
                {"source": "THOMPSON RIVER downstream of Kamloops Lake", "limit": 1, "level": 5, "active": True}
            ]
        },
        {
            "type": "SIZE_LIMIT",
            "species": "Trout",
            "min_size": 30,
            "source": "FRASER RIVER [inherited]",
            "precedence_chain": [
                {"source": "FRASER RIVER [inherited]", "min_size": 30, "level": 3, "active": True}
            ]
        },
        {
            "type": "QUOTA",
            "species": "Salmon",
            "limit": 1,
            "source": "THOMPSON RIVER (waterbody-specific)",
            "precedence_chain": [
                {"source": "THOMPSON RIVER", "limit": 1, "level": 4, "active": True}
            ]
        },
        {
            "type": "BAIT_BAN",
            "species": "ALL",
            "source": "Region 3 (zonal)",
            "precedence_chain": [
                {"source": "Region 3", "level": 2
                {"source": "Region 3", "active": True}
            ]
        }
    ]
}
```

### Benefits of Showing Precedence Chain

- **User understanding**: User sees why a rule applies
- **Transparency**: Clear rule resolution logic
- **Debugging**: Easy to spot errors in precedence
- **Educational**: Helps anglers learn regulation structure

---

## Conflict Resolution Rules

### Rule 1: Same Rule Type + Species + Location

**Most specific scope wins**.

Example:
- Provincial: Trout limit = 5 (entire BC)
- Waterbody: Trout limit = 2 (Thompson River)
- Result: Thompson River gets limit = 2 (waterbody overrides provincial)

### Rule 2: Different Rule Types

**No conflict - both apply**.

Example:
- Zonal: Bait ban (Region 3)
- Waterbody: Trout limit = 2 (Thompson River)
- Result: Thompson River has bait ban AND trout limit = 2

### Rule 3: Different Species

**No conflict - both apply**.

Example:
- Waterbody: Trout limit = 2
- Waterbody: Salmon limit = 1
- Result: Both limits apply

### Rule 4: Partial Overlap in Scope

**Split into distinct regions, apply appropriate rule to each**.

Example:
- Waterbody: Thompson River → Trout limit = 2
- Rule-scoped: Thompson River downstream of Kamloops Lake → Trout limit = 1

Result:
- Upstream of Kamloops Lake: Trout limit = 2 (waterbody rule)
- Downstream of Kamloops Lake: Trout limit = 1 (rule-scoped overrides waterbody)

### Rule 5: Contradictory Rules at Same Level

**Error - should not happen, requires manual review**.

Example:
- Waterbody rule 1: Trout limit = 2
- Waterbody rule 2: Trout limit = 3 (same location, same scope)

This indicates a data quality issue in the regulation parsing or database.

---

## Special Cases

### Case 1: Zonal Regulation Overrides Provincial, But Waterbody Has No Specific Rule

**Result**: Zonal rule applies to waterbody.

**Example**:
- Provincial: Bait allowed (implicit)
- Zone: Bait banned (Region 3)
- Waterbody: Thompson River (no bait rule specified)

**Result**: Thompson River has bait ban from zonal regulation.

**Reasoning**: Waterbody inherits zonal rule, which overrides provincial.

### Case 2: Waterbody Regulation Applies Outside Its Zone

**Rare but possible** (waterbody crosses zone boundary).

**Handling**:
- Each segment gets rules from its own zone
- Waterbody-specific rules still apply
- Precedence: waterbody > zonal > provincial (waterbody is more specific than zone)

**Example**:
- Thompson River crosses from Region 3 to Region 4
- Segment in Region 3: Gets Region 3 zonal rules + Thompson River waterbody rules
- Segment in Region 4: Gets Region 4 zonal rules + Thompson River waterbody rules

**Conflict Resolution**:
- If Region 3 has "Trout limit = 3" and Thompson River has "Trout limit = 2":
  - Region 3 segments: Trout limit = 2 (waterbody overrides zonal)
- If Region 4 has "Trout limit = 4" and Thompson River has "Trout limit = 2":
  - Region 4 segments: Trout limit = 2 (waterbody overrides zonal)

### Case 3: Exclusions via Precedence Override (Hypothesis - Requires Testing)

**Hypothesis**: Some exclusions might be handled automatically through the precedence hierarchy if certain conditions hold.

**Example**:
```
FRASER RIVER [including tributaries] - Trout limit = 4
EXCEPT: Thompson River (see separate entry)

THOMPSON RIVER [including tributaries] - Trout limit = 2 (separate regulation)
```

**Potential Approach**:
- Thompson segments inherit Fraser rules (level 3)
- Thompson's own regulation (level 4) overrides inherited rules
- Result: Thompson gets limit = 2, Fraser's limit = 4 doesn't apply

**CRITICAL ASSUMPTION (Needs Validation)**:

This only works if waterbodies with separate entries are **always exceptions** that fully replace inherited rules, never additions that supplement them.

**Counter-example that breaks this approach**:
```
FRASER RIVER [including tributaries] - Trout limit = 4, Bait ban
THOMPSON RIVER [including tributaries] - Salmon limit = 1 (separate entry, NOT an exception)
```

If Thompson should get:
- Trout limit = 4 (inherited from Fraser) ✓
- Salmon limit = 1 (from Thompson) ✓  
- Bait ban (inherited from Fraser) ✓

Then the precedence override approach fails - Thompson would lose Fraser's rules.

**Required Analysis**:
1. Extract all regulations with exclusions that reference "see separate entry"
2. Compare excluded waterbody's rules to parent waterbody's rules
3. Determine if excluded waterbody:
   - **Replaces** parent rules (same rule types → exception case, precedence works)
   - **Supplements** parent rules (different rule types → additive case, precedence fails)
4. Calculate % of cases where precedence override would work correctly

**If hypothesis fails**: Explicit exclusion processing is required. Precedence approach won't work.

**If hypothesis holds**: Could simplify ~60-80% of exclusions, but still need explicit processing for cases without separate entries.

### Case 4: Rule-Scoped Addition Outside Global Scope

**Example**:
- Waterbody: Thompson River [including tributaries]
- Global scope: 5,000 segments
- Rule: "Kokanee limit = 5 in Adams Lake only"

**Handling**:
- Adams Lake NOT in Thompson River tributaries
- Segment receives ONLY this rule (not other Thompson River rules)
- Still inherits provincial + zonal rules

**Result**:
- Adams Lake segment gets:
  - Provincial rules
  - Zonal rules (for its zone)
  - Kokanee limit = 5 (from Thompson River regulation)
- Adams Lake does NOT get:
  - Other Thompson River rules (trout limit, salmon limit, etc.)

**Reasoning**: Rule-scoped addition is independent of global scope.

### Case 5: Rule-Scoped Restriction Within Waterbody

**Example**:
- Waterbody: Thompson River [including tributaries] → Trout limit = 2
- Rule: "Trout limit = 1 downstream of Kamloops Lake"

**Handling**:
- Upstream segments: Trout limit = 2 (waterbody rule)
- Downstream segments: Trout limit = 1 (rule-scoped overrides waterbody)

**Reasoning**: Rule-scoped is more specific than waterbody, so overrides within its scope.

---

## Query-Time Algorithm
waterbody = segment.waterbody
    
    # 2. Find parent waterbodies (for inheritance)
    parent_waterbodies = find_parent_waterbodies_with_tributary_scope(segment_id)
    
    # 3. Collect all applicable regulations
    provincial_regs = get_provincial_regulations()
    zonal_regs = get_zonal_regulations(zone)
    inherited_regs = []
    for parent in parent_waterbodies:
        inherited_regs.extend(get_waterbody_regulations(parent.id))
    waterbody_regs = get_waterbody_regulations(waterbody.id)
    rule_scoped_regs = get_rule_scoped_regulations(segment_id)
    
    # 4. Flatten to individual rules
    all_rules = []
    all_rules.extend([(r, "provincial", 1) for r in provincial_regs])
    all_rules.extend([(r, "zonal", 2) for r in zonal_regs])
    all_rules.extend([(r, "inherited", 3) for r in inherited_regs])
    all_rules.extend([(r, "waterbody", 4) for r in waterbody_regs])
    all_rules.extend([(r, "rule_scoped", 5) for r in rule_scoped_regs])
    
    # 5. Group by (rule_type, species)
    rule_groups = defaultdict(list)
    for rule, source_level, precedence in all_rules:
        key = (rule.type, rule.species)
        rule_groups[key].append((rule, source_level, precedence))
    
    # 6. Apply precedence for each group
    final_rules = []
    
    for key, rules in rule_groups.items():
        if len(rules) == 1:
            # Only one rule - no conflict
            final_rules.append(rules[0][0])
        else:
            # Multiple rules - apply precedence (higher number = more specific)
            sorted_rules = sorted(rules, 
                                key=lambda x: x[2],  # Sort by precedence number
                                reverse=True)
            final_rule = sorted_rules[0][0]  # Highest precedence
            
            # Build precedence chain
            final_rule.precedence_chain = [
                {
                    "source": source_level,
                    "rule": rule,
                    "level": precedence,
                    "active": (precedence == sorted_rules[0][2])
                }
                for rule, source_level, precedence in sorted(sorted_rules, key=lambda x: x[2])
            ]
            
            final_rules.append(final_rule)
    
    return final_rules

def find_parent_waterbodies_with_tributary_scope(segment_id):
    """
    Find all waterbodies that include this segment via tributary relationships
    
    Example: Thompson River segment → finds Fraser River if Fraser regulation
             has "includes tributaries" and Thompson is a tributary of Fraser
    """
    segment = get_segment(segment_id)
    waterbody = segment.waterbody
    parents = []
    
    # Walk up the stream network to find parent waterbodies
    current = waterbody
    while current.parent_waterbody:
        parent = current.parent_waterbody
        
        # Check if parent has a regulation that includes tributaries
        parent_regulation = get_waterbody_regulation(parent.id)
        if parent_regulation and parent_regulation.global_scope.includes_tributaries:
         inherited": ["wb_fraser_001"],  # Thompson is a tributary of Fraser
        "   parents.append(parent)
        
        current = parent
    
    return parente.precedence_chain = [
                {
                    "source": source_level,
                    "rule": rule,
                    "active": (source_level == sorted_rules[0][1])
                }
                for rule, source_level in sorted_rules
            ]
            
            final_rules.append(final_rule)
    
    return final_rules
```

### Optimization: Precomputed Indexes

For query performance, precompute segment-to-regulation mappings:

```python
# Build index during regulation processing
segment_index = {
    "fwa_segment_001": {
        "provincial": ["prov_001", "prov_002"],
        "zonal": ["zone_r3_001"],
        "waterbody": ["wb_thompson_001"],
        "rule_scoped": ["rule_thompson_downstream_001"]
    }
}

# Query time: O(1) lookup + O(R) rule resolution (R = number of rules)
def get_regulations_for_segment_fast(segment_id):
    regulation_ids = segment_index[segment_id]
    all_rules = []
    precedence_map = {
        "provincial": 1,
        "zonal": 2,
        "inherited": 3,
        "waterbody": 4,
        "rule_scoped": 5
    }
    
    for level in ["provincial", "zonal", "inherited", "waterbody", "rule_scoped"]:
        for reg_id in regulation_ids.get(level, []):
            regulation = get_regulation(reg_id)
            all_rules.extend([(r, level, precedence_map[level](reg_id)
            all_rules.extend([(r, level) for r in regulation.rules])
    
    # Apply precedence (same as above)
    return apply_precedence(all_rules)
```

---

## Example Scenarios

### Scenario 1: Simple Override

**Setup**:
- Provincial: Trout limit = 5
- Zonal (Region 3): Trout limit = 3
- Query: Segment in Region 3, no waterbody-specific regulation

**Result**: Trout limit = 3 (zonal overrides provincial)

**Precedence chain**:
1. Provincial: 5 (overridden)
2. Zonal: 3 (active)

### Scenario 2: Multiple Overrides

**Setup**:
- Provincial: Trout limit = 5
- Zonal (Region 3): Trout limit = 3
- Waterbody (Thompson River): Trout limit = 2
- Query: Segment in Thompson River, Region 3

**Result**: Trout limit = 2 (waterbody overrides zonal overrides provincial)

**Precedence chain**:
1. Provincial: 5 (overridden)
2. Zonal: 3 (overridden)
3. Waterbody: 2 (active)

### Scenario 3: Different Rule Types

**Setup**:
- Provincial: Trout limit = 5
- Zonal (Region 3): Bait ban
- Waterbody (Thompson River): Trout limit = 2
- Query: Segment in Thompson River, Region 3

**Result**:
- Trout limit = 2 (waterbody overrides provincial)
- Bait ban = True (zonal, no conflict)

**Reasoning**: Different rule types, no conflict.

### Scenario 4: Partial Waterbody Scope

**Setup**:
- Provincial: Trout limit = 5
- Waterbody (Thompson River): Trout limit = 2
- Rule-scoped (Thompson River downstream of Kamloops Lake): Trout limit = 1
- Query A: Segment upstream of Kamloops Lake
- Query B: Segment downstream of Kamloops Lake

**Result A**: Trout limit = 2 (waterbody rule applies, no rule-scoped)

**Result B**: Trout limit = 1 (rule-scoped overrides waterbody)

### Scenario 5: Rule Addition Outside Global Scope

**Setup**:
- Waterbody (Thompson River): Trout limit = 2, Salmon limit = 1
- Rule-scoped (Adams Lake only): Kokanee limit = 5
- Query: Segment in Adams Lake

**Result**:
- Kokanee limit = 5 (rule-scoped)
- Trout limit = 5 (provincial - Thompson River rules don't apply)

**Reasoning**: Adams Lake not in Thompson River tributaries, so only gets the specific rule added for it plus provincial/zonal defaults.

---

## Edge Cases

### Edge Case 1: Same Rule Type at Same Level

**Problem**: Two rules at same level (e.g., two waterbody rules) for same species.

**Example**:
- Waterbody rule 1: Trout limit = 2
- Waterbody rule 2: Trout limit = 3

**Handling**: This is a **data error**. Flag for manual review.

**Temporary resolution**: Use first rule, log warning.

### Edge Case 2: Circular Precedence

**Problem**: Shouldn't be possible with hierarchy, but could occur with complex rule-scoped additions.

**Example**:
- Regulation A adds rule to Regulation B's scope
- Regulation B adds rule to Regulation A's scope

**Handling**: Detect cycles, flag for manual review.

### Edge Case 3: Regulation Applies Outside Its Declared Zone

**Example**:
- Regulation declared for "Region 3"
- Waterbody (Thompson River) crosses into Region 4
- Should Region 4 segments get the regulation?

**Handling**: Yes - waterbody-specific regulations apply regardless of zone boundary crossings. Zone is just administrative metadata.

### Edge Case 4: Provincial Rule with Species Not in Lower Levels

**Example**:
- Provincial: Sturgeon limit = 2
- Zonal/Waterbody: No sturgeon rules

**Handling**: Provincial rule applies (no override).

**Note**: Common case - provincial provides baseline, lower levels only override when needed.

### Edge Case 5: Rule Removal via Override to NULL

**Question**: Can a waterbody regulation "remove" a provincial regulation?

**Example**:
- Provincial: Trout limit = 5
- Waterbody: Trout limit = NULL (no restriction)

**Handling**: Depends on interpretation:
- Option 1: NULL means "unspecified" → provincial rule applies
- Option 2: NULL means "no limit" → overrides provincial with unlimited

**Recommended**: Option 2 - explicit NULL in waterbody regulation means "no limit" for that waterbody.

---

## UI/UX Considerations

### Displaying Regulations to Users

**Bad (confusing)**:
```
Regulations for Thompson River downstream of Kamloops Lake:
- Provincial: Trout limit = 5
- Region 3: Trout limit = 3
- Thompson River: Trout limit = 2
- Downstream section: Trout limit = 1
```
User sees conflicting rules and doesn't know which applies.

**Good (consolidated)**:
```
Regulations for Thompson River downstream of Kamloops Lake:
- Trout: Daily limit = 1
- Salmon: Daily limit = 1
- Bait: Prohibited

[Show precedence chain]
```

**Best (with explanation)**:
```
Regulations for Thompson River downstream of Kamloops Lake:

Trout: Daily limit = 1
  ↳ Downstream of Kamloops Lake restriction (most specific)
  ↳ Overrides: Thompson River (2), Region 3 (3), Provincial (5)

Salmon: Daily limit = 1
  ↳ Thompson River regulation

Bait: Prohibited
  ↳ Region 3 regulation
```

### Showing Warnings

When a regulation has warnings (e.g., MVP scope simplification):

```
Regulations for Thompson River (all segments):

⚠️ Warning: Regulation specifies "downstream of Kamloops Lake" but is 
           applied to entire Thompson River system in current version.
           Verify before fishing.

Trout: Daily limit = 1 (may not apply to entire river)
```

---

## Summary

**Key Takeaways**:

1. **Five-level hierarchy**: Provincial → Zonal → Inherited → Waterbody-specific → Rule-scoped
2. **More specific wins**: Lower levels override higher levels
3. **Inheritance through tributaries**: Waterbodies inherit from parent waterbodies that include tributaries
4. **Exclusions via precedence**: Many exclusions can be handled automatically through the precedence hierarchy rather than explicit segment removal
5. **No conflict for different types/species**: Multiple rules can coexist
6. **Precedence chain is important**: Show users why a rule applies
7. **Edge cases exist**: Handle circular references, cross-zone waterbodies, null overrides

**Critical Implementation Strategy**:

For exclusions, the **precedence override approach is unproven** and requires validation:
- Hypothesis: Waterbodies with separate entries fully replace (not supplement) parent rules
- If true: ~60-80% of exclusions could be handled via precedence automatically
- If false: All exclusions require explicit segment removal processing
- **Action**: Analyze actual regulation data to validate before implementing

This could dramatically simplify exclusion processing if the hypothesis holds, but needs empirical validation against real regulation text.

For implementation details, see:
- MVP implementation: [MVP_LINKING_IMPLEMENTATION.md](MVP_LINKING_IMPLEMENTATION.md)
- Post-MVP phases: [POST_MVP_ROADMAP.md](POST_MVP_ROADMAP.md)
