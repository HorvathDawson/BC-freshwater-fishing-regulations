# Water-Specific Regulations tables — how to read them

Transcribed verbatim from the 2025-2027 synopsis. **Source text.** This is the key to the
per-water tables, which are the other ~3,000 rules in the corpus.

---

## 1 Water-Specific Regulations

This column lists waters with restrictions not covered by the Regional Regulations.

* An **asterisk** symbol means the regulation applies to **tributary streams** as well.
* A **fish** symbol means the lake is **stocked**. Not all stocked lakes are listed.
* A **(CW)** symbol means that this is a **Classified Water**.

## 2 Management Unit (M.U.)

> This column lists one or more Management Unit's **only as a reference** to help you locate your
> target lake or stream and to distinguish waters in a region with the same name. **Not all
> applicable M.U.'s may be listed.**

## 3 Exceptions to the Regional Regulations

**Catch and Release:** You may fish for the named species, but you must release any that you catch.

**Tributaries:** when **all** regulations cited apply to both the named body of water and its
tributaries, an asterisk is placed in the **first column**. When only **some** regulations apply to
the tributaries then an asterisk is placed **after the relevant regulation** cited in this column.

**No fishing for:** **you may not deliberately fish for the species named even if your intention is
to release** any fish that you may catch. If you accidentally catch a fish of the species named,
you must release it as quickly and carefully as possible.

**Bait Ban:** the use of natural bait is prohibited in waters with a bait ban. Bait may be banned
year round or seasonally. **During the period when bait is banned it is banned for all angling and
for all species.**

**Single Hook:** it is prohibited to angle with a hook with more than one point on waters that are
subject to a single hook regulation. **Where single hook regulations are in place on a water body
it applies to angling for all species.** Often combined with barbless and termed "single barbless
hook".

**Barbless Hook:** it is prohibited to use a hook with a barb on waters subject to a barbless hook
regulation. **Where barbless hook regulations are in place on a water body, it applies to angling
for all species.** Often combined with single hook.

**Dates:** regulations may apply for all or part of the year. **When no date is listed, the
regulations apply all year. Start and end dates are inclusive.**

## Youth/Disabled Accompanied Waters

**Authorized Angler:** A person that is **under 16 years of age or a disabled resident**.

**Companion:** A person who accompanies and attends an authorized angler. A person must not angle
in a Youth/Disabled Accompanied Water unless the person is an authorized angler or a companion to
an authorized angler. **An authorized angler can be accompanied by up to two companion anglers.**

## Boating Regulations

* **No angling from boats:** you may use a boat or other floating device for transportation in
  these waters, but you may not angle from that boat.
* **No angling from powered boats:** you are not allowed to angle from a boat equipped with a motor.
* **No powered boats:** boat motors of all types (internal combustion, steam and electric) are
  prohibited.
* **Electric motor only:** you may use only battery-powered electric motors — **max 7.5 kW**. All
  other types of motors are prohibited. In addition, wind or human propelled craft may be used.
* **Engine power regulations:** boat motors cannot exceed the engine power (given in kilowatts)
  listed in the "Exceptions" column.
* **Speed regulations:** boats equipped with motors cannot exceed the speed limit listed.
* **No towing:** do not tow a person on water skis, a surf board or other water toy.
* **No vessels:** boats and rafts of all types are prohibited.

> **Please note:** most boating regulations are the responsibility of Government of Canada, Marine
> Transportation. They are published here as a courtesy to anglers but, **due to space limitations,
> may not be complete.**

> All anglers of any age must comply with all regulations set out in this Synopsis **as well as any
> in-season changes as made public by the Ministry. The regulations described in this Synopsis do
> not apply to tidal waters.**

---

## What this settles

| the page says | the model |
|---|---|
| *"**Catch and Release:** you may fish for the named species, but must release"* | `take=0, may_target=true` |
| *"**No fishing for:** you may not **deliberately fish** for the species named **even if your intention is to release**"* | `take=0, may_target=false` — **and it is SPECIES-SCOPED, not a water closure.** This is the printed definition of the distinction, and it settles the kokanee reading: *"Kokanee: none from streams"* is no-targeting. |
| *"bait is banned **for all angling and for all species**"* · single hook and barbless *"applies to angling for **all species**"* | confirms `bait_restriction` and `tackle_restriction` **must refuse a species field**. The 20 rules carrying leaked codes are not merely untidy — the page says they cannot be species-scoped. |
| *"When no date is listed, the regulations apply **all year**. Start and end dates are **inclusive**."* | `windows: []` means all year, not "unknown". Ranges are closed intervals. |
| asterisk in the **first column** = all rules reach tributaries; asterisk **after one regulation** = only that one does | this is exactly `Entry.includes_tributaries` vs per-rule `includes_tributaries`. Both already exist; the page confirms the distinction is real and which is which. |
| *"**(CW)** means this is a Classified Water"* | the symbol join — the fact lives on the entry, the obligation on the rule |
| *"Not all applicable M.U.'s may be listed"* | MU is a **locator**, never an extent. Never bind a rule to a water by its MU column. |
| *"Electric motor only — **max 7.5 kW**"* is part of the DEFINITION | the 161 rules that repeat "max 7.5 kW" are restating the definition, not setting a per-water cap. `electric_only` implies 7.5 kW; a rule needs `max_power_kw` only when it differs. |
| *"most boating regulations are the responsibility of … Marine Transportation … may not be complete"* | vessel rules are **federal and knowingly incomplete**. Absence of a vessel rule is not evidence there is none. |
| *"Authorized Angler: under 16 **or a disabled resident**"* · up to **two** companions | `program_membership` needs `disabled` as an angler class, and a companion count |
| *"do not apply to **tidal waters**"* | the corpus boundary, and why the 11 tidal-licence rules have no home |
