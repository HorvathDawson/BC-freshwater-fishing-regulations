# Section pipeline tests

Tests for the v2 `pipeline/sections/` package. One file per module (mirrors the module
names). All tests are `@pytest.mark.skip` scaffolds until the corresponding module is
implemented — remove the skip as you build each step (test-as-you-build; see
`pipeline/redesign/10-testing-plan.md`).

Two regression tests encode the known hard cases and gate deleting legacy logic:
- `test_topology.py::test_chehalis_harrison_braiding_no_leak` — SCC condensation must stop
  Harrison being seen as a tributary of the Chehalis.
- `test_topology.py::test_kootenay_columbia_lake_barrier` — collapsing Columbia Lake to a
  barrier node must sever the `2300` bridge (blk 356366076) with no edge_type rule.

Fixtures: build SMALL synthetic graphs in-code where possible (as `test_tributary_logic.py`
does today); use a tiny real-data extract only for the two named regression cases.
