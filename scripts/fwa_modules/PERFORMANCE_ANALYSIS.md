# Graph Builder Performance Analysis

## Overview
Analysis of `graph_builder.py` performance bottlenecks based on execution with full BC dataset (~4.6M nodes, ~4.7M edges).

## Execution Time Breakdown

### Total Runtime: ~62 minutes

| Phase | Time | % of Total | Description |
|-------|------|-----------|-------------|
| Step 1: Build Graph | ~4 min | 6% | Parallel layer processing (14 workers) |
| Step 1.5: Preprocess | ~2 min | 3% | Remove spurious order-1 tailwater edges |
| **Step 1.6: Filter Unnamed** | **~52 min** | **84%** | **MAJOR BOTTLENECK** |
| Step 2: Enrich Tributaries | ~8 sec | <1% | DFS traversal for tributary relationships |
| Export (Pickle) | ~13 sec | <1% | Binary serialization |
| Export (GraphML) | ~7.5 min | 12% | XML serialization |

---

## The Critical Bottleneck: filter_unnamed_depth()

This single function takes **~52 minutes** (84% of total runtime).

### Phase-by-Phase Breakdown

#### Phase 1: Collect Named Stream Anchors (~3 seconds)
- **What it does:** Finds all edges with `gnis_name` attribute
- **Performance:** ✅ FAST - simple iteration
- **Result:** ~786,000 named stream edges

#### Phase 2: Measure Distances from Named Streams (~26 minutes)
**Total: 26 minutes broken down as:**

**2a. Build Edge Data Cache (~6 seconds)**
- Creates dictionary of edge attributes for O(1) lookup
- ✅ Fast and necessary optimization

**2b. Build Predecessor Cache (~25.5 minutes) - BOTTLENECK #1**
- **What it does:** For each of 4.6M nodes, calls `G.predecessors(node)`
- **Why it's slow:**
  ```python
  # This single line takes 25 minutes:
  pred_cache = {node: list(self.G.predecessors(node)) for node in self.G.nodes()}
  ```
- **The Problem:**
  - NetworkX `predecessors()` is not cached/indexed
  - Each call traverses the graph's internal edge dictionary
  - 4.6 million expensive graph lookups
  - Rate: ~3,000 nodes/second (should be 100,000+)

- **Why we need it:**
  - The BFS phase calls `predecessors()` millions of times
  - Without caching, BFS would take **hours** instead of minutes
  - This is trading 25 min of cache building for faster BFS

**2c. Initialize BFS Queue (<1 second)**
- Adds 786K named edges to queue
- ✅ Fast

**2d. BFS Traversal Upstream (~30 seconds)**
- Processes 4.4M edge traversals
- ✅ Relatively fast because predecessor cache is used
- Rate: ~146,000 edges/second

#### Phase 2.5: Correct Distances (~38 seconds)
- Ensures network consistency
- BFS from headwaters to propagate minimum distances
- ✅ Reasonable performance

#### Phase 3: Mark Edges for Removal (~10 seconds)
- Iterates through computed distances
- Marks 2M edges exceeding threshold
- ✅ Fast

#### Phase 4: Upstream Expansion BFS (~27.5 minutes) - BOTTLENECK #2
- **What it does:** Starting from 2M marked edges, finds ALL upstream segments
- **Why it's slow:**
  ```python
  # For each marked edge, find all upstream edges recursively
  while queue:
      u, v, key = queue.popleft()
      for pred_u in pred_cache.get(u, []):  # Uses cached predecessors
          for pred_key in self.G[pred_u][u]:  # But still must query graph
              # Add to removal set and queue
  ```
- **The Problem:**
  - Must traverse ~2M edges upstream
  - Each edge requires checking predecessors and their edge keys
  - Graph dictionary access `self.G[pred_u][u]` is expensive at scale
  - Even with predecessor cache, the edge key lookup is slow

- **Performance:**
  - Rate: ~1,200-1,500 edges/second
  - Should be: 50,000+ edges/second
  - This suggests the `self.G[pred_u][u]` dictionary access is the bottleneck

#### Phase 5: Export Removed Edge Data (~1 minute)
- Builds JSON export of 2.1M removed edges
- Mostly JSON serialization time
- ✅ Acceptable

#### Phase 6: Remove Edges from Graph (~16 seconds)
- NetworkX edge and node removal
- ✅ Fast enough

---

## Root Causes

### 1. NetworkX MultiDiGraph Performance at Scale
- **Not designed for millions of nodes/edges**
- Internal data structure is nested dictionaries: `{node1: {node2: {key: data}}}`
- Every graph operation requires dictionary traversals
- No indexing, no optimization for bulk operations

### 2. Python Iteration Overhead
- Millions of iterations through Python loops
- Each loop has interpreter overhead
- No vectorization or compiled code paths

### 3. No Reverse Edge Index
- Graph stores edges forward, but we need to traverse backward (upstream)
- `predecessors()` must scan all edges to find reverse connections
- A reverse index built once would make this O(1)

---

## Optimization Opportunities (Not Yet Implemented)

### High Impact (Could reduce 50min → 10min)

#### 1. Build Reverse Edge Index During Graph Construction
```python
# Instead of computing predecessors on demand, build once:
class FWAPrimalGraph:
    def __init__(self):
        self.G = nx.MultiDiGraph()
        self.reverse_edges = {}  # {node: [(pred_u, edge_key), ...]}
    
    # Update when adding edges:
    def add_edge(self, u, v, key, **attrs):
        self.G.add_edge(u, v, key, **attrs)
        if v not in self.reverse_edges:
            self.reverse_edges[v] = []
        self.reverse_edges[v].append((u, key))
```
**Impact:** Eliminates 25-minute predecessor cache building

#### 2. Use Edge List Instead of Graph for BFS
```python
# Store edges as tuples instead of graph structure
edges = [(u, v, key, attrs) for u, v, key, attrs in G.edges(keys=True, data=True)]
# Build adjacency using defaultdict
from collections import defaultdict
upstream_edges = defaultdict(list)
for u, v, key, attrs in edges:
    upstream_edges[v].append((u, key, attrs))
```
**Impact:** Faster iteration, less overhead

#### 3. Use NumPy/Scipy Sparse Matrices
```python
# Convert graph to scipy sparse CSR matrix
# Use matrix operations for BFS instead of Python loops
```
**Impact:** 10-100x faster for numerical operations

### Medium Impact (Could reduce by 5-10min)

#### 4. Parallelize Phase 4 BFS
- Split marked edges into chunks
- Process each chunk in separate process
- Merge results
**Challenge:** Need thread-safe graph access

#### 5. Use Numba/Cython for Hot Loops
- Compile BFS loops to machine code
- Eliminate Python interpreter overhead

### Low Impact (Optimizations already good)

- Phase 1: Already fast ✅
- Phase 2a: Cache is necessary ✅
- Phase 3: Simple iteration, can't optimize much ✅

---

## Comparison to Other Approaches

### Current: NetworkX MultiDiGraph
- **Pros:** Easy to use, flexible, supports multiple edges
- **Cons:** Slow at scale, high memory usage
- **Best for:** <100K edges

### Alternative: Graph Database (Neo4j)
- **Pros:** Optimized indexes, efficient traversals
- **Cons:** External dependency, complexity
- **Speedup:** 10-100x for large graphs

### Alternative: igraph
- **Pros:** C-based, very fast
- **Cons:** Less flexible than NetworkX, harder to debug
- **Speedup:** 5-20x

### Alternative: Custom Edge List + Dictionaries
- **Pros:** Full control, can optimize for our exact use case
- **Cons:** More code to maintain
- **Speedup:** 3-10x

---

## Recommended Next Steps

### Immediate (Quick Wins)
1. ✅ **Add comprehensive logging** (DONE in this update)
2. Build reverse edge index during graph construction
3. Profile with `cProfile` to confirm bottlenecks

### Short-term
4. Benchmark alternative graph libraries (igraph, graph-tool)
5. Implement edge list + dictionary approach for BFS operations
6. Consider chunking/streaming for very large datasets

### Long-term
7. Evaluate graph database for production use
8. Implement spatial indexing for geographic queries
9. Consider pre-computing common queries offline

---

## Performance Expectations After Optimization

| Current | Optimized (Conservative) | Optimized (Aggressive) |
|---------|-------------------------|------------------------|
| 62 min | 20-25 min | 5-10 min |

**Conservative:** Reverse edge index + minor optimizations  
**Aggressive:** Complete rewrite with igraph or custom data structures
