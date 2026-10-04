//! Experimental compact port of Supercluster 8.0.1 / KDBush 4.1.0.
//! See UPSTREAM-LICENSES. Not wired to the public tiling engine yet.
//!
//! Raw rows keep f32 projected coordinates + category code, NOT Feature objects.
//! Level entries are source ordinals or arena references. Arena references stay
//! live across unchanged levels; consumed cluster slots are recycled after the
//! pass, so dropping a previous level cannot invalidate a surviving entry.
//! Only current/next levels are held. Callbacks must consume output synchronously.
//! Original singleton geometry/properties must be recovered by source ordinal;
//! projected coordinates returned here are for clustering, not original output.
mod kdbush;
use kdbush::KdBush;

const CLUSTER: u32 = 1 << 31;
const JS_SAFE: u64 = (1 << 53) - 1;
const EXTENT: f64 = 512.0;

fn reserve<T>(v: &mut Vec<T>, n: usize) -> Result<(), String> {
    v.try_reserve(n)
        .map_err(|e| format!("Clustering allocation failed: {e}"))
}
fn push<T>(v: &mut Vec<T>, value: T) -> Result<(), String> {
    reserve(v, 1)?;
    v.push(value);
    Ok(())
}

pub struct Input {
    coords: Vec<[f32; 2]>,
    categories: Vec<u8>,
    width: usize,
}
impl Input {
    pub fn new(category_count: usize) -> Result<Self, String> {
        if !(1..=64).contains(&category_count) {
            return Err("Expected 1..64 categories".into());
        }
        Ok(Self {
            coords: Vec::new(),
            categories: Vec::new(),
            width: category_count + 1,
        })
    }
    pub fn reserve_exact(&mut self, n: usize) -> Result<(), String> {
        if n >= CLUSTER as usize {
            return Err("Clustering currently supports fewer than 2^31 rows".into());
        }
        self.coords
            .try_reserve_exact(n.saturating_sub(self.coords.len()))
            .map_err(|e| e.to_string())?;
        self.categories
            .try_reserve_exact(n.saturating_sub(self.categories.len()))
            .map_err(|e| e.to_string())
    }
    pub fn push(&mut self, lon: f64, lat: f64, category: u8) -> Result<(), String> {
        if !lon.is_finite() || !lat.is_finite() || lon.abs() > 180.0 || lat.abs() > 90.0 {
            return Err("Expected finite geographic point coordinates".into());
        }
        if category as usize >= self.width {
            return Err("Invalid category code (last code is other)".into());
        }
        if self.len() >= CLUSTER as usize - 1 {
            return Err("Clustering input limit reached".into());
        }
        let sin = (lat * std::f64::consts::PI / 180.0).sin();
        let y =
            (0.5 - 0.25 * ((1.0 + sin) / (1.0 - sin)).ln() / std::f64::consts::PI).clamp(0.0, 1.0);
        reserve(&mut self.coords, 1)?;
        reserve(&mut self.categories, 1)?;
        self.coords.push([(lon / 360.0 + 0.5) as f32, y as f32]);
        self.categories.push(category);
        Ok(())
    }
    pub fn len(&self) -> usize {
        self.coords.len()
    }
    pub fn is_empty(&self) -> bool {
        self.coords.is_empty()
    }
    pub fn bytes(&self) -> usize {
        self.coords.capacity() * 8 + self.categories.capacity()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Options {
    pub min_zoom: u8,
    pub max_zoom: u8,
    pub radius: f64,
    pub min_points: u32,
    pub node_size: usize,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            min_zoom: 0,
            max_zoom: 16,
            radius: 40.0,
            min_points: 2,
            node_size: 64,
        }
    }
}
impl Options {
    pub(crate) fn validate(self) -> Result<(), String> {
        if self.min_zoom > self.max_zoom
            || self.max_zoom > 30
            || !self.radius.is_finite()
            || self.radius <= 0.0
            || self.min_points == 0
            || !(2..=65535).contains(&self.node_size)
        {
            return Err("Invalid Supercluster options".into());
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ProjectedNode {
    pub id: u64,
    pub x: f64,
    pub y: f64,
    pub count: u64,
    pub source: Option<u32>,
    pub expansion_zoom: Option<u8>,
}
#[derive(Clone, Copy)]
struct ClusterNode {
    id: u64,
    x: f64,
    y: f64,
    count: u64,
}
struct Arena {
    nodes: Vec<ClusterNode>,
    // Raw input is capped below 2^31 rows, so unweighted category counts fit.
    // Keep public counts and accumulation in u64; narrow only at this boundary.
    counts: Vec<u32>,
    free: Vec<u32>,
    width: usize,
}
impl Arena {
    fn bytes(&self) -> usize {
        self.nodes.capacity() * std::mem::size_of::<ClusterNode>()
            + self.counts.capacity() * std::mem::size_of::<u32>()
            + self.free.capacity() * 4
    }
    fn insert(&mut self, n: ClusterNode, counts: &[u64]) -> Result<u32, String> {
        let mut compact = [0u32; 65];
        for (target, &count) in compact[..self.width].iter_mut().zip(counts) {
            *target = u32::try_from(count)
                .map_err(|_| "Unweighted cluster category count exceeds u32 capacity")?;
        }
        let slot = if let Some(slot) = self.free.pop() {
            self.nodes[slot as usize] = n;
            slot
        } else {
            if self.nodes.len() >= CLUSTER as usize {
                return Err("Cluster arena limit reached".into());
            }
            let slot = self.nodes.len() as u32;
            reserve(&mut self.nodes, 1)?;
            reserve(&mut self.counts, self.width)?;
            self.nodes.push(n);
            self.counts.resize(self.counts.len() + self.width, 0);
            slot
        };
        self.counts[slot as usize * self.width..(slot as usize + 1) * self.width]
            .copy_from_slice(&compact[..self.width]);
        Ok(slot | CLUSTER)
    }
}
enum Refs {
    Source(usize),
    Level(Vec<u32>),
}
impl Refs {
    fn len(&self) -> usize {
        match self {
            Self::Source(n) => *n,
            Self::Level(v) => v.len(),
        }
    }
    fn get(&self, i: usize) -> u32 {
        match self {
            Self::Source(_) => i as u32,
            Self::Level(v) => v[i],
        }
    }
    fn bytes(&self) -> usize {
        match self {
            Self::Source(_) => 0,
            Self::Level(v) => v.capacity() * 4,
        }
    }
}
fn node(input: &Input, arena: &Arena, r: u32) -> ProjectedNode {
    if r & CLUSTER == 0 {
        let [x, y] = input.coords[r as usize].map(f64::from);
        ProjectedNode {
            id: r as u64,
            x,
            y,
            count: 1,
            source: Some(r),
            expansion_zoom: None,
        }
    } else {
        let n = arena.nodes[(r & !CLUSTER) as usize];
        ProjectedNode {
            id: n.id,
            x: n.x,
            y: n.y,
            count: n.count,
            source: None,
            expansion_zoom: Some(((n.id - input.len() as u64) % 32) as u8),
        }
    }
}
fn add_counts(input: &Input, arena: &Arena, r: u32, sums: &mut [u64]) {
    if r & CLUSTER == 0 {
        sums[input.categories[r as usize] as usize] += 1;
    } else {
        let start = (r & !CLUSTER) as usize * input.width;
        for (s, c) in sums
            .iter_mut()
            .zip(&arena.counts[start..start + input.width])
        {
            *s += u64::from(*c);
        }
    }
}

pub struct Level<'a> {
    input: &'a Input,
    arena: &'a Arena,
    refs: &'a Refs,
    tree: &'a KdBush,
}
impl Level<'_> {
    pub fn len(&self) -> usize {
        self.refs.len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn node(&self, i: usize) -> ProjectedNode {
        node(self.input, self.arena, self.refs.get(i))
    }
    pub fn category_count(&self, i: usize, k: usize) -> u64 {
        assert!(k < self.input.width);
        let r = self.refs.get(i);
        if r & CLUSTER == 0 {
            u64::from(self.input.categories[r as usize] as usize == k)
        } else {
            u64::from(self.arena.counts[(r & !CLUSTER) as usize * self.input.width + k])
        }
    }
    pub fn category_width(&self) -> usize {
        self.input.width
    }
    /// Normalized-world bounds; emits indexes into this level in KD order.
    pub fn range(&self, bounds: [f64; 4], visit: impl FnMut(u32)) {
        self.tree.range(bounds, visit);
    }
    /// Marks original rows that survived the finest cluster pass. The caller
    /// can reread immutable shards once to recover only these exact originals.
    pub fn singleton_bitset(&self) -> Result<Vec<u8>, String> {
        let mut bits = Vec::new();
        let bytes = (self.input.len() + 7) / 8;
        bits.try_reserve_exact(bytes).map_err(|e| e.to_string())?;
        bits.resize(bytes, 0);
        for i in 0..self.len() {
            if let Some(r) = self.node(i).source {
                bits[r as usize / 8] |= 1 << (r % 8);
            }
        }
        Ok(bits)
    }
}

#[derive(Default, Debug)]
pub struct Stats {
    /// Array capacities only, NOT process RSS. Includes input and pass scratch.
    pub peak_accounted_bytes: usize,
    pub levels: Vec<(u8, usize)>,
}
/// Checked equivalent of the upstream ID formula without JS's signed shift.
pub fn cluster_id(seed_index: u64, zoom: u8, source_count: u64) -> Result<u64, String> {
    if zoom > 30 {
        return Err("Cluster zoom exceeds ID encoding".into());
    }
    let id = seed_index
        .checked_mul(32)
        .and_then(|v| v.checked_add(zoom as u64 + 1))
        .and_then(|v| v.checked_add(source_count))
        .filter(|v| *v <= JS_SAFE)
        .ok_or("Cluster ID exceeds safe integer range")?;
    Ok(id)
}

/// Consume each level immediately; it becomes invalid when the callback returns.
pub fn build(
    input: &Input,
    options: Options,
    mut emit: impl FnMut(u8, &Level<'_>) -> Result<(), String>,
) -> Result<Stats, String> {
    run(input, options, options.max_zoom, false, |z, l, _| emit(z, l))
}
/// Include unclustered point levels above the clustering cutoff.
pub fn build_with_points(
    input: &Input,
    options: Options,
    max_zoom: u8,
    mut emit: impl FnMut(u8, &Level<'_>) -> Result<(), String>,
) -> Result<Stats, String> {
    run(input, options, max_zoom, false, |z, l, _| emit(z, l))
}
/// Diagnostic parent maps (previous-level position -> new-level position).
/// No parent arrays are allocated on the normal build path.
pub fn build_traced(
    input: &Input,
    options: Options,
    emit: impl FnMut(u8, &Level<'_>, &[u32]) -> Result<(), String>,
) -> Result<Stats, String> {
    run(input, options, options.max_zoom, true, emit)
}
fn run(
    input: &Input,
    options: Options,
    max_zoom: u8,
    trace: bool,
    mut emit: impl FnMut(u8, &Level<'_>, &[u32]) -> Result<(), String>,
) -> Result<Stats, String> {
    options.validate()?;
    if max_zoom < options.max_zoom || max_zoom > 30 {
        return Err("Output max_zoom must be between cluster_maxzoom and 30".into());
    }
    let mut arena = Arena {
        nodes: Vec::new(),
        counts: Vec::new(),
        free: Vec::new(),
        width: input.width,
    };
    let mut refs = Refs::Source(input.len());
    let mut tree = KdBush::new(input.coords.iter().copied(), options.node_size)?;
    let mut stats = Stats::default();
    stats.peak_accounted_bytes = input.bytes() + tree.bytes();
    for zoom in ((options.max_zoom + 1)..=max_zoom).rev() {
        emit(zoom, &Level { input, arena: &arena, refs: &refs, tree: &tree }, &[])?;
        stats.levels.push((zoom, refs.len()));
    }
    for zoom in (options.min_zoom..=options.max_zoom).rev() {
        let mut visited = Vec::new();
        visited
            .try_reserve_exact(refs.len())
            .map_err(|e| e.to_string())?;
        visited.resize(refs.len(), 0u8);
        let mut parents = Vec::new();
        if trace {
            parents
                .try_reserve_exact(refs.len())
                .map_err(|e| e.to_string())?;
            parents.resize(refs.len(), u32::MAX);
        }
        let mut next = Vec::new();
        let mut retired = Vec::new();
        // Only pre-existing slots can be retired during this pass. In the raw
        // base pass this is zero, even if many new clusters are created.
        retired
            .try_reserve_exact(arena.nodes.len())
            .map_err(|e| e.to_string())?;
        let r = options.radius / (EXTENT * 2f64.powi(zoom.into()));
        for i in 0..refs.len() {
            if visited[i] != 0 {
                continue;
            }
            visited[i] = 1;
            let seed_ref = refs.get(i);
            let seed = node(input, &arena, seed_ref);
            let mut count = seed.count;
            tree.within(seed.x, seed.y, r, |j| {
                if visited[j as usize] == 0 {
                    count += node(input, &arena, refs.get(j as usize)).count;
                }
            });
            let out_index = next.len() as u32;
            if trace {
                parents[i] = out_index;
            }
            if count > seed.count && count >= options.min_points as u64 {
                let mut sx = seed.x * seed.count as f64;
                let mut sy = seed.y * seed.count as f64;
                let mut sums = [0u64; 65];
                add_counts(input, &arena, seed_ref, &mut sums[..input.width]);
                if seed_ref & CLUSTER != 0 {
                    retired.push(seed_ref & !CLUSTER);
                }
                tree.within(seed.x, seed.y, r, |j| {
                    let j = j as usize;
                    if visited[j] != 0 {
                        return;
                    }
                    visited[j] = 1;
                    if trace {
                        parents[j] = out_index;
                    }
                    let reference = refs.get(j);
                    let n = node(input, &arena, reference);
                    sx += n.x * n.count as f64;
                    sy += n.y * n.count as f64;
                    add_counts(input, &arena, reference, &mut sums[..input.width]);
                    if reference & CLUSTER != 0 {
                        retired.push(reference & !CLUSTER);
                    }
                });
                let id = cluster_id(i as u64, zoom, input.len() as u64)?;
                let reference = arena.insert(
                    ClusterNode {
                        id,
                        x: sx / count as f64,
                        y: sy / count as f64,
                        count,
                    },
                    &sums[..input.width],
                )?;
                push(&mut next, reference)?;
            } else {
                push(&mut next, seed_ref)?;
                if count > 1 {
                    // Failed minPoints groups preserve upstream's neighbor ordering.
                    let mut failure = None;
                    tree.within(seed.x, seed.y, r, |j| {
                        let j = j as usize;
                        if visited[j] != 0 || failure.is_some() {
                            return;
                        }
                        visited[j] = 1;
                        if trace {
                            parents[j] = next.len() as u32;
                        }
                        if let Err(e) = push(&mut next, refs.get(j)) {
                            failure = Some(e);
                        }
                    });
                    if let Some(e) = failure {
                        return Err(e);
                    }
                }
            }
        }
        stats.peak_accounted_bytes = stats.peak_accounted_bytes.max(
            input.bytes()
                + tree.bytes()
                + refs.bytes()
                + next.capacity() * 4
                + visited.capacity()
                + parents.capacity() * 4
                + retired.capacity() * 4
                + arena.bytes(),
        );
        // Free old level/index BEFORE allocating the next index. References to
        // unchanged raw rows and live arena slots remain valid.
        drop(tree);
        drop(refs);
        drop(visited);
        reserve(&mut arena.free, retired.len())?;
        stats.peak_accounted_bytes = stats.peak_accounted_bytes.max(
            input.bytes()
                + next.capacity() * 4
                + parents.capacity() * 4
                + retired.capacity() * 4
                + arena.bytes(),
        );
        arena.free.extend(retired);
        refs = Refs::Level(next);
        tree = KdBush::new(
            (0..refs.len()).map(|i| {
                let n = node(input, &arena, refs.get(i));
                [n.x as f32, n.y as f32]
            }),
            options.node_size,
        )?;
        stats.peak_accounted_bytes = stats.peak_accounted_bytes.max(
            input.bytes() + tree.bytes() + refs.bytes() + arena.bytes() + parents.capacity() * 4,
        );
        emit(
            zoom,
            &Level {
                input,
                arena: &arena,
                refs: &refs,
                tree: &tree,
            },
            &parents,
        )?;
        stats.levels.push((zoom, refs.len()));
    }
    Ok(stats)
}

#[cfg(test)]
mod tests;
