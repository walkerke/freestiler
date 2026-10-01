//! KDBush 4.1.0 sort/search port. See UPSTREAM-LICENSES.
//! Preserve selection and right-before-left query traversal, not just sets.
//! Float32 storage; all arithmetic in searches/selection is JS-number (f64).
pub(super) struct KdBush {
    pub ids: Vec<u32>,
    pub coords: Vec<[f32; 2]>,
    node_size: usize,
}

impl KdBush {
    pub fn new(
        points: impl ExactSizeIterator<Item = [f32; 2]>,
        node_size: usize,
    ) -> Result<Self, String> {
        let n = points.len();
        if n > u32::MAX as usize {
            return Err("KD index exceeds u32 capacity".into());
        }
        let mut ids = Vec::new();
        let mut coords = Vec::new();
        ids.try_reserve_exact(n).map_err(|e| e.to_string())?;
        coords.try_reserve_exact(n).map_err(|e| e.to_string())?;
        for (i, p) in points.enumerate() {
            if p.iter().any(|x| !x.is_finite()) {
                return Err("Nonfinite KD coordinate".into());
            }
            ids.push(i as u32);
            coords.push(p);
        }
        let mut result = Self {
            ids,
            coords,
            node_size: node_size.clamp(2, 65535),
        };
        if n > 0 {
            result.sort(0, n - 1, 0);
        }
        Ok(result)
    }
    pub fn bytes(&self) -> usize {
        self.ids.capacity() * 4 + self.coords.capacity() * 8
    }
    fn swap(&mut self, i: usize, j: usize) {
        self.ids.swap(i, j);
        self.coords.swap(i, j);
    }
    fn sort(&mut self, left: usize, right: usize, axis: usize) {
        if right - left <= self.node_size {
            return;
        }
        let m = (left + right) / 2;
        self.select(m, left, right, axis);
        self.sort(left, m - 1, 1 - axis);
        self.sort(m + 1, right, 1 - axis);
    }
    fn select(&mut self, k: usize, mut left: usize, mut right: usize, axis: usize) {
        while right > left {
            if right - left > 600 {
                let n = (right - left + 1) as f64;
                let m = (k - left + 1) as f64;
                let z = n.ln();
                let s = 0.5 * (2.0 * z / 3.0).exp();
                let sd =
                    0.5 * (z * s * (n - s) / n).sqrt() * if m - n / 2.0 < 0.0 { -1.0 } else { 1.0 };
                let nl = left.max((k as f64 - m * s / n + sd).floor().max(0.0) as usize);
                let nr = right.min((k as f64 + (n - m) * s / n + sd).floor().max(0.0) as usize);
                self.select(k, nl, nr, axis);
            }
            let t = self.coords[k][axis];
            let mut i = left;
            let mut j = right;
            self.swap(left, k);
            if self.coords[right][axis] > t {
                self.swap(left, right);
            }
            while i < j {
                self.swap(i, j);
                i += 1;
                j -= 1;
                while self.coords[i][axis] < t {
                    i += 1;
                }
                while self.coords[j][axis] > t {
                    j -= 1;
                }
            }
            if self.coords[left][axis] == t {
                self.swap(left, j);
            } else {
                j += 1;
                self.swap(j, right);
            }
            if j <= k {
                left = j + 1;
            }
            if k <= j {
                if j == 0 {
                    break;
                }
                right = j - 1;
            }
        }
    }
    /// Allocation-free neighborhood visitation in the exact upstream order.
    pub fn within(&self, x: f64, y: f64, r: f64, mut visit: impl FnMut(u32)) {
        self.search(
            [x - r, y - r, x + r, y + r],
            Some((x, y, r * r)),
            &mut visit,
        );
    }
    pub fn range(&self, bounds: [f64; 4], mut visit: impl FnMut(u32)) {
        self.search(bounds, None, &mut visit);
    }
    fn search<F: FnMut(u32)>(&self, b: [f64; 4], circle: Option<(f64, f64, f64)>, visit: &mut F) {
        if self.ids.is_empty() {
            return;
        }
        // At most two child frames per level; depth <= 32 for u32 input size.
        let mut stack = [(0usize, 0usize, 0usize); 64];
        stack[0] = (0, self.ids.len() - 1, 0);
        let mut sp = 1;
        let check = |i: usize, visit: &mut F| {
            let [x, y] = self.coords[i].map(f64::from);
            let hit = if let Some((qx, qy, r2)) = circle {
                let dx = x - qx;
                let dy = y - qy;
                dx * dx + dy * dy <= r2
            } else {
                x >= b[0] && x <= b[2] && y >= b[1] && y <= b[3]
            };
            if hit {
                visit(self.ids[i]);
            }
        };
        while sp > 0 {
            sp -= 1;
            let (left, right, axis) = stack[sp];
            if right - left <= self.node_size {
                for i in left..=right {
                    check(i, visit);
                }
                continue;
            }
            let m = (left + right) / 2;
            check(m, visit);
            let split = self.coords[m][axis] as f64;
            if b[axis] <= split {
                stack[sp] = (left, m - 1, 1 - axis);
                sp += 1;
            }
            if b[axis + 2] >= split {
                stack[sp] = (m + 1, right, 1 - axis);
                sp += 1;
            }
        }
    }
}
