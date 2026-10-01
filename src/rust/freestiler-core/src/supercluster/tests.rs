use super::*;
use serde_json::Value;

fn oracle() -> Value {
    serde_json::from_reader(flate2::read::GzDecoder::new(
        &include_bytes!("../../tests/fixtures/supercluster-8.0.1.json.gz")[..],
    ))
    .unwrap()
}
fn ints(v: &Value) -> Vec<u32> {
    v.as_array()
        .unwrap()
        .iter()
        .map(|n| n.as_u64().unwrap() as u32)
        .collect()
}

#[test]
fn kdbush_matches_upstream_sort_and_query_order() {
    let value = oracle();
    let f = &value["kdbush"];
    let pts: Vec<[f32; 2]> = f["points"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| [p[0].as_f64().unwrap() as f32, p[1].as_f64().unwrap() as f32])
        .collect();
    let tree = KdBush::new(
        pts.iter().copied(),
        f["nodeSize"].as_u64().unwrap() as usize,
    )
    .unwrap();
    assert_eq!(tree.ids, ints(&f["ids"]));
    let coords: Vec<f32> = f["coords"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_f64().unwrap() as f32)
        .collect();
    assert_eq!(
        tree.coords.iter().flatten().copied().collect::<Vec<_>>(),
        coords
    );
    for q in f["queries"].as_array().unwrap() {
        let x = q["x"].as_f64().unwrap();
        let y = q["y"].as_f64().unwrap();
        let r = q["r"].as_f64().unwrap();
        let mut actual = Vec::new();
        tree.within(x, y, r, |i| actual.push(i));
        assert_eq!(actual, ints(&q["within"]));
        actual.clear();
        tree.range([x - r, y - r, x + r, y + r], |i| actual.push(i));
        assert_eq!(actual, ints(&q["range"]));
    }
}

#[test]
fn supercluster_matches_all_pinned_ordered_fixtures() {
    let value = oracle();
    for f in value["fixtures"].as_array().unwrap() {
        let name = f["name"].as_str().unwrap();
        let coords = f["coords"].as_array().unwrap();
        let mut input = Input::new(3).unwrap();
        input.reserve_exact(coords.len()).unwrap();
        for (i, p) in coords.iter().enumerate() {
            input
                .push(
                    p[0].as_f64().unwrap(),
                    p[1].as_f64().unwrap(),
                    (i % 3) as u8,
                )
                .unwrap();
        }
        let o = &f["options"];
        let options = Options {
            min_zoom: o["minZoom"].as_u64().unwrap() as u8,
            max_zoom: o["maxZoom"].as_u64().unwrap() as u8,
            radius: o["radius"].as_f64().unwrap(),
            min_points: o["minPoints"].as_u64().unwrap() as u32,
            node_size: o["nodeSize"].as_u64().unwrap() as usize,
        };
        let mut owners: Vec<u32> = (0..input.len() as u32).collect();
        let mut finest_singletons = Vec::new();
        build_traced(&input, options, |z, level, parents| {
            assert!(!parents.contains(&u32::MAX), "{name} z{z}");
            for owner in &mut owners {
                *owner = parents[*owner as usize];
            }
            let expect = f["levels"]
                .as_array()
                .unwrap()
                .iter()
                .find(|l| l["zoom"] == z)
                .unwrap();
            let nodes = expect["nodes"].as_array().unwrap();
            assert_eq!(level.len(), nodes.len(), "{name} z{z}");
            if z == options.max_zoom {
                finest_singletons = level.singleton_bitset().unwrap();
            }
            for (i, e) in nodes.iter().enumerate() {
                let n = level.node(i);
                assert_eq!(n.id, e["id"].as_u64().unwrap(), "{name} z{z} i{i} ID");
                assert_eq!(
                    n.count,
                    e["count"].as_u64().unwrap(),
                    "{name} z{z} i{i} count"
                );
                assert_eq!(
                    n.x.to_bits(),
                    e["x"].as_f64().unwrap().to_bits(),
                    "{name} z{z} i{i} x"
                );
                assert_eq!(
                    n.y.to_bits(),
                    e["y"].as_f64().unwrap().to_bits(),
                    "{name} z{z} i{i} y"
                );
                assert_eq!(
                    n.expansion_zoom,
                    e["expansion"].as_u64().map(|z| z as u8),
                    "{name} z{z} expansion"
                );
                let actual: Vec<u32> = owners
                    .iter()
                    .enumerate()
                    .filter_map(|(r, o)| (*o as usize == i).then_some(r as u32))
                    .collect();
                let mut members = ints(&e["members"]);
                members.sort_unstable();
                assert_eq!(actual, members, "{name} z{z} i{i} membership");
                for k in 0..3 {
                    assert_eq!(level.category_count(i, k), e["counts"][k].as_u64().unwrap());
                }
                assert_eq!(level.category_count(i, 3), 0);
                if let Some(source) = n.source {
                    assert_ne!(
                        finest_singletons[source as usize / 8] & (1 << (source % 8)),
                        0
                    );
                    assert_eq!(
                        coords[source as usize], e["original"],
                        "exact singleton row recovery"
                    );
                }
            }
            Ok(())
        })
        .unwrap();
    }
}

#[test]
fn sparse_levels_share_raw_rows_instead_of_accumulating_nodes() {
    let mut input = Input::new(3).unwrap();
    input.reserve_exact(100_000).unwrap();
    for i in 0..100_000 {
        input
            .push(
                -179.0 + (i % 1000) as f64 * 0.35,
                -80.0 + (i / 1000) as f64 * 1.6,
                (i % 3) as u8,
            )
            .unwrap();
    }
    let options = Options {
        max_zoom: 11,
        min_zoom: 3,
        radius: 0.001,
        ..Options::default()
    };
    let stats = build(&input, options, |_, level| {
        assert_eq!(level.len(), input.len());
        for i in [0, 57, 99_999] {
            assert_eq!(level.node(i).source, Some(i as u32));
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(stats.levels.len(), 9);
    assert!(stats.peak_accounted_bytes < 40 * input.len(), "{stats:?}");
}

#[test]
fn checked_ids_beyond_signed_js_shift_and_overflow() {
    for i in [(1 << 26) - 1, 1 << 26, 1 << 27, 331_449_280] {
        assert_eq!(
            cluster_id(i, 11, 331_449_281).unwrap(),
            i * 32 + 12 + 331_449_281
        );
    }
    assert!(cluster_id(u64::MAX, 11, 1).is_err());
    assert!(cluster_id(0, 31, 1).is_err());
    assert!(cluster_id(0, 11, JS_SAFE).is_err());
}

#[test]
fn unchanged_clusters_keep_identity_and_all_64_categories() {
    let mut input = Input::new(64).unwrap();
    for i in 0..1000 {
        let lon = -179.0 + (i % 100) as f64 * 3.5;
        let lat = -70.0 + (i / 100) as f64 * 14.0;
        input.push(lon, lat, (i % 64) as u8).unwrap();
        input.push(lon, lat, 64).unwrap(); // reserved other category
    }
    let options = Options {
        min_zoom: 3,
        max_zoom: 11,
        radius: 0.001,
        ..Options::default()
    };
    build(&input, options, |_, level| {
        assert_eq!(level.len(), 1000);
        for i in 0..level.len() {
            let node = level.node(i);
            assert_eq!(node.id, cluster_id((2 * i) as u64, 11, 2000).unwrap());
            assert_eq!(node.expansion_zoom, Some(12));
            assert_eq!(node.count, 2);
            assert_eq!(level.category_count(i, i % 64), 1);
            assert_eq!(level.category_count(i, 64), 1);
        }
        Ok(())
    })
    .unwrap();
}

#[test]
fn validation_and_callback_failure_propagate() {
    let mut input = Input::new(2).unwrap();
    assert!(input.push(f64::NAN, 0.0, 0).is_err());
    assert!(input.push(0.0, 91.0, 0).is_err());
    assert!(input.push(0.0, 0.0, 3).is_err());
    input.push(0.0, 0.0, 2).unwrap();
    input.push(0.0, 0.0, 0).unwrap();
    let err = build(&input, Options::default(), |_, _| Err("cancelled".into())).unwrap_err();
    assert_eq!(err, "cancelled");
    assert!(build(
        &input,
        Options {
            radius: f64::NAN,
            ..Options::default()
        },
        |_, _| Ok(())
    )
    .is_err());
    build(
        &input,
        Options {
            min_zoom: 1,
            max_zoom: 1,
            ..Options::default()
        },
        |_, l| {
            assert_eq!(l.category_count(0, 2), 1);
            assert_eq!(l.category_count(0, 0), 1);
            Ok(())
        },
    )
    .unwrap();
}

#[test]
fn category_storage_narrows_checked_without_changing_public_counts() {
    let mut arena = Arena {
        nodes: Vec::new(),
        counts: Vec::new(),
        free: Vec::new(),
        width: 1,
    };
    let cluster = ClusterNode {
        id: 5,
        x: 0.5,
        y: 0.5,
        count: u32::MAX as u64,
    };
    arena.insert(cluster, &[u32::MAX as u64]).unwrap();
    assert_eq!(u64::from(arena.counts[0]), u32::MAX as u64);
    let before_nodes = arena.nodes.len();
    assert!(arena.insert(cluster, &[u32::MAX as u64 + 1]).is_err());
    assert_eq!(arena.nodes.len(), before_nodes);
    assert_eq!(arena.counts, [u32::MAX]);
    // Eight categories plus other: row payload falls from 104 to 68 bytes.
    assert_eq!(
        std::mem::size_of::<ClusterNode>() + 9 * std::mem::size_of::<u32>(),
        68
    );
}
