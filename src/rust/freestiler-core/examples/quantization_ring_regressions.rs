//! Qualification of retained snapped-coordinate failure cases, before encoding.
use freestiler_core::{
    geo_types::{LineString, Polygon},
    quantize::quantize_polygon,
};
use serde_json::{json, Value};
fn main() {
    let args: Vec<_> = std::env::args().collect();
    let cases: Value = serde_json::from_slice(&std::fs::read(&args[1]).unwrap()).unwrap();
    let mut out = Vec::new();
    for case in cases.as_array().unwrap() {
        let rings: Vec<LineString<f64>> = case["geometry"]["coordinates"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| {
                r.as_array()
                    .unwrap()
                    .iter()
                    .map(|c| {
                        let x = c[0].as_f64().unwrap();
                        let y = c[1].as_f64().unwrap();
                        let merc = 1f64.to_radians().tan().asinh();
                        (x, (merc - y / 4096. * merc).sinh().atan().to_degrees())
                    })
                    .collect()
            })
            .collect();
        let p = Polygon::new(rings[0].clone(), rings[1..].to_vec());
        let parts = quantize_polygon(&p, 0., 0., 4096., 1.);
        let coords: Vec<Vec<Vec<(i32, i32)>>> = parts
            .into_iter()
            .map(|p| {
                std::iter::once(p.exterior)
                    .chain(p.interiors)
                    .map(|mut r| {
                        r.push(r[0]);
                        r
                    })
                    .collect()
            })
            .collect();
        out.push(json!({"id":case["id"],"tile":case["tile"],"geometry":{"type":"MultiPolygon","coordinates":coords}}));
    }
    std::fs::write(&args[2], serde_json::to_vec(&out).unwrap()).unwrap();
}
