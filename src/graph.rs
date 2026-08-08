use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};

// kahn's algorithm; ties broken by declaration order so ordering is deterministic
pub(crate) fn topo_order(ops: &[(String, Vec<String>)]) -> Result<Vec<String>, String> {
    let mut index: HashMap<&str, usize> = HashMap::new();
    for (i, (name, _)) in ops.iter().enumerate() {
        if index.insert(name.as_str(), i).is_some() {
            return Err(format!("duplicate op {name}"));
        }
    }

    let mut indegree = vec![0u32; ops.len()];
    let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); ops.len()];
    for (i, (name, deps)) in ops.iter().enumerate() {
        for dep in deps {
            let Some(&j) = index.get(dep.as_str()) else {
                return Err(format!("op {name} depends on unknown op {dep}"));
            };
            indegree[i] += 1;
            dependents[j].push(i);
        }
    }

    let mut ready: BinaryHeap<Reverse<usize>> = indegree
        .iter()
        .enumerate()
        .filter(|&(_, &d)| d == 0)
        .map(|(i, _)| Reverse(i))
        .collect();
    let mut order = Vec::with_capacity(ops.len());
    while let Some(Reverse(i)) = ready.pop() {
        order.push(ops[i].0.clone());
        for &d in &dependents[i] {
            indegree[d] -= 1;
            if indegree[d] == 0 {
                ready.push(Reverse(d));
            }
        }
    }

    if order.len() < ops.len() {
        // leftovers include the cycle's downstream too; walk unfinished deps
        // until a node repeats to name one actually on the cycle
        let mut i = indegree.iter().position(|&d| d > 0).unwrap();
        let mut seen = vec![false; ops.len()];
        while !seen[i] {
            seen[i] = true;
            i = ops[i]
                .1
                .iter()
                .map(|d| index[d.as_str()])
                .find(|&j| indegree[j] > 0)
                .unwrap();
        }
        return Err(format!("dependency cycle involving {}", ops[i].0));
    }
    Ok(order)
}

// transitive dependents of root, root itself excluded
pub(crate) fn downstream(ops: &[(String, Vec<String>)], root: &str) -> HashSet<String> {
    let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new();
    for (name, deps) in ops {
        for d in deps {
            dependents
                .entry(d.as_str())
                .or_default()
                .push(name.as_str());
        }
    }
    let mut out = HashSet::new();
    let mut stack = vec![root];
    while let Some(n) = stack.pop() {
        for &k in dependents.get(n).map(Vec::as_slice).unwrap_or_default() {
            if out.insert(k.to_string()) {
                stack.push(k);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ops(pairs: &[(&str, &[&str])]) -> Vec<(String, Vec<String>)> {
        pairs
            .iter()
            .map(|(n, ds)| (n.to_string(), ds.iter().map(|d| d.to_string()).collect()))
            .collect()
    }

    #[test]
    fn topo_prefers_declaration_order() {
        // diamond declared sink-first: among ready ops the earliest declared wins
        let g = ops(&[("d", &["b", "c"]), ("b", &["a"]), ("c", &["a"]), ("a", &[])]);
        for _ in 0..10 {
            assert_eq!(topo_order(&g).unwrap(), ["a", "b", "c", "d"]);
        }
    }

    #[test]
    fn duplicate_op_rejected() {
        let g = ops(&[("a", &[]), ("b", &["a"]), ("a", &[])]);
        assert_eq!(topo_order(&g).unwrap_err(), "duplicate op a");
    }

    #[test]
    fn unknown_dep_rejected() {
        let g = ops(&[("a", &["ghost"])]);
        assert_eq!(
            topo_order(&g).unwrap_err(),
            "op a depends on unknown op ghost"
        );
    }

    #[test]
    fn cycle_names_a_cycle_member() {
        let g = ops(&[("z", &["a"]), ("a", &["c"]), ("b", &["a"]), ("c", &["b"])]);
        let err = topo_order(&g).unwrap_err();
        assert!(err.starts_with("dependency cycle involving"), "{err}");
        // z hangs off the cycle but is not on it
        assert!(!err.contains('z'), "{err}");
    }

    #[test]
    fn downstream_is_transitive_and_excludes_root() {
        let g = ops(&[
            ("a", &[]),
            ("b", &["a"]),
            ("c", &["b"]),
            ("d", &["a"]),
            ("e", &[]),
        ]);
        let want: HashSet<String> = ["b", "c", "d"].iter().map(|s| s.to_string()).collect();
        assert_eq!(downstream(&g, "a"), want);
        assert!(downstream(&g, "e").is_empty());
    }
}
