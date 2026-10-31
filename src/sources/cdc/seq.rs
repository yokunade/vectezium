//! The master sequence (`seq`) every event carries.

/// The master sequence: `<change counter, 20 digits>.<snapshot position, 10 digits>`.
///
/// Change events (from the transaction log) have position 0 and advance the counter by one each,
/// so their numbers are identical on every replay; snapshot-derived events take the current
/// counter and the next position. Fixed width, so it sorts correctly as text.
#[must_use]
pub fn master_seq(counter: u64, position: u64) -> String {
    format!("{counter:020}.{position:010}")
}

#[cfg(test)]
mod tests {
    use super::master_seq;

    #[test]
    fn master_seq_is_fixed_width_and_sorts_as_text() {
        assert_eq!(master_seq(9001, 0), "00000000000000009001.0000000000");
        assert_eq!(master_seq(9001, 42), "00000000000000009001.0000000042");
        let mut seqs = vec![
            master_seq(10, 0),
            master_seq(9, 4_096),
            master_seq(9, 0),
            master_seq(u64::MAX, 9_999_999_999),
            master_seq(9, 1),
        ];
        let numeric = {
            let mut v: Vec<(u64, u64)> = vec![
                (10, 0),
                (9, 4_096),
                (9, 0),
                (u64::MAX, 9_999_999_999),
                (9, 1),
            ];
            v.sort_unstable();
            v.into_iter()
                .map(|(a, b)| master_seq(a, b))
                .collect::<Vec<_>>()
        };
        seqs.sort();
        assert_eq!(seqs, numeric);
        assert!(seqs.iter().all(|s| s.len() == 31));
    }
}
