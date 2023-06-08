pub const REMOVED_PREFIX: &str = "__removed";

pub const ALPHANUMERICS_NAME_PATTERN: &str = "[a-zA-Z_][a-zA-Z0-9_]*";

pub fn to_removed_key(key: &str) -> String {
    format!("{REMOVED_PREFIX}-{key}")
}

#[cfg(test)]
mod tests {
    use super::to_removed_key;

    #[test]
    fn test_to_removed_key() {
        let key = "test_key";
        let removed = "__removed-test_key";
        assert_eq!(removed, to_removed_key(key));
    }
}
