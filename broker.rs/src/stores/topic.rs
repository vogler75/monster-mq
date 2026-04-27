/// Schema-compatible topic helpers, byte-identical with the JVM/Go brokers.
pub const MAX_FIXED_TOPIC_LEVELS: usize = 9;

/// Returns (fixed[9], rest_json, last). `fixed` are the first 9 segments
/// right-padded with empty strings; `rest_json` is the JSON-encoded array of
/// any remaining levels (empty list `[]` if there are none); `last` is the
/// final level of the topic.
pub fn split_topic(topic: &str) -> ([String; MAX_FIXED_TOPIC_LEVELS], String, String) {
    let parts: Vec<&str> = topic.split('/').collect();
    let mut fixed: [String; MAX_FIXED_TOPIC_LEVELS] = Default::default();
    for i in 0..MAX_FIXED_TOPIC_LEVELS.min(parts.len()) {
        fixed[i] = parts[i].to_string();
    }
    let rest_json = if parts.len() > MAX_FIXED_TOPIC_LEVELS {
        serde_json::to_string(&parts[MAX_FIXED_TOPIC_LEVELS..].to_vec()).unwrap_or_else(|_| "[]".to_string())
    } else {
        "[]".to_string()
    };
    let last = parts.last().copied().unwrap_or("").to_string();
    (fixed, rest_json, last)
}

/// MQTT-style topic match (`+` single-level, `#` multi-level, must be last).
pub fn match_topic(pattern: &str, topic: &str) -> bool {
    let pp: Vec<&str> = pattern.split('/').collect();
    let tt: Vec<&str> = topic.split('/').collect();
    for (i, p) in pp.iter().enumerate() {
        if *p == "#" {
            return true;
        }
        if i >= tt.len() {
            return false;
        }
        if *p == "+" {
            continue;
        }
        if *p != tt[i] {
            return false;
        }
    }
    pp.len() == tt.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn match_basic() {
        assert!(match_topic("a/b", "a/b"));
        assert!(!match_topic("a/b", "a/b/c"));
        assert!(match_topic("a/+", "a/b"));
        assert!(!match_topic("a/+", "a/b/c"));
        assert!(match_topic("a/#", "a/b/c"));
        assert!(match_topic("#", "x/y/z"));
    }
}
