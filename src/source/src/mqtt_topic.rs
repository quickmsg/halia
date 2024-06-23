struct MqttTopic {}

pub fn valid_filter(filter: &str) -> bool {
    if filter.is_empty() {
        return false;
    }

    let hirerarchy = filter.split('/').collect::<Vec<&str>>();
    if let Some((last, remaining)) = hirerarchy.split_last() {
        for entry in remaining.iter() {
            // # is not allowed in filter except as a last entry
            // invalid: sport/tennis#/player
            // invalid: sport/tennis/#/ranking
            if entry.contains('#') {
                return false;
            }

            // + must occupy an entire level of the filter
            // invalid: sport+
            if entry.len() > 1 && entry.contains('+') {
                return false;
            }
        }

        // only single '#" or '+' is allowed in last entry
        // invalid: sport/tennis#
        // invalid: sport/++
        if last.len() != 1 && (last.contains('#') || last.contains('+')) {
            return false;
        }
    }

    true
}

pub fn matches(topic: &str, filter: &str) -> bool {
    if !topic.is_empty() && topic[..1].contains('$') {
        return false;
    }

    let mut topics = topic.split('/');
    let mut filters = filter.split('/');

    for f in filters.by_ref() {
        // "#" being the last element is validated by the broker with 'valid_filter'
        if f == "#" {
            return true;
        }

        // filter still has remaining elements
        // filter = a/b/c/# should match topci = a/b/c
        // filter = a/b/c/d should not match topic = a/b/c
        let top = topics.next();
        match top {
            Some("#") => return false,
            Some(_) if f == "+" => continue,
            Some(t) if f != t => return false,
            Some(_) => continue,
            None => return false,
        }
    }

    // topic has remaining elements and filter's last element isn't "#"
    if topics.next().is_some() {
        return false;
    }

    true
}