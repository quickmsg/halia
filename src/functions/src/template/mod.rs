use std::collections::HashSet;

use message::{MessageBatch, MessageValue};
use regex::Regex;

struct Template {
    template: String,
    fields: Vec<String>,
}

pub fn new(template: String) -> Template {
    let re = Regex::new(r"\{\{(.*?)\}\}").unwrap();
    let mut fields = HashSet::new();
    for cap in re.captures_iter(&template) {
        fields.insert(cap[1].to_owned());
    }
    Template {
        template,
        fields: fields.into_iter().collect(),
    }
}

impl Template {
    pub fn render(&self, mb: &mut MessageBatch) {
        for message in mb.get_messages_mut().iter_mut() {
            let mut template_rendered = self.template.clone();
            for field in &self.fields {
                let value = match message.get(field) {
                    Some(value) => value,

                    None => &MessageValue::Null,
                };
                template_rendered =
                    template_rendered.replace(&format!("{{{{{}}}}}", field), &value.to_string());
            }
            message.add("tpl".to_owned(), MessageValue::String(template_rendered));
        }
    }
}