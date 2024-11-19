use std::collections::HashSet;

use anyhow::Result;
use message::{Message, MessageValue};
use regex::Regex;
use types::rules::functions::ItemConf;

use crate::{add_or_set_message_value, computes::Computer, get_string_arg};

// TODO new 不应该有field字段
struct New {
    field: String,
    target_field: Option<String>,
    template: String,
    template_fields: Vec<String>,
}

pub fn new(conf: ItemConf) -> Result<Box<dyn Computer>> {
    let template = get_string_arg(&conf, "template")?;

    let re = Regex::new(r"\$\{(.*?)\}").unwrap();
    let mut template_fields = HashSet::new();
    for cap in re.captures_iter(&template) {
        template_fields.insert(cap[0][2..cap[0].len() - 1].to_owned());
    }
    let template_fields = template_fields.into_iter().collect();

    Ok(Box::new(New {
        field: conf.field,
        target_field: conf.target_field,
        template,
        template_fields,
    }))
}

impl Computer for New {
    fn compute(&mut self, message: &mut Message) {
        let mut template_rendered = self.template.clone();
        for field in &self.template_fields {
            match message.get(field) {
                Some(value) => {
                    template_rendered =
                        template_rendered.replace(&format!("${{{}}}", field), &value.to_string());
                }
                None => return,
            }
        }

        let result = MessageValue::String(template_rendered);
        add_or_set_message_value!(self, message, result);
    }
}

#[cfg(test)]
mod tests {
    use message::MessageValue;

    #[test]
    fn test_new() {
        use std::collections::HashMap;

        use message::Message;
        use types::rules::functions::ItemConf;

        use super::new;

        let mut message = Message::default();
        message.add(
            "key_a".to_owned(),
            message::MessageValue::String("value_a".to_owned()),
        );
        message.add("key_b".to_owned(), message::MessageValue::Int64(33));

        let mut args = HashMap::new();
        args.insert(
            "template".to_owned(),
            serde_json::Value::String("hello ${key_a} bb ${key_b} ${22".to_owned()),
        );
        let conf = ItemConf {
            // TODO change type
            typ: types::rules::functions::Type::ArrayCardinality,
            args: Some(args),
            field: "".to_owned(),
            target_field: Some("key_c".to_owned()),
        };

        let mut computer = new(conf).unwrap();
        computer.compute(&mut message);

        assert_eq!(
            message.get("key_c"),
            Some(&MessageValue::String("hello value_a bb 33 ${22".to_owned()))
        );
    }
}
