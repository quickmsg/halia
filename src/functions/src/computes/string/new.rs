use std::collections::HashSet;

use anyhow::Result;
use message::{Message, MessageValue};
use regex::Regex;

use crate::{computes::Computer, Args};

const TARGET_FIELD_KEY: &str = "target_field";
const TEMPLATE_KEY: &str = "template";

struct New {
    target_field: String,
    template: String,
    template_fields: Vec<String>,
}

pub fn validate_conf(mut args: Args) -> Result<()> {
    Ok(())
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let target_field = args.take_string(TARGET_FIELD_KEY)?;
    let template = args.take_string(TEMPLATE_KEY)?;
    let re = Regex::new(r"\$\{(.*?)\}").unwrap();
    let mut template_fields = HashSet::new();
    for cap in re.captures_iter(&template) {
        template_fields.insert(cap[0][2..cap[0].len() - 1].to_owned());
    }
    let template_fields = template_fields.into_iter().collect();

    Ok(Box::new(New {
        target_field,
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
        message.add(self.target_field.clone(), result);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use message::Message;
    use message::MessageValue;

    use crate::args::Args;
    use crate::args::TARGET_FIELD_KEY;

    use super::new;

    #[test]
    fn test_new() {
        let mut message = Message::default();
        message.add(
            "key_a".to_owned(),
            message::MessageValue::String("value_a".to_owned()),
        );
        message.add("key_b".to_owned(), message::MessageValue::Int64(33));

        let mut args = HashMap::new();
        args.insert(
            TARGET_FIELD_KEY.to_owned(),
            serde_json::Value::String("key_c".to_owned()),
        );
        args.insert(
            "template".to_owned(),
            serde_json::Value::String("hello ${key_a} bb ${key_b} ${22".to_owned()),
        );

        let args = Args::new(args);
        let mut computer = new(args).unwrap();
        computer.compute(&mut message);

        assert_eq!(
            message.get("key_c"),
            Some(&MessageValue::String("hello value_a bb 33 ${22".to_owned()))
        );
    }
}
