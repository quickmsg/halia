use std::collections::HashSet;

use anyhow::{bail, Result};
use common::error::HaliaResult;
use message::MessageBatch;
use regex::Regex;

use crate::Encoder;

struct Template {
    template: String,
    fields: Vec<String>,
}

pub(crate) fn validate_conf(_conf: &serde_json::Value) -> Result<()> {
    Ok(())
}

pub(crate) fn new(template: String) -> HaliaResult<Box<dyn Encoder>> {
    let re = Regex::new(r"\$\{(.*?)\}").unwrap();
    let mut fields = HashSet::new();
    for cap in re.captures_iter(&template) {
        fields.insert(cap[0][2..cap[0].len() - 1].to_owned());
    }
    Ok(Box::new(Template {
        template,
        fields: fields.into_iter().collect(),
    }))
}

impl Encoder for Template {
    fn encode(&self, mut mb: MessageBatch) -> Result<bytes::Bytes> {
        let message = mb.take_one_message().unwrap();
        let mut template_rendered = self.template.clone();
        for field in &self.fields {
            match message.get(field) {
                Some(value) => {
                    template_rendered =
                        template_rendered.replace(&format!("${{{}}}", field), &value.to_string());
                }
                None => bail!("Field {} not found in message", field),
            }
        }

        return Ok(template_rendered.into_bytes().into());
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use message::{Message, MessageBatch, MessageValue};

    use crate::encoders::template::new;

    #[test]
    fn test_template_encode() {
        let template = new("hello ${name} ${age} ${f}".to_owned()).unwrap();
        let mut message = Message::new();
        message.add("name".to_owned(), MessageValue::String("world".to_owned()));
        message.add("age".to_owned(), MessageValue::Int64(18));
        message.add("f".to_owned(), MessageValue::Float64(5.0));
        let mut mb = MessageBatch::new();
        mb.push_message(message);

        let encoded = template.encode(mb).unwrap();
        assert_eq!(encoded, Bytes::from("hello world 18 5.0"));
    }
}