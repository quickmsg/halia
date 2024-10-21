use std::collections::HashSet;

use anyhow::{bail, Result};
use message::MessageBatch;
use regex::Regex;

use crate::Encoder;

struct Template {
    template: String,
    fields: Vec<String>,
}

pub(crate) fn new(template: String) -> Result<Template> {
    let re = Regex::new(r"\$\{(.*?)\}").unwrap();
    let mut fields = HashSet::new();
    for cap in re.captures_iter(&template) {
        fields.insert(cap[1].to_owned());
    }
    Ok(Template {
        template,
        fields: fields.into_iter().collect(),
    })
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
