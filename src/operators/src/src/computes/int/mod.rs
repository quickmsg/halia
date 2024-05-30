use self::{
    abs::Abs,
    add::{AddAnother, AddLit},
    division::Division,
    modulo::Modulo,
    multi::Multi,
    sub::Sub,
};

use super::{Computer, Rule};
use anyhow::{bail, Result};

pub mod abs;
pub mod add;
pub mod division;
pub mod modulo;
pub mod multi;
pub mod sub;

pub(crate) fn get_int_computer(rule: &Rule) -> Result<Box<dyn Computer>> {
    match rule.name.as_str() {
        "abs" => Abs::new(rule.field.clone()),
        "add_lit" => match &rule.value {
            Some(value) => {
                if let Some(value) = value.as_i64() {
                    AddLit::new(rule.field.clone(), value)
                } else {
                    bail!("must provide int arg")
                }
            }
            None => bail!("must provide int arg"),
        },
        "add_another" => match &rule.value {
            Some(value) => {
                if let Some(value) = value.as_str() {
                    AddAnother::new(rule.field.clone(), value.to_string())
                } else {
                    bail!("must provide string arg")
                }
            }
            None => bail!("must provide string arg"),
        },
        "division" => match &rule.value {
            Some(value) => {
                if let Some(value) = value.as_i64() {
                    Division::new(rule.field.clone(), value)
                } else {
                    bail!("must provide int arg")
                }
            }
            None => bail!("must provide int arg"),
        },
        "modulo" => match &rule.value {
            Some(value) => {
                if let Some(value) = value.as_i64() {
                    Modulo::new(rule.field.clone(), value)
                } else {
                    bail!("must provide int arg")
                }
            }
            None => bail!("must provide int arg"),
        },
        "multi" => match &rule.value {
            Some(value) => {
                if let Some(value) = value.as_i64() {
                    Multi::new(rule.field.clone(), value)
                } else {
                    bail!("must provide int arg")
                }
            }
            None => bail!("must provide int arg"),
        },
        "sub" => match &rule.value {
            Some(value) => {
                if let Some(value) = value.as_i64() {
                    Sub::new(rule.field.clone(), value)
                } else {
                    bail!("must provide int arg")
                }
            }
            None => bail!("must provide int arg"),
        },
        _ => bail!("not support"),
    }
}
