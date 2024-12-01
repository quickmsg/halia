extern crate proc_macro;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(SourceRxs)]
pub fn derive_source_rxs(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = input.ident;

    let expanded = quote! {
        impl #struct_name {
            pub async fn get_rxs(&mut self, cnt: usize) -> Vec<tokio::sync::mpsc::UnboundedReceiver<message::RuleMessageBatch>> {
                let mut rxs = Vec::with_capacity(cnt);
                let mut txs = Vec::with_capacity(cnt);
                for _ in 0..cnt {
                    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<message::RuleMessageBatch>();
                    txs.push(tx);
                    rxs.push(rx);
                }
                self.mb_txs.lock().await.append(&mut txs);
                rxs
            }
        }
    };

    proc_macro::TokenStream::from(expanded)
}

#[proc_macro_derive(ResourceStop)]
pub fn derive_resource_stop(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = input.ident;

    let expanded = quote! {
        impl #struct_name {
            pub async fn stop(&mut self) -> TaskLoop {
                self.stop_signal_tx.send(()).unwrap();
                self.join_handle.take().unwrap().await.unwrap()
            }
        }
    };

    proc_macro::TokenStream::from(expanded)
}

#[proc_macro_derive(ResourceErr)]
pub fn derive_resource_err(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = input.ident;

    let expanded = quote! {
        impl #struct_name {
            pub async fn read_err(&self) -> Option<Arc<String>> {
                let err_guard = self.err.lock().await;
                match &(*err_guard) {
                    Some(err) => Some(err.clone()),
                    None => None,
                }
            }
        }
    };

    proc_macro::TokenStream::from(expanded)
}

#[proc_macro_derive(SinkTxs)]
pub fn derive_sink_txs(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = input.ident;

    let expanded = quote! {
        impl #struct_name {
            pub fn get_txs(&self, cnt: usize) -> Vec<tokio::sync::mpsc::UnboundedSender<message::RuleMessageBatch>> {
                let mut txs = vec![];
                for _ in 0..cnt {
                    txs.push(self.mb_tx.clone());
                }
                txs
            }
        }
    };

    proc_macro::TokenStream::from(expanded)
}