extern crate proc_macro;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(Source)]
pub fn derive_source(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = input.ident;

    let expanded = quote! {
        impl #struct_name {
            pub async fn get_rxs(&mut self, cnt: usize) -> Vec<tokio::sync::mpsc::UnboundedReceiver<RuleMessageBatch>> {
                let mut rxs = Vec::with_capacity(cnt);
                let mut txs = Vec::with_capacity(cnt);
                for _ in 0..cnt {
                    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<RuleMessageBatch>();
                    txs.push(tx);
                    rxs.push(rx);
                }
                self.mb_txs.lock().await.append(&mut txs);
                rxs
            }

            pub async fn stop(&mut self) -> TaskLoop {
                self.stop_signal_tx.send(()).unwrap();
                self.join_handle.take().unwrap().await.unwrap()
            }

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
