use functions::Function;
use message::MessageBatch;
use tokio::sync::{broadcast, mpsc};

pub struct Segement {
    rx: broadcast::Receiver<MessageBatch>,

    functions: Vec<Box<dyn Function>>,

    single_tx: Option<mpsc::Sender<MessageBatch>>,
    broadcast_tx: Option<broadcast::Sender<MessageBatch>>,
}

// pub(crate) async fn start_stream(
//     nodes: Vec<&CreateRuleNode>,
//     mut rx: broadcast::Receiver<MessageBatch>,
//     tx: broadcast::Sender<MessageBatch>,
//     mut stop_signal: broadcast::Receiver<()>,
// ) {
//     let mut functions = Vec::new();
//     for create_graph_node in nodes {
//         let node = functions::new(&create_graph_node).unwrap();
//         functions.push(node);
//     }

//     tokio::spawn(async move {
//         loop {
//             select! {
//                 biased;

//                 _ = stop_signal.recv() => {
//                     debug!("stream stop");
//                     return
//                 }

//                 message_batch = rx.recv() => {
//                     match message_batch {
//                         Ok(mut message_batch) => {
//                             for function in &functions {
//                                 function.call(&mut message_batch);
//                                 if message_batch.len() == 0 {
//                                     break;
//                                 }
//                             }

//                             if message_batch.len() != 0 {
//                                 if let Err(e) = tx.send(message_batch) {
//                                     error!("stream send err:{}, ids", e);
//                                 }
//                             }
//                         },
//                         Err(e) => {
//                             error!("stream recv err:{}", e);
//                             break;
//                         }
//                     }
//                 }
//             }
//         }
//     });
// }
