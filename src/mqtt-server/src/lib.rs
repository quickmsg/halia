use rumqttd::{Broker, Config, Notification};
use types::mqtt_server::MqttServerConf;

pub struct MqttServer {
    conf: MqttServerConf,
}

impl MqttServer {
    pub fn start(&self) {
        // let config = config::Config::builder()
        //     .add_source(config::File::with_name("rumqttd.toml"))
        //     .build()
        //     .unwrap();

        // let config: Config = config.try_deserialize().unwrap();
        let config: Config = Config {
            id: todo!(),
            router: todo!(),
            v4: todo!(),
            v5: todo!(),
            ws: todo!(),
            cluster: todo!(),
            console: todo!(),
            bridge: todo!(),
            prometheus: todo!(),
            metrics: todo!(),
        };

        let mut broker = Broker::new(config);
        let alerts = broker.alerts().unwrap();
        let (mut link_tx, mut link_rx) = broker.link("consumer").unwrap();
        tokio::spawn(async move {
            broker.start().unwrap();
        });
        println!("here");

        link_tx.subscribe("#").unwrap();
        tokio::spawn(async move {
            let mut count = 0;
            loop {
                let notification = match link_rx.recv().unwrap() {
                    Some(v) => v,
                    None => continue,
                };

                match notification {
                    Notification::Forward(forward) => {
                        count += 1;
                        println!(
                            "Topic = {:?}, Count = {}, Payload = {:?} bytes",
                            forward.publish.topic, count, forward.publish.payload,
                        );
                    }
                    v => {
                        println!("{v:?}");
                    }
                }
            }
        });
    }
}
