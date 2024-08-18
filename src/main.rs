use log::{error, info, warn};
use metrics::Metrics;
use semtech_udp::client_runtime;
use semtech_udp::client_runtime::{ClientRx, ClientTx, DownlinkRequest, UdpRuntime};
use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    time::Instant,
};
use structopt::StructOpt;
use tokio::{
    signal,
    time::{sleep, Duration},
};

mod error;
mod metrics;
mod settings;

#[cfg(feature = "async-radio")]
mod async_virtual_device;
#[cfg(feature = "async-radio")]
use async_virtual_device::VirtualDevice;

mod util;
#[cfg(not(feature = "async-radio"))]
mod virtual_device;

#[cfg(not(feature = "async-radio"))]
use virtual_device::VirtualDevice;

pub use error::{Error, Result};
pub use settings::{mac_string_into_buf, Credentials};

#[derive(Debug, StructOpt)]
#[structopt(name = "virtual-lorawan-device", about = "LoRaWAN test device utility")]
pub struct Opt {
    /// Path to settings subdirectory
    #[structopt(short, long, default_value = "./settings")]
    pub settings: PathBuf,
    /// Limit number of devices to spawn
    #[structopt(short, long)]
    pub limit: Option<usize>,
}

const DEFAULT_PF: &str = "default";

fn setup_shutdown_listener() -> Result<triggered::Listener> {
    let (trigger, listener) = triggered::trigger();
    let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;
    tokio::spawn(async move {
        tokio::select! {
            _ = sigterm.recv() => trigger.trigger(),
            _ = tokio::signal::ctrl_c() => trigger.trigger(),
        }
    });
    Ok(listener)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Default log level to INFO unless environment override
    let mut log_builder = env_logger::Builder::from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "INFO"),
    );

    // Allow timestamps to be disabled
    let timestamps = std::env::var("VDEVICE_LOG_TIMESTAMP").unwrap_or_else(|_| "true".to_string());
    if timestamps != "true" {
        log_builder.format_timestamp(None).init();
    } else {
        log_builder.init();
    }

    let cli = Opt::from_args();
    let instant = Instant::now();
    let settings = settings::Settings::new(&cli.settings)?;
    let metrics_server: IpAddr = settings.metrics_server.parse()?;
    let metrics = Metrics::new(settings.get_servers());
    let device_limit = if let Some(limit) = cli.limit {
        limit
    } else {
        usize::MAX
    };
    let trigger_listener = setup_shutdown_listener()?;
    let mut pf_map = setup_packet_forwarders(settings.packet_forwarder).await?;
    for (label, device) in settings.device.clone().into_iter().take(device_limit) {
        let packet_forwarder = if let Some(pf) = &device.packet_forwarder {
            pf
        } else {
            DEFAULT_PF
        };

        let metrics_sender = metrics.get_server_sender(if let Some(server) = &device.server {
            server
        } else {
            &settings.default_server
        });
        if let Some((_udp_runtime, client_tx, _client_rx, senders)) =
            pf_map.get_mut(packet_forwarder)
        {
            let (packet_sender, virtual_device) = VirtualDevice::new(
                label.clone(),
                instant,
                client_tx.clone(),
                device.credentials,
                metrics_sender,
                device.rejoin_frames,
                device.secs_between_transmits,
                device.secs_between_join_transmits,
                device.region,
            )
            .await?;

            senders.push(packet_sender);

            tokio::spawn(async move {
                if let Err(e) = virtual_device.run().await {
                    error!("{} device threw error: {:?}", label, e)
                }
            });
        } else {
            panic!("Unknown macaddress linked to device!");
        }
    }
    for (_label, (udp_runtime, _client_tx, client_rx, senders)) in pf_map {
        tokio::spawn(udp_runtime.run(trigger_listener.clone()));
        tokio::spawn(packet_muxer(
            instant,
            client_rx,
            senders,
            trigger_listener.clone(),
        ));
    }

    metrics
        .run(
            trigger_listener,
            (metrics_server, settings.metrics_port).into(),
        )
        .await?;
    Ok(())
}

async fn setup_packet_forwarders<S: DownlinkSender>(
    mut packet_forwarder: HashMap<String, settings::PacketForwarder>,
) -> Result<HashMap<String, (UdpRuntime, ClientTx, ClientRx, Vec<S>)>> {
    // prune the default packet forwarder if we have more than one
    if packet_forwarder.len() != 1 && packet_forwarder.contains_key("default") {
        packet_forwarder.remove("default");
    }

    let mut pf_map = HashMap::new();
    for (label, packet_forwarder) in packet_forwarder {
        let outbound = SocketAddr::from(([0, 0, 0, 0], 0));
        info!(
            "Creating packet forwarder {} connecting to {} from {}",
            label,
            packet_forwarder.host,
            outbound.to_string()
        );
        let (sender, receiver, udp_runtime) = UdpRuntime::new(
            packet_forwarder.mac_cloned_into_buf().unwrap().into(),
            packet_forwarder.host,
        )
        .await?;
        pf_map.insert(label, (udp_runtime, sender, receiver, vec![]));
    }

    Ok(pf_map)
}

async fn packet_muxer<S: DownlinkSender>(
    instant: Instant,
    mut client_rx: ClientRx,
    senders: Vec<S>,
    trigger: triggered::Listener,
) -> Result {
    tokio::select!(
        _ = trigger => Ok(()),
        resp = async move {
            loop {
                let msg = client_rx.recv().await.ok_or(Error::RxChannelSemtechUdpClientRuntimeClosed)?;
                if let client_runtime::Event::DownlinkRequest(downlink) = msg {
                    println!("Downlink request: {:?}", downlink);
                    if let Some(scheduled_time) = downlink.pull_resp.data.txpk.time.tmst() {
                        let time = instant.elapsed().as_micros() as u32;
                        if scheduled_time > time {
                            let downlink = Box::new(downlink);
                            let delay = scheduled_time - time;
                            for sender in &senders {
                                let sender = sender.clone();
                                let downlink = downlink.clone();
                                tokio::spawn(async move {
                                    info!("Sending downlink in {} ms", delay/1_000);
                                    #[cfg(not(feature = "async-radio"))]
                                    sleep(Duration::from_micros(delay as u64 + 50_000)).await;
                                    #[cfg(feature = "async-radio")]
                                    sleep(Duration::from_micros(delay as u64)).await;
                                    if let Err(e) = sender.send(downlink, delay as u64).await {
                                        error!("Error sending packet to virtual-lorawan-device instance: {e}");
                                    }
                                });
                            }
                            downlink.ack().await?;
                        } else {
                            let time_since_scheduled_time = time - scheduled_time;
                            if time_since_scheduled_time > 1000 {
                                warn!(
                                    "UDP packet received after tx time by {} ms",
                                    time_since_scheduled_time / 1000
                                );
                            } else {
                                warn!(
                                    "UDP packet received after tx time by {} μs",
                                    time_since_scheduled_time
                                );
                            }
                            downlink.nack(semtech_udp::tx_ack::Error::TooLate).await?;
                        }
                    } else {
                        #[cfg(not(feature = "async-radio"))]
                        warn!(
                            "Unexpected! UDP packet to transmit radio packet immediately"
                        );
                        #[cfg(feature = "async-radio")]
                        {
                            let send_downlink = Box::new(downlink.clone());
                            for sender in &senders {
                                if let Err(e) = sender.send(send_downlink.clone(), 0).await {
                                    error ! ("Error sending packet to virtual-lorawan-device instance: {e}");
                                }
                            }
                            downlink.ack().await?;
                        }
                    }
                }
            }
        } => resp
    )
}

trait DownlinkSender: Send + Sync + Clone + 'static {
    fn send(
        &self,
        downlink: Box<DownlinkRequest>,
        delayed_for: u64,
    ) -> impl std::future::Future<Output = Result> + Send;
}
