pub mod radio;

use radio::*;

use crate::{metrics, settings, Credentials, Result};

use log::{error, info};
use lorawan_device::async_device::{
    Device, Downlink, JoinResponse, NetworkCredentials, SendResponse,
};
use lorawan_device::region::Configuration;
use lorawan_device::{AppEui, AppKey, DevEui, JoinMode};
use semtech_udp::client_runtime::ClientTx;
use std::str::FromStr;
use std::time::Instant;
use tokio::{
    sync::{mpsc, oneshot},
    time::{sleep, Duration},
};

#[allow(dead_code)]
pub struct VirtualDevice {
    label: String,
    metrics_sender: metrics::Sender,
    rejoin_frames: u32,
    secs_between_transmits: u64,
    secs_between_join_transmits: u64,
    credentials: NetworkCredentials,
    device: Device<VirtualRadio, VirtualTimer, rand_core::OsRng, 512, 4>,
    state: State,
    smoke_test_tx: Option<oneshot::Sender<()>>,
    smoke_test_done: bool,
}

enum State {
    Joined,
    NotJoined,
}

impl VirtualDevice {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        label: String,
        time: Instant,
        client_tx: ClientTx,
        credentials: Credentials,
        metrics_sender: metrics::Sender,
        rejoin_frames: u32,
        secs_between_transmits: u64,
        secs_between_join_transmits: u64,
        region: settings::Region,
        smoke_test_tx: Option<oneshot::Sender<()>>,
    ) -> Result<(DS, Self)> {
        let (sender, receiver) = mpsc::channel(100);
        let mut device = Device::new(
            Configuration::new(region.into()),
            VirtualRadio {
                receiver,
                client_tx,
                time,
                rx_config: None,
            },
            VirtualTimer(time),
            rand_core::OsRng,
        );
        device.enable_class_c();
        #[cfg(feature = "multicast")]
        device.set_multicast_ke_key(lorawan_device::async_device::McRootKey::from_str(
            "3371C1F28C755A5D328DFDD6E01A32ED",
        )?);
        Ok((
            DS(sender),
            Self {
                label,
                metrics_sender,
                rejoin_frames,
                secs_between_transmits,
                secs_between_join_transmits,
                credentials: NetworkCredentials::new(
                    AppEui::from_str(&credentials.app_eui)?,
                    DevEui::from_str(&credentials.dev_eui)?,
                    AppKey::from_str(&credentials.app_key)?,
                ),
                device,
                state: State::NotJoined,
                smoke_test_tx,
                smoke_test_done: false,
            },
        ))
    }

    pub async fn run(mut self) -> Result {
        // stagger the starts slightly
        let random = rand::random::<u64>() % 1000;
        sleep(Duration::from_millis(random)).await;

        loop {
            let result = match self.state {
                State::NotJoined => self.do_join().await,
                State::Joined => self.do_send().await,
            };
            let duration = match result {
                Ok(d) => d,
                Err(e) => {
                    error!("{} error: {:?}", self.label, e);
                    Duration::from_secs(0)
                }
            };

            if self.smoke_test_done {
                return Ok(());
            }

            tokio::select!(
                _ = sleep(duration) => {},
                result = self.device.rxc_listen() => {
                    match result {
                        Ok(response) => {
                            println!("response: {:?}", response);
                        }
                        Err(e) => {
                            error!("{} error: {:?}", self.label, e);
                        }
                    }
                    while let Some(downlink) = self.device.take_downlink() {
                        self.handle_downlink(downlink, true);
                    }
                }
            )
        }
    }

    async fn do_join(&mut self) -> Result<Duration> {
        let join_response = self
            .device
            .join(&JoinMode::OTAA {
                deveui: self.credentials.deveui().clone(),
                appeui: self.credentials.appeui().clone(),
                appkey: self.credentials.appkey().clone(),
            })
            .await
            .map_err(|e| crate::Error::AsyncLorawanRadio(e))?;

        match join_response {
            JoinResponse::JoinSuccess => {
                self.state = State::Joined;
                info!("{} joined successfully", self.label);
                Ok(Duration::from_secs(0))
            }
            JoinResponse::NoJoinAccept => {
                error!("{} failed to join", self.label);
                Ok(Duration::from_secs(self.secs_between_join_transmits))
            }
        }
    }

    async fn do_send(&mut self) -> Result<Duration> {
        let send_response = self
            .device
            .send(
                &vec![
                    rand::random(),
                    rand::random(),
                    rand::random(),
                    rand::random(),
                ],
                5,
                true,
            )
            .await
            .map_err(|e| crate::Error::AsyncLorawanRadio(e))?;
        match send_response {
            SendResponse::SessionExpired => {}
            SendResponse::NoAck => {}
            SendResponse::DownlinkReceived(_fcnt) => {
                while let Some(downlink) = self.device.take_downlink() {
                    self.handle_downlink(downlink, false);
                }
                if let Some(tx) = self.smoke_test_tx.take() {
                    info!("{} smoke test complete", self.label);
                    let _ = tx.send(());
                    self.smoke_test_done = true;
                    return Ok(Duration::from_secs(0));
                }
            }
            SendResponse::RxComplete => {}
        }
        println!("{}", self.secs_between_transmits);
        Ok(Duration::from_secs(self.secs_between_transmits))
    }

    fn handle_downlink(&mut self, downlink: Downlink, is_class_c: bool) {
        let data_len = downlink.data.len();
        let fport = downlink.fport;
        info!(
            "{} downlink received: len = {data_len}, fport = {fport}, class_c = {is_class_c}",
            self.label
        );
        info!("downlink data: {:?}", downlink.data);
    }
}
