#[cfg(feature = "persistence")]
mod nvm;
pub mod radio;

use radio::*;

use crate::{metrics, settings, Credentials, DownlinkSender, Result};

use log::{debug, error, info, warn};
use lorawan_device::async_device::{
    radio::{PhyRxTx, RxConfig, Timer},
    Device, Downlink, JoinResponse, NetworkCredentials, SendResponse, Timings,
};
use lorawan_device::region::Configuration;
use lorawan_device::{AppEui, AppKey, DevEui, DeviceHandler, JoinMode};
pub use semtech_udp::client_runtime::{ClientTx, DownlinkRequest};
use std::str::FromStr;
use std::time::Instant;
use tokio::{
    sync::mpsc,
    time::{sleep, Duration},
};

struct ResetHandler {
    reset_tx: mpsc::UnboundedSender<()>,
}

impl DeviceHandler for ResetHandler {
    fn reboot(&mut self) {
        let _ = self.reset_tx.send(());
    }
}

#[cfg(feature = "persistence")]
type DeviceStack =
    Device<VirtualRadio, VirtualTimer, rand_core::OsRng, 512, 4, nvm::FileNvm, ResetHandler>;
#[cfg(not(feature = "persistence"))]
type DeviceStack = Device<
    VirtualRadio,
    VirtualTimer,
    rand_core::OsRng,
    512,
    4,
    lorawan_device::nvm::NoNvm,
    ResetHandler,
>;

pub struct VirtualDevice {
    label: String,
    metrics_sender: metrics::Sender,
    rejoin_frames: u32,
    secs_between_transmits: u64,
    secs_between_join_transmits: u64,
    credentials: NetworkCredentials,
    device: DeviceStack,
    state: State,
    /// Kept outside the device so a reboot can build a new radio on the same
    /// downlink channel and a new stack on the same store.
    time: Instant,
    client_tx: ClientTx,
    region: settings::Region,
    reset_tx: mpsc::UnboundedSender<()>,
    reset_rx: mpsc::UnboundedReceiver<()>,
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
    ) -> Result<(DS, Self)> {
        let (sender, receiver) = mpsc::channel(100);
        let (reset_tx, reset_rx) = mpsc::unbounded_channel();

        let device = Self::make_device(
            &label,
            time,
            client_tx.clone(),
            receiver,
            reset_tx.clone(),
            region.clone(),
        )
        .await?;

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
                time,
                client_tx,
                region,
                reset_tx,
                reset_rx,
            },
        ))
    }

    /// Build a device stack. With persistence this restores the identity
    /// (monotonic DevNonce, JoinNonce replay protection and the certification
    /// port state) from the store; the session is never persisted, so the
    /// result is unjoined either way.
    async fn make_device(
        label: &str,
        time: Instant,
        client_tx: ClientTx,
        receiver: mpsc::Receiver<Box<DownlinkRequest>>,
        reset_tx: mpsc::UnboundedSender<()>,
        region: settings::Region,
    ) -> Result<DeviceStack> {
        let _ = label;
        let radio = VirtualRadio {
            receiver,
            client_tx,
            time,
            rx_config: None,
        };
        let timer = VirtualTimer(time);

        #[cfg(feature = "persistence")]
        let mut device = {
            let store = nvm::FileNvm::new(nvm::state_dir(label))?;
            info!("{}: NVM state directory {:?}", label, store.dir());
            Device::restore_with_handler(
                ResetHandler { reset_tx },
                Configuration::new(region.into()),
                radio,
                timer,
                rand_core::OsRng,
                store,
            )
            .await?
        };

        #[cfg(not(feature = "persistence"))]
        let mut device = Device::new_with_handler(
            ResetHandler { reset_tx },
            Configuration::new(region.into()),
            radio,
            timer,
            rand_core::OsRng,
        );

        // TODO: Make Class C optional
        device.enable_class_c();

        #[cfg(feature = "multicast")]
        device.set_multicast_ke_key(lorawan_device::async_device::McRootKey::from_str(
            "3371C1F28C755A5D328DFDD6E01A32ED",
        )?);

        Ok(device)
    }

    /// Simulate the MCU reboot requested by the stack: rebuild the device from
    /// its store instead of reusing the reset one. The stack dropped the
    /// in-memory identity on the reboot event, so continuing with it would draw
    /// random DevNonces and violate the LoRaWAN 1.0.4 monotonic DevNonce rule.
    async fn reboot(&mut self) -> Result {
        info!("{} reboot requested, reloading persisted state", self.label);

        // Keep the downlink channel paired with the packet forwarder's sender:
        // take the receiver back out of the old radio before dropping it.
        let (dummy_tx, dummy_rx) = mpsc::channel(1);
        let receiver = std::mem::replace(&mut self.device.get_mut_radio().receiver, dummy_rx);
        drop(dummy_tx);

        self.device = Self::make_device(
            &self.label,
            self.time,
            self.client_tx.clone(),
            receiver,
            self.reset_tx.clone(),
            self.region.clone(),
        )
        .await?;
        self.state = State::NotJoined;
        Ok(())
    }

    pub async fn run(mut self) -> Result {
        // stagger the starts slightly
        let random = rand::random::<u64>() % 1000;
        sleep(Duration::from_millis(random)).await;

        loop {
            // Drain a reset handled during the previous operation.
            if self.reset_rx.try_recv().is_ok() {
                self.reboot().await?;
            }

            // The MAC layer can be reset, dropping the session.
            if matches!(self.state, State::Joined) && self.device.get_session().is_none() {
                self.state = State::NotJoined;
            }

            let result = match self.state {
                State::NotJoined => self.do_join().await,
                State::Joined => self.do_send().await,
            };
            let mut duration = match result {
                Ok(d) => d,
                Err(e) => {
                    error!("{} error: {:?}", self.label, e);
                    Duration::from_secs(0)
                }
            };

            // The certification protocol can override the uplink periodicity.
            if let Some(seconds) = self.device.get_override_periodicity() {
                duration = Duration::from_secs(seconds.into());
            }

            let mut reboot = false;
            tokio::select!(
                _ = sleep(duration) => {},
                _ = self.reset_rx.recv() => reboot = true,
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
            );

            if reboot {
                self.reboot().await?;
            }
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
            SendResponse::DownlinkReceived(fcnt) => {
                while let Some(downlink) = self.device.take_downlink() {
                    self.handle_downlink(downlink, false);
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
