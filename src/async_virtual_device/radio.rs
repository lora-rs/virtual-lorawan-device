use crate::{
    util::{tx_request_to_rxpk, Settings},
    DownlinkSender,
};
use log::{info, warn};
use lorawan_device::async_device::radio::{
    PhyRxTx, RxConfig, RxMode, RxQuality, RxStatus, Timer, TxConfig,
};
use lorawan_device::async_device::Timings;
use lorawan_device::nb_device::radio;
use semtech_udp::client_runtime::{ClientTx, DownlinkRequest};
use std::mem::offset_of;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time::sleep;

#[derive(Clone)]
pub struct DS(pub mpsc::Sender<Box<DownlinkRequest>>);

impl DownlinkSender for DS {
    async fn send(&self, downlink: Box<DownlinkRequest>, _delayed_for: u64) -> crate::Result {
        self.0
            .send(downlink)
            .await
            .map_err(|_| crate::Error::SendingDownlinkToUdpRadio)
    }
}

pub struct VirtualRadio {
    pub receiver: mpsc::Receiver<Box<DownlinkRequest>>,
    pub client_tx: ClientTx,
    pub time: Instant,
    pub rx_config: Option<RxConfig>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Error sending uplink to UDP radio")]
    SendingUplinktoUdpRadio,
    #[error("Error receiving downlink from UDP Radio receiver")]
    ReceivingDownlinkfromUdpRadio,
    #[error("Receive called when no RX config is set")]
    NoRxConfig,
    #[error("Continuous receive called when RxConfig is not continuous")]
    ContinuousReceiveCalledWhenRxConfigIsNotContinuous,
    #[error("Single called when RxConfig is not single")]
    SingleCalledWhenRxConfigIsNotSingle,
}

impl PhyRxTx for VirtualRadio {
    type PhyError = Error;
    const ANTENNA_GAIN: i8 = 0;
    const MAX_RADIO_POWER: u8 = 26;

    async fn tx(&mut self, config: TxConfig, buf: &[u8]) -> Result<u32, Self::PhyError> {
        let tmst = self.time.elapsed().as_micros() as u32;
        let settings = Settings::from(config.rf);
        info!(
            "Transmit @ {tmst} on {} Hz {:?}",
            settings.get_freq(),
            settings.get_datr()
        );
        let packet = tx_request_to_rxpk(settings, &buf, tmst);
        self.client_tx
            .send(packet)
            .await
            .map_err(|_| Error::SendingUplinktoUdpRadio)?;
        Ok(0)
    }

    async fn setup_rx(&mut self, config: RxConfig) -> Result<(), Self::PhyError> {
        self.rx_config = Some(config);
        Ok(())
    }

    async fn rx_continuous(
        &mut self,
        rx_buf: &mut [u8],
    ) -> Result<(usize, RxQuality), Self::PhyError> {
        loop {
            if let Some(rx_config) = self.rx_config {
                if let RxMode::Continuous = rx_config.mode {
                    if let Some((len, quality)) = self.receive(rx_buf).await? {
                        return Ok((len, quality));
                    }
                } else {
                    return Err(Error::ContinuousReceiveCalledWhenRxConfigIsNotContinuous);
                }
            } else {
                return Err(Error::NoRxConfig);
            }
        }
    }

    async fn rx_single(&mut self, buf: &mut [u8]) -> Result<RxStatus, Self::PhyError> {
        loop {
            if let Some(rx_config) = self.rx_config {
                if let RxMode::Single { ms } = rx_config.mode {
                    tokio::select!(
                        rx = self.receive(buf) => {
                            if let Some((len, quality)) = rx? {
                                return Ok(RxStatus::Rx(len, quality));
                            }
                        }
                        _ = tokio::time::sleep(Duration::from_millis(ms.into())) => {
                            return Ok(RxStatus::RxTimeout);
                        },
                    )
                } else {
                    return Err(Error::SingleCalledWhenRxConfigIsNotSingle);
                }
            } else {
                return Err(Error::NoRxConfig);
            }
        }
    }
}

impl VirtualRadio {
    async fn receive(&mut self, buf: &mut [u8]) -> Result<Option<(usize, RxQuality)>, Error> {
        match self.rx_config {
            Some(rx_config) => {
                let rx = self
                    .receiver
                    .recv()
                    .await
                    .ok_or(Error::ReceivingDownlinkfromUdpRadio)?;
                let (sf, bw) = (
                    radio::SpreadingFactor::from(rx.pull_resp.data.txpk.datr.spreading_factor()),
                    rx.pull_resp.data.txpk.datr.bandwidth().into(),
                );
                if rx_config.rf.bb.sf == sf && rx_config.rf.bb.bw == bw {
                    let len = rx.pull_resp.data.txpk.data.len();
                    buf[..len].copy_from_slice(&rx.pull_resp.data.txpk.data.data());
                    Ok(Some((len, RxQuality::new(-86, 1))))
                } else {
                    warn!(
                        "Received packet with SF {:?} and BW {:?} but expected SF {:?} and BW {:?}",
                        sf, bw, rx_config.rf.bb.sf, rx_config.rf.bb.bw
                    );
                    Ok(None)
                }
            }
            None => Err(Error::NoRxConfig),
        }
    }
}

impl Timings for VirtualRadio {
    fn get_rx_window_lead_time_ms(&self) -> u32 {
        10
    }
    fn get_rx_window_buffer(&self) -> u32 {
        20
    }
}

pub struct VirtualTimer(pub Instant);

impl Timer for VirtualTimer {
    fn reset(&mut self) {
        self.0 = Instant::now();
    }

    async fn at(&mut self, millis: u64) {
        let delay = millis - self.0.elapsed().as_millis() as u64;
        if delay > 0 {
            sleep(Duration::from_millis(delay)).await;
        }
    }

    async fn delay_ms(&mut self, millis: u64) {
        tokio::time::sleep(Duration::from_millis(millis)).await;
    }
}
