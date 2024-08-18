use crate::util::{tx_request_to_rxpk, Settings};
use log::info;
use lorawan_device::{nb_device::radio, Timings};
use semtech_udp::client_runtime::{ClientTx, DownlinkRequest};
use std::time::{Duration, Instant};
pub use tokio::sync::mpsc;
use tokio::time::sleep;
#[derive(Debug)]
// I need some intermediate event because of Lifetimes
// maybe there's a cleaner way of doing this
pub enum IntermediateEvent {
    RadioEvent(Box<DownlinkRequest>, u64),
    NewSession,
    Timeout(usize),
    SendPacket(Vec<u8>, u8, bool),
}

#[derive(Debug)]
pub enum Response {}

#[derive(Debug)]
pub struct UdpRadio {
    client_tx: ClientTx,
    lorawan_sender: mpsc::Sender<IntermediateEvent>,
    time: Instant,
    settings: Settings,
    timeout_id: usize,
    window_start: u32,
    rx_buffer: [u8; 512],
    pos: usize,
}

impl UdpRadio {
    pub async fn new(
        time: Instant,
        client_tx: ClientTx,
    ) -> (
        UdpRadio,
        mpsc::Receiver<IntermediateEvent>,
        mpsc::Sender<IntermediateEvent>,
    ) {
        let (lorawan_sender, lorawan_receiver) = mpsc::channel(100);
        (
            UdpRadio {
                time,
                settings: Settings::default(),
                client_tx,
                timeout_id: 0,
                lorawan_sender: lorawan_sender.clone(),
                window_start: 0,
                rx_buffer: [0; 512],
                pos: 0,
            },
            lorawan_receiver,
            lorawan_sender,
        )
    }

    pub async fn timer(&mut self, future_time: u32) {
        let timeout_id = rand::random::<usize>();
        self.timeout_id = timeout_id;
        // units are in millis here because
        // the lorawan device stack operates in millis
        let elapsed = self.time.elapsed().as_millis() as u32;
        // only kick out the packet if its on time
        if future_time > elapsed {
            let delay = future_time - elapsed;
            let sender = self.lorawan_sender.clone();

            tokio::spawn(async move {
                sleep(Duration::from_millis(delay as u64)).await;
                sender
                    .send(IntermediateEvent::Timeout(timeout_id))
                    .await
                    .unwrap()
            });
            self.window_start = delay;
        }
    }

    pub fn most_recent_timeout(&mut self, timeout_id: usize) -> bool {
        self.timeout_id == timeout_id
    }
}

use lorawan_device::nb_device::radio::{Event as LoraEvent, Response as LoraResponse, RxQuality};

impl radio::PhyRxTx for UdpRadio {
    type PhyEvent = Box<DownlinkRequest>;
    type PhyError = Error;

    type PhyResponse = Response;
    const ANTENNA_GAIN: i8 = 3;
    const MAX_RADIO_POWER: u8 = 20;

    fn get_mut_radio(&mut self) -> &mut Self {
        self
    }

    fn get_received_packet(&mut self) -> &mut [u8] {
        &mut self.rx_buffer[0..self.pos]
    }

    fn handle_event(&mut self, event: LoraEvent<Self>) -> Result<LoraResponse<Self>, Error> {
        match event {
            radio::Event::TxRequest(tx_config, buffer) => {
                let tmst = self.time.elapsed().as_micros() as u32;
                let settings = Settings::from(tx_config.rf);
                info!(
                    "Transmit @ {tmst} on {} Hz {:?}",
                    settings.get_freq(),
                    settings.get_datr()
                );
                let packet = tx_request_to_rxpk(settings, &buffer, tmst);
                let sender = self.client_tx.clone();
                tokio::spawn(async move {
                    if let Err(e) = sender.send(packet).await {
                        panic!("UdpTx Queue Overflow! {}", e)
                    }
                });

                // units are in millis here because
                // the lorawan device stack operates in millis
                Ok(radio::Response::TxDone(
                    self.time.elapsed().as_millis() as u32
                ))
            }
            radio::Event::RxRequest(config) => {
                self.settings = Settings::from(config);
                Ok(radio::Response::Idle)
            }
            radio::Event::CancelRx => Ok(radio::Response::Idle),
            radio::Event::Phy(packet) => {
                let data = packet.pull_resp.data.txpk.data.as_ref();
                self.pos = data.len();
                for (i, el) in data.iter().enumerate() {
                    self.rx_buffer[i] = *el;
                }
                Ok(LoraResponse::RxDone(RxQuality::new(-120, 5)))
            }
        }
    }
}

impl Timings for UdpRadio {
    fn get_rx_window_offset_ms(&self) -> i32 {
        20
    }
    fn get_rx_window_duration_ms(&self) -> u32 {
        100
    }
}

#[derive(Debug)]
pub enum Error {}
