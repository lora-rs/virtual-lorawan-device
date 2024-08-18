use crate::*;
use thiserror::Error;

pub type Result<T = ()> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("udp_radio receive closed or overflowed")]
    UdpRadioClosed,
    #[error("unable to parse socket address")]
    AddrParse(#[from] std::net::AddrParseError),
    #[error("configuration file error")]
    Config(#[from] config::ConfigError),
    #[error("invalid hex input")]
    InvalidHex(#[from] hex::FromHexError),
    #[error("io error")]
    IoError(#[from] std::io::Error),
    #[error("metrics channel error")]
    MetricsChannel,
    #[error("semtech_udp client_runtime error")]
    SemtechUdpClientRuntime(#[from] semtech_udp::client_runtime::Error),
    #[error("invalid region string")]
    InvalidRegionString(String),
    #[error("error sending downlink to upd_radio instance")]
    SendingDownlinkToUdpRadio,
    #[error("receive channel from semtech_udp::client_runtime unexpectedly closed")]
    RxChannelSemtechUdpClientRuntimeClosed,
    #[error("tokio join error: {0}")]
    TokioJoin(tokio::task::JoinError),
    #[cfg(feature = "async-radio")]
    #[error("async radio error")]
    AsyncLorawanRadio(lorawan_device::async_device::Error<async_virtual_device::radio::Error>),
}
