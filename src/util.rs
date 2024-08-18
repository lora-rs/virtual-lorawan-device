use lorawan_device::nb_device::radio;
use semtech_udp::push_data::RxPkV1;
use semtech_udp::{push_data, CodingRate, DataRate};

pub(crate) fn tx_request_to_rxpk(
    settings: Settings,
    buffer: &[u8],
    tmst: u32,
) -> push_data::Packet {
    use push_data::{Packet, RxPk, CRC};
    let rxpk = RxPkV1 {
        chan: 0,
        data: Vec::from(buffer),
        size: buffer.len() as u64,
        codr: Some(settings.get_codr()),
        datr: settings.get_datr(),
        freq: settings.get_freq(),
        lsnr: 5.5,
        modu: semtech_udp::Modulation::LORA,
        rfch: 0,
        rssi: -112,
        rssis: None,
        stat: CRC::OK,
        tmst,
        time: None,
    };
    let mut packet = Packet::from_rxpk([0, 0, 0, 0, 0, 0, 0, 0].into(), RxPk::V1(rxpk));

    packet.data.stat = Some(push_data::Stat {
        time: chrono::Utc::now().to_string(),
        lati: None, //37.8507396,
        long: None, //-122.2817759,
        alti: None,
        rxnb: 0,
        rxok: 1,
        rxfw: 0,
        ackr: None,
        dwnb: 0,
        txnb: 0,
        temp: None,
    });
    packet.into()
}

#[derive(Debug)]
pub struct Settings {
    rf_config: radio::RfConfig,
}

impl Default for Settings {
    fn default() -> Settings {
        Settings {
            rf_config: radio::RfConfig {
                frequency: 903000000,

                bb: radio::BaseBandModulationParams::new(
                    radio::SpreadingFactor::_7,
                    radio::Bandwidth::_125KHz,
                    radio::CodingRate::_4_5,
                ),
            },
        }
    }
}

impl From<radio::RfConfig> for Settings {
    fn from(rf_config: radio::RfConfig) -> Settings {
        Settings { rf_config }
    }
}

impl Settings {
    pub fn get_datr(&self) -> DataRate {
        DataRate::new(self.rf_config.bb.sf.into(), self.rf_config.bb.bw.into())
    }

    pub fn get_codr(&self) -> CodingRate {
        match self.rf_config.bb.cr {
            radio::CodingRate::_4_5 => CodingRate::_4_5,
            radio::CodingRate::_4_6 => CodingRate::_4_6,
            radio::CodingRate::_4_7 => CodingRate::_4_7,
            radio::CodingRate::_4_8 => CodingRate::_4_8,
        }
    }

    pub fn get_freq(&self) -> f64 {
        self.rf_config.frequency as f64 / 1_000_000.0
    }
}
