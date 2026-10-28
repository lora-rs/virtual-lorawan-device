//! File-backed [`NonVolatileStore`].

use lorawan_device::nvm::{NonVolatileStoreSync, NvmRegion};
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// Per-device state directory, relative to the working directory.
pub fn state_dir(label: &str) -> PathBuf {
    PathBuf::from("state").join(label)
}

fn file_name(region: NvmRegion) -> &'static str {
    match region {
        NvmRegion::Identity => "identity",
        NvmRegion::Session => "session",
    }
}

pub struct FileNvm {
    dir: PathBuf,
}

impl FileNvm {
    pub fn new(dir: impl Into<PathBuf>) -> io::Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    fn path(&self, region: NvmRegion) -> PathBuf {
        self.dir.join(file_name(region))
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

impl NonVolatileStoreSync for FileNvm {
    type Error = io::Error;

    fn save(&mut self, region: NvmRegion, bytes: &[u8]) -> io::Result<()> {
        let tmp = self.dir.join(format!("{}.tmp", file_name(region)));
        let mut file = File::create(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&tmp, self.path(region))?;
        let _ = File::open(&self.dir).and_then(|dir| dir.sync_all());
        Ok(())
    }

    fn load(&mut self, region: NvmRegion, buf: &mut [u8]) -> io::Result<Option<usize>> {
        match fs::read(self.path(region)) {
            Ok(data) => {
                if data.len() > buf.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "stored blob exceeds caller buffer",
                    ));
                }
                buf[..data.len()].copy_from_slice(&data);
                Ok(Some(data.len()))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }
}
