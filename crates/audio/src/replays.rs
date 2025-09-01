use anyhow::Context;
use async_tar::{Builder, Header};
use gpui::{BackgroundExecutor, Task};

use collections::HashMap;
use parking_lot::Mutex;
use rodio::Source;
use smol::fs::File;
use std::{io, path::PathBuf, sync::Arc};

use crate::rodio_ext::Replay;

#[derive(Default, Clone)]
pub(crate) struct Replays(Arc<Mutex<HashMap<String, Replay>>>);

impl Replays {
    pub(crate) fn add_output_stream(&mut self, stream_name: String, source: Replay) {
        let mut map = self.0.lock();
        map.retain(|_, replay| replay.source_is_active());
        map.insert(stream_name, source);
    }

    pub(crate) fn replays_to_tar(
        &self,
        executor: BackgroundExecutor,
    ) -> Task<anyhow::Result<PathBuf>> {
        let map = Arc::clone(&self.0);
        executor.spawn(async move {
            let recordings: Vec<_> = map
                .lock()
                .iter_mut()
                .map(|(name, replay)| (name.clone(), replay.record()))
                .collect();

            let path = std::env::current_dir()
                .context("Could not get current dir")?
                .join("replays.tar");
            let tar = File::create(&path)
                .await
                .context("Could not create file for tar")?;

            let mut header = Header::new_gnu();
            header.set_size(4);
            header.set_cksum();
            let mut tar = Builder::new(tar);

            for (name, recording) in recordings {
                let mut writer = io::Cursor::new(Vec::new());
                rodio::collect_to_wav(recording, &mut writer).context("failed to encode wav")?;
                writer.set_position(0);
                tar.append_data(&mut header, name, writer.into_inner().as_slice())
                    .await
                    .context("failed to apped wav to tar")?;
            }
            tar.finish().await.context("Could not finish writing tar")?;
            Ok(path)
        })
    }
}
