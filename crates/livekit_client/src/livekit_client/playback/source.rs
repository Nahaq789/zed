use std::num::NonZero;

use futures::StreamExt;
use libwebrtc::{audio_stream::native::NativeAudioStream, prelude::AudioFrame};
use livekit::track::RemoteAudioTrack;
use rodio::{Source, buffer::SamplesBuffer, conversions::SampleTypeConverter};

use crate::livekit_client::playback::{NUM_CHANNELS, SAMPLE_RATE};

fn frame_to_samplesbuffer(frame: AudioFrame) -> SamplesBuffer {
    let samples = frame.data.iter().copied();
    let samples = SampleTypeConverter::<_, _>::new(samples);
    let samples: Vec<f32> = samples.collect();
    SamplesBuffer::new(
        NonZero::new(frame.num_channels as u16).expect("audio frame channels is nonzero"),
        NonZero::new(frame.sample_rate).expect("audio frame sample rate is nonzero"),
        samples,
    )
}

pub struct LiveKitStream {
    // shared_buffer: SharedBuffer,
    inner: rodio::queue::SourcesQueueOutput,
    _receiver_task: gpui::Task<()>,
}

impl LiveKitStream {
    pub fn new(executor: &gpui::BackgroundExecutor, track: &RemoteAudioTrack) -> Self {
        let mut stream =
            NativeAudioStream::new(track.rtc_track(), SAMPLE_RATE as i32, NUM_CHANNELS as i32);
        let (queue_input, queue_output) = rodio::queue::queue(true);
        // spawn rtc stream
        let receiver_task = executor.spawn({
            async move {
                while let Some(frame) = stream.next().await {
                    let samples = frame_to_samplesbuffer(frame);
                    queue_input.append(samples);
                }
            }
        });

        LiveKitStream {
            _receiver_task: receiver_task,
            inner: queue_output,
        }
    }
}

impl Iterator for LiveKitStream {
    type Item = rodio::Sample;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl Source for LiveKitStream {
    fn current_span_len(&self) -> Option<usize> {
        self.inner.current_span_len()
    }

    fn channels(&self) -> rodio::ChannelCount {
        self.inner.channels()
    }

    fn sample_rate(&self) -> rodio::SampleRate {
        self.inner.sample_rate()
    }

    fn total_duration(&self) -> Option<std::time::Duration> {
        self.inner.total_duration()
    }
}

pub trait RodioExt: Source + Sized {
    fn process_buffer<F>(self, callback: F) -> ProcessBuffer<Self, F>
    where
        F: FnMut(&mut [rodio::Sample; 200]);
}

impl<S: Source> RodioExt for S {
    fn process_buffer<F>(self, callback: F) -> ProcessBuffer<Self, F>
    where
        F: FnMut(&mut [rodio::Sample; 200]),
    {
        ProcessBuffer {
            inner: self,
            callback,
            buffer: [0.0; 200],
            next: 200,
        }
    }
}

pub struct ProcessBuffer<S, F>
where
    S: Source + Sized,
    F: FnMut(&mut [rodio::Sample; 200]),
{
    inner: S,
    callback: F,
    buffer: [rodio::Sample; 200],
    next: usize,
}

impl<S, F> Iterator for ProcessBuffer<S, F>
where
    S: Source + Sized,
    F: FnMut(&mut [rodio::Sample; 200]),
{
    type Item = rodio::Sample;

    fn next(&mut self) -> Option<Self::Item> {
        self.next += 1;
        if self.next < self.buffer.len() {
            let sample = self.buffer[self.next];
            return Some(sample);
        }

        for sample in &mut self.buffer {
            *sample = self.inner.next()?
        }
        (self.callback)(&mut self.buffer);

        self.next = 0;
        Some(self.buffer[0])
    }
}

// TODO dvdsk this should be a spanless Source
impl<S, F> Source for ProcessBuffer<S, F>
where
    S: Source + Sized,
    F: FnMut(&mut [rodio::Sample; 200]),
{
    fn current_span_len(&self) -> Option<usize> {
        None
    }

    fn channels(&self) -> rodio::ChannelCount {
        self.inner.channels()
    }

    fn sample_rate(&self) -> rodio::SampleRate {
        self.inner.sample_rate()
    }

    fn total_duration(&self) -> Option<std::time::Duration> {
        self.inner.total_duration()
    }
}
