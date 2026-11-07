use bytes::{Buf, BufMut, BytesMut};
use lockin_proto::message::Message;
use tokio::io;
use tokio_util::codec::{Decoder, Encoder};

#[derive(Clone)]
pub struct Codec {}

impl Codec {
    pub fn new() -> Self {
        Self {}
    }
}

impl Encoder<Message> for Codec {
    type Error = io::Error;
    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let payload =
            serde_json::to_vec(&item).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        dst.put_u32(payload.len() as u32);
        dst.put_slice(&payload);
        Ok(())
    }
}

impl Decoder for Codec {
    type Item = Message;
    type Error = io::Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // 4 byte length header
        if src.len() < 4 {
            //not enough bytes yet
            return Ok(None);
        }

        let len = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
        let frame_len = 4 + len;

        if src.len() < frame_len {
            src.reserve(frame_len - src.len());
            //not enough bytes yet
            return Ok(None);
        }

        let mut frame = src.split_to(frame_len);
        //skip header
        frame.advance(4);

        let msg: Message = serde_json::from_slice(&frame)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(Some(msg))
    }
}
