use framing::{FrameType, Frame, FramePayload, MethodFrame};
use amqp_error::AMQPResult;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct EncodedMethod(Vec<u8>);

impl EncodedMethod {
    pub fn new(data: Vec<u8>) -> Self {
        EncodedMethod(data)
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }

    pub fn inner(&self) -> &[u8] {
        &self.0
    }
}

pub trait Method {
    fn decode(method_frame: MethodFrame) -> AMQPResult<Self> where Self: Sized;
    fn encode(&self) -> AMQPResult<EncodedMethod>;
    fn name(&self) -> &'static str;
    fn id(&self) -> u16;
    fn class_id(&self) -> u16;

    fn encode_method_frame(&self) -> AMQPResult<FramePayload> {
        let frame = MethodFrame { class_id: self.class_id(), method_id: self.id(), arguments: self.encode()? };
        frame.encode()
    }

    fn to_frame(&self, channel: u16) -> AMQPResult<Frame> {
        Ok(Frame {
            frame_type: FrameType::METHOD,
            channel: channel,
            payload: self.encode_method_frame()?,
        })
    }
}
