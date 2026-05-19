use std::pin::Pin;
use std::task::{Context, Poll};

use raccoon_contract_object_store::{ByteStream, Bytes, ObjectStoreError, Result, Stream};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;

use crate::error::map_io_error_with_message;

pub(crate) async fn write_body(file: &mut File, body: ByteStream) -> Result<()> {
    let mut stream = body.into_stream();

    while let Some(chunk) = std::future::poll_fn(|context| stream.as_mut().poll_next(context)).await
    {
        file.write_all(&chunk?)
            .await
            .map_err(|err| map_io_error_with_message("failed to write object body", err, None))?;
    }

    Ok(())
}

pub(crate) struct FileByteStream {
    inner: ReaderStream<File>,
}

impl FileByteStream {
    pub(crate) fn new(file: File) -> Self {
        Self {
            inner: ReaderStream::new(file),
        }
    }
}

impl Stream for FileByteStream {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(context) {
            Poll::Ready(Some(Ok(bytes))) => Poll::Ready(Some(Ok(bytes))),
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(
                ObjectStoreError::backend_with_source("failed to read object body", err),
            ))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
