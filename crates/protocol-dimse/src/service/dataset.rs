use std::collections::VecDeque;

use raccoon_contract_object_store::{ByteStream, Bytes};

use crate::error::DimseError;

const DICOM_FILE_PREAMBLE_AND_PREFIX_LEN: usize = 132;
const DICOM_FILE_PREFIX_OFFSET: usize = 128;
const FILE_META_GROUP_LENGTH_OFFSET: usize = DICOM_FILE_PREAMBLE_AND_PREFIX_LEN + 8;
const FILE_META_GROUP_LENGTH_VALUE_END: usize = FILE_META_GROUP_LENGTH_OFFSET + 4;

pub(super) async fn prepare_dimse_dataset(
    body: &mut ByteStream,
) -> Result<VecDeque<Bytes>, DimseError> {
    let mut buffered = read_until(body, DICOM_FILE_PREAMBLE_AND_PREFIX_LEN).await?;

    if buffered_len(&buffered) < DICOM_FILE_PREAMBLE_AND_PREFIX_LEN {
        return Ok(buffered);
    }

    let mut prefix = Vec::with_capacity(DICOM_FILE_PREAMBLE_AND_PREFIX_LEN);
    copy_prefix(&buffered, DICOM_FILE_PREAMBLE_AND_PREFIX_LEN, &mut prefix);
    if &prefix[DICOM_FILE_PREFIX_OFFSET..DICOM_FILE_PREAMBLE_AND_PREFIX_LEN] != b"DICM" {
        return Ok(buffered);
    }

    buffered = read_until_with(body, FILE_META_GROUP_LENGTH_VALUE_END, buffered).await?;
    let mut file_meta_header = Vec::with_capacity(FILE_META_GROUP_LENGTH_VALUE_END);
    copy_prefix(
        &buffered,
        FILE_META_GROUP_LENGTH_VALUE_END,
        &mut file_meta_header,
    );
    if file_meta_header.len() < FILE_META_GROUP_LENGTH_VALUE_END {
        return Err(DimseError::protocol(
            "DICOM Part 10 file ended before file meta information group length",
        ));
    }

    let group = u16::from_le_bytes([
        file_meta_header[DICOM_FILE_PREAMBLE_AND_PREFIX_LEN],
        file_meta_header[DICOM_FILE_PREAMBLE_AND_PREFIX_LEN + 1],
    ]);
    let element = u16::from_le_bytes([
        file_meta_header[DICOM_FILE_PREAMBLE_AND_PREFIX_LEN + 2],
        file_meta_header[DICOM_FILE_PREAMBLE_AND_PREFIX_LEN + 3],
    ]);
    let vr = &file_meta_header
        [DICOM_FILE_PREAMBLE_AND_PREFIX_LEN + 4..DICOM_FILE_PREAMBLE_AND_PREFIX_LEN + 6];
    let value_len = u16::from_le_bytes([
        file_meta_header[DICOM_FILE_PREAMBLE_AND_PREFIX_LEN + 6],
        file_meta_header[DICOM_FILE_PREAMBLE_AND_PREFIX_LEN + 7],
    ]);
    if group != 0x0002 || element != 0x0000 || vr != b"UL" || value_len != 4 {
        return Err(DimseError::protocol(
            "DICOM Part 10 file is missing file meta information group length",
        ));
    }

    let meta_len = u32::from_le_bytes([
        file_meta_header[FILE_META_GROUP_LENGTH_OFFSET],
        file_meta_header[FILE_META_GROUP_LENGTH_OFFSET + 1],
        file_meta_header[FILE_META_GROUP_LENGTH_OFFSET + 2],
        file_meta_header[FILE_META_GROUP_LENGTH_OFFSET + 3],
    ]) as usize;
    let dataset_offset = FILE_META_GROUP_LENGTH_VALUE_END
        .checked_add(meta_len)
        .ok_or_else(|| DimseError::protocol("DICOM Part 10 file meta length overflow"))?;

    let buffered = read_until_with(body, dataset_offset, buffered).await?;
    Ok(skip_prefix(buffered, dataset_offset))
}

async fn read_until(body: &mut ByteStream, min_len: usize) -> Result<VecDeque<Bytes>, DimseError> {
    read_until_with(body, min_len, VecDeque::new()).await
}

async fn read_until_with(
    body: &mut ByteStream,
    min_len: usize,
    mut buffered: VecDeque<Bytes>,
) -> Result<VecDeque<Bytes>, DimseError> {
    use futures_util::StreamExt;

    while buffered_len(&buffered) < min_len {
        let Some(chunk) = body.next().await else {
            break;
        };
        buffered.push_back(chunk.map_err(|error| DimseError::protocol(error.to_string()))?);
    }
    Ok(buffered)
}

fn buffered_len(buffered: &VecDeque<Bytes>) -> usize {
    buffered.iter().map(Bytes::len).sum()
}

fn copy_prefix(buffered: &VecDeque<Bytes>, max_len: usize, out: &mut Vec<u8>) {
    for chunk in buffered {
        if out.len() >= max_len {
            break;
        }
        let remaining = max_len - out.len();
        out.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
    }
}

fn skip_prefix(mut buffered: VecDeque<Bytes>, mut skip_len: usize) -> VecDeque<Bytes> {
    let mut stripped = VecDeque::new();
    while let Some(chunk) = buffered.pop_front() {
        if skip_len >= chunk.len() {
            skip_len -= chunk.len();
            continue;
        }
        stripped.push_back(chunk.slice(skip_len..));
        stripped.extend(buffered);
        break;
    }
    stripped
}

#[cfg(test)]
mod tests {
    use futures_util::StreamExt;

    use super::*;

    #[tokio::test]
    async fn preserves_dataset_chunks() {
        let first = Bytes::from_static(&[0x01; 64]);
        let second = Bytes::from_static(&[0x02; 80]);
        let mut body = ByteStream::from_chunks([first.clone(), second.clone()]);

        let buffered = prepare_dimse_dataset(&mut body)
            .await
            .expect("dataset prefix should pass");

        assert_eq!(
            buffered.into_iter().collect::<Vec<_>>(),
            vec![first, second]
        );
    }

    #[tokio::test]
    async fn strips_part_10_preamble_and_file_meta() {
        let dataset = Bytes::from_static(b"DATASET");
        let mut file = part_10_file(dataset.clone());
        let first = Bytes::copy_from_slice(&file.drain(..70).collect::<Vec<_>>());
        let second = Bytes::copy_from_slice(&file.drain(..80).collect::<Vec<_>>());
        let third = Bytes::from(file);
        let mut body = ByteStream::from_chunks([first, second, third]);

        let mut buffered = prepare_dimse_dataset(&mut body)
            .await
            .expect("Part 10 file should be stripped");
        let mut delivered = Vec::new();
        while let Some(chunk) = buffered.pop_front() {
            delivered.extend_from_slice(&chunk);
        }
        while let Some(chunk) = body.next().await {
            delivered.extend_from_slice(&chunk.expect("body chunk"));
        }

        assert_eq!(delivered, dataset);
    }

    fn part_10_file(dataset: Bytes) -> Vec<u8> {
        let mut file = vec![0; DICOM_FILE_PREFIX_OFFSET];
        file.extend_from_slice(b"DICM");
        file.extend_from_slice(&[0x02, 0x00, 0x00, 0x00]);
        file.extend_from_slice(b"UL");
        file.extend_from_slice(&4_u16.to_le_bytes());
        file.extend_from_slice(&12_u32.to_le_bytes());
        file.extend_from_slice(&[0x02, 0x00, 0x10, 0x00]);
        file.extend_from_slice(b"UI");
        file.extend_from_slice(&4_u16.to_le_bytes());
        file.extend_from_slice(b"1.2\0");
        file.extend_from_slice(&dataset);
        file
    }
}
