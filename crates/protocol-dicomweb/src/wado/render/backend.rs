use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::process::Command;

use async_trait::async_trait;
use dicom_core::Tag;
use dicom_dictionary_std::{tags, uids};
use dicom_object::{
    DefaultDicomObject, FileMetaTableBuilder, InMemDicomObject, collector::DicomCollector,
};
use jpeg_encoder::{ColorType, Encoder as JpegEncoder};
use raccoon_contract_object_store::Bytes;
use tempfile::NamedTempFile;

use super::{RenderInput, RenderedImage};
use crate::media;

#[async_trait]
pub trait RendererBackend: Send + Sync {
    fn name(&self) -> &'static str;

    async fn render(&self, input: &RenderInput) -> Result<RenderedImage, BackendRenderError>;
}

#[derive(Debug, thiserror::Error)]
pub enum BackendRenderError {
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("failed: {0}")]
    Failed(String),
}

pub struct NativeRenderer;

#[async_trait]
impl RendererBackend for NativeRenderer {
    fn name(&self) -> &'static str {
        "native"
    }

    async fn render(&self, input: &RenderInput) -> Result<RenderedImage, BackendRenderError> {
        let input = input.clone();
        tokio::task::spawn_blocking(move || render_native(input))
            .await
            .map_err(|error| BackendRenderError::Failed(format!("render task failed: {error}")))?
    }
}

pub struct DcmtkRenderer {
    pub(crate) path: PathBuf,
}

#[async_trait]
impl RendererBackend for DcmtkRenderer {
    fn name(&self) -> &'static str {
        "dcmtk"
    }

    async fn render(&self, input: &RenderInput) -> Result<RenderedImage, BackendRenderError> {
        if input.media_type != media::IMAGE_JPEG {
            return Err(BackendRenderError::Unsupported(
                "dcmj2pnm fallback supports JPEG output only".to_string(),
            ));
        }
        let executable = self.path.clone();
        let dicom = input.dicom.clone();
        tokio::task::spawn_blocking(move || render_dcmtk(&executable, &dicom))
            .await
            .map_err(|error| BackendRenderError::Failed(format!("dcmtk task failed: {error}")))?
    }
}

#[derive(Debug, Clone)]
struct RgbImage {
    width: u32,
    height: u32,
    pixels: Vec<u8>,
}

fn render_native(input: RenderInput) -> Result<RenderedImage, BackendRenderError> {
    if input.media_type != media::IMAGE_JPEG && input.media_type != media::IMAGE_PNG {
        return Err(BackendRenderError::Unsupported(format!(
            "native renderer supports only {} and {}",
            media::IMAGE_JPEG,
            media::IMAGE_PNG
        )));
    }

    let object = parse_object(&input)?;
    let mut image = image_from_object(&object, requested_frame(&input)?)?;
    if let Some(window) = input.params.window.as_deref() {
        image = apply_window(image, window)?;
    }
    if let Some(viewport) = input.params.viewport.as_deref() {
        image = apply_viewport(image, viewport)?;
    } else if input.thumbnail {
        image = constrain_thumbnail(image);
    }

    let bytes = match input.media_type.as_str() {
        media::IMAGE_JPEG => encode_jpeg(&image, input.params.quality)?,
        media::IMAGE_PNG => encode_png(&image),
        _ => unreachable!("media type checked above"),
    };
    Ok(RenderedImage {
        media_type: input.media_type,
        bytes: Bytes::from(bytes),
    })
}

fn render_dcmtk(executable: &Path, dicom: &Bytes) -> Result<RenderedImage, BackendRenderError> {
    let input = NamedTempFile::new()
        .map_err(|error| BackendRenderError::Failed(format!("temp input failed: {error}")))?;
    std::fs::write(input.path(), dicom)
        .map_err(|error| BackendRenderError::Failed(format!("temp input write failed: {error}")))?;
    let output = NamedTempFile::new()
        .map_err(|error| BackendRenderError::Failed(format!("temp output failed: {error}")))?;
    let status = Command::new(executable)
        .arg("+oj")
        .arg(input.path())
        .arg(output.path())
        .status()
        .map_err(|error| BackendRenderError::Failed(format!("dcmtk execution failed: {error}")))?;
    if !status.success() {
        return Err(BackendRenderError::Failed(format!(
            "dcmtk exited with status {status}"
        )));
    }
    let bytes = std::fs::read(output.path()).map_err(|error| {
        BackendRenderError::Failed(format!("dcmtk output read failed: {error}"))
    })?;
    Ok(RenderedImage {
        media_type: media::IMAGE_JPEG.to_string(),
        bytes: Bytes::from(bytes),
    })
}

fn parse_object(input: &RenderInput) -> Result<DefaultDicomObject, BackendRenderError> {
    match dicom_object::from_reader(Cursor::new(input.dicom.clone())) {
        Ok(object) => Ok(object),
        Err(file_error) => {
            let transfer_syntax = input
                .transfer_syntax_uid
                .as_ref()
                .map(|uid| uid.as_str())
                .unwrap_or(uids::EXPLICIT_VR_LITTLE_ENDIAN);
            let mut collector = DicomCollector::new_with_ts(
                std::io::BufReader::new(Cursor::new(input.dicom.clone())),
                transfer_syntax.to_string(),
            );
            let mut object = InMemDicomObject::new_empty();
            collector.read_dataset_to_end(&mut object).map_err(|error| {
                BackendRenderError::Failed(format!(
                    "DICOM parse failed: Part 10 parse failed: {file_error}; dataset parse failed: {error}"
                ))
            })?;
            object
                .with_meta(FileMetaTableBuilder::new().transfer_syntax(transfer_syntax))
                .map_err(|error| BackendRenderError::Failed(format!("DICOM meta failed: {error}")))
        }
    }
}

fn image_from_object(
    object: &DefaultDicomObject,
    frame: usize,
) -> Result<RgbImage, BackendRenderError> {
    let rows = required_u32(object, tags::ROWS)?;
    let columns = required_u32(object, tags::COLUMNS)?;
    let samples = required_usize(object, tags::SAMPLES_PER_PIXEL)?;
    let bits_allocated = required_usize(object, tags::BITS_ALLOCATED)?;
    let photometric = required_str(object, tags::PHOTOMETRIC_INTERPRETATION)?;
    if bits_allocated != 8 {
        return Err(BackendRenderError::Unsupported(
            "native renderer supports only 8-bit pixel data".to_string(),
        ));
    }
    if samples != 1 && samples != 3 {
        return Err(BackendRenderError::Unsupported(
            "native renderer supports only monochrome or RGB pixel data".to_string(),
        ));
    }

    let bytes = object
        .element(tags::PIXEL_DATA)
        .map_err(|_| BackendRenderError::Unsupported("Pixel Data is missing".to_string()))?
        .value()
        .to_bytes()
        .map_err(|_| {
            BackendRenderError::Unsupported("encapsulated Pixel Data is not supported".to_string())
        })?
        .into_owned();
    let frame_size = rows as usize * columns as usize * samples;
    let start = frame
        .checked_sub(1)
        .and_then(|frame| frame.checked_mul(frame_size))
        .ok_or_else(|| BackendRenderError::Unsupported("invalid frame number".to_string()))?;
    let end = start + frame_size;
    if end > bytes.len() {
        return Err(BackendRenderError::Unsupported(format!(
            "frame {frame} is missing"
        )));
    }
    let frame_bytes = &bytes[start..end];
    let pixels = match (samples, photometric.as_str()) {
        (1, "MONOCHROME2") => frame_bytes.iter().flat_map(|value| [*value; 3]).collect(),
        (1, "MONOCHROME1") => frame_bytes
            .iter()
            .flat_map(|value| {
                let value = 255_u8.saturating_sub(*value);
                [value; 3]
            })
            .collect(),
        (3, "RGB") => frame_bytes.to_vec(),
        _ => {
            return Err(BackendRenderError::Unsupported(format!(
                "photometric interpretation {photometric} is not supported"
            )));
        }
    };
    Ok(RgbImage {
        width: columns,
        height: rows,
        pixels,
    })
}

fn requested_frame(input: &RenderInput) -> Result<usize, BackendRenderError> {
    match input.frames.as_deref() {
        Some([frame]) => Ok(*frame as usize),
        Some([]) => Ok(1),
        Some(_) => Err(BackendRenderError::Unsupported(
            "native renderer renders one frame per image".to_string(),
        )),
        None => Ok(1),
    }
}

fn apply_window(mut image: RgbImage, window: &str) -> Result<RgbImage, BackendRenderError> {
    let (center, width) = parse_window(window)?;
    let low = center - width / 2.0;
    for value in &mut image.pixels {
        let normalized = ((*value as f64 - low) / width * 255.0).clamp(0.0, 255.0);
        *value = normalized.round() as u8;
    }
    Ok(image)
}

fn apply_viewport(image: RgbImage, viewport: &str) -> Result<RgbImage, BackendRenderError> {
    let values = viewport
        .split(',')
        .map(|part| {
            part.parse::<f64>()
                .map_err(|_| BackendRenderError::Unsupported("invalid viewport".to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if !matches!(values.len(), 2 | 6) {
        return Err(BackendRenderError::Unsupported(
            "invalid viewport".to_string(),
        ));
    }
    let output_width = positive_u32(values[0], "viewport width")?;
    let output_height = positive_u32(values[1], "viewport height")?;
    let source = if values.len() == 6 {
        let source_x = non_negative_u32(values[2], "viewport source x")?;
        let source_y = non_negative_u32(values[3], "viewport source y")?;
        let source_width = positive_u32(values[4], "viewport source width")?;
        let source_height = positive_u32(values[5], "viewport source height")?;
        crop(image, source_x, source_y, source_width, source_height)?
    } else {
        image
    };
    Ok(resize_nearest(source, output_width, output_height))
}

fn constrain_thumbnail(image: RgbImage) -> RgbImage {
    let max_side = image.width.max(image.height);
    if max_side <= 128 {
        return image;
    }
    let scale = 128.0 / max_side as f64;
    let width = ((image.width as f64 * scale).round() as u32).max(1);
    let height = ((image.height as f64 * scale).round() as u32).max(1);
    resize_nearest(image, width, height)
}

fn crop(
    image: RgbImage,
    source_x: u32,
    source_y: u32,
    source_width: u32,
    source_height: u32,
) -> Result<RgbImage, BackendRenderError> {
    if source_x >= image.width || source_y >= image.height {
        return Err(BackendRenderError::Unsupported(
            "viewport source origin is outside the image".to_string(),
        ));
    }
    let width = source_width.min(image.width - source_x);
    let height = source_height.min(image.height - source_y);
    let mut pixels = Vec::with_capacity(width as usize * height as usize * 3);
    for y in source_y..source_y + height {
        let start = ((y * image.width + source_x) * 3) as usize;
        let end = start + width as usize * 3;
        pixels.extend_from_slice(&image.pixels[start..end]);
    }
    Ok(RgbImage {
        width,
        height,
        pixels,
    })
}

fn resize_nearest(image: RgbImage, output_width: u32, output_height: u32) -> RgbImage {
    let mut pixels = Vec::with_capacity(output_width as usize * output_height as usize * 3);
    for y in 0..output_height {
        let source_y = y as usize * image.height as usize / output_height as usize;
        for x in 0..output_width {
            let source_x = x as usize * image.width as usize / output_width as usize;
            let index = (source_y * image.width as usize + source_x) * 3;
            pixels.extend_from_slice(&image.pixels[index..index + 3]);
        }
    }
    RgbImage {
        width: output_width,
        height: output_height,
        pixels,
    }
}

fn parse_window(window: &str) -> Result<(f64, f64), BackendRenderError> {
    let parts = window.split(',').collect::<Vec<_>>();
    if !matches!(parts.len(), 2 | 3) {
        return Err(BackendRenderError::Unsupported(
            "window requires center,width".to_string(),
        ));
    }
    let center = parts[0]
        .parse::<f64>()
        .map_err(|_| BackendRenderError::Unsupported("invalid window center".to_string()))?;
    let width = parts[1]
        .parse::<f64>()
        .map_err(|_| BackendRenderError::Unsupported("invalid window width".to_string()))?;
    if width <= 0.0 {
        return Err(BackendRenderError::Unsupported(
            "window width must be positive".to_string(),
        ));
    }
    match parts.get(2).copied().unwrap_or("linear") {
        "linear" | "linear-exact" | "sigmoid" | "LINEAR" | "LINEAR_EXACT" | "SIGMOID" => {}
        value => {
            return Err(BackendRenderError::Unsupported(format!(
                "unsupported window function {value}"
            )));
        }
    }
    Ok((center, width))
}

fn positive_u32(value: f64, name: &str) -> Result<u32, BackendRenderError> {
    if value <= 0.0 {
        return Err(BackendRenderError::Unsupported(format!(
            "{name} must be positive"
        )));
    }
    Ok(value.round().max(1.0).min(u32::MAX as f64) as u32)
}

fn non_negative_u32(value: f64, name: &str) -> Result<u32, BackendRenderError> {
    if value < 0.0 {
        return Err(BackendRenderError::Unsupported(format!(
            "{name} must be non-negative"
        )));
    }
    Ok(value.round().min(u32::MAX as f64) as u32)
}

fn encode_jpeg(image: &RgbImage, quality: Option<u8>) -> Result<Vec<u8>, BackendRenderError> {
    let mut bytes = Vec::new();
    JpegEncoder::new(&mut bytes, quality.unwrap_or(90))
        .encode(
            &image.pixels,
            image.width as u16,
            image.height as u16,
            ColorType::Rgb,
        )
        .map_err(|error| BackendRenderError::Failed(format!("JPEG encoding failed: {error}")))?;
    Ok(bytes)
}

fn encode_png(image: &RgbImage) -> Vec<u8> {
    let mut raw = Vec::with_capacity((image.width as usize * 3 + 1) * image.height as usize);
    for row in image.pixels.chunks(image.width as usize * 3) {
        raw.push(0);
        raw.extend_from_slice(row);
    }

    let mut png = Vec::new();
    png.extend_from_slice(b"\x89PNG\r\n\x1a\n");
    let mut ihdr = Vec::new();
    ihdr.extend_from_slice(&image.width.to_be_bytes());
    ihdr.extend_from_slice(&image.height.to_be_bytes());
    ihdr.extend_from_slice(&[8, 2, 0, 0, 0]);
    png_chunk(&mut png, b"IHDR", &ihdr);
    png_chunk(&mut png, b"IDAT", &zlib_stored(&raw));
    png_chunk(&mut png, b"IEND", &[]);
    png
}

fn zlib_stored(raw: &[u8]) -> Vec<u8> {
    let mut output = vec![0x78, 0x01];
    let mut remaining = raw;
    while !remaining.is_empty() {
        let chunk_len = remaining.len().min(u16::MAX as usize);
        let final_block = usize::from(chunk_len == remaining.len());
        output.push(final_block as u8);
        output.extend_from_slice(&(chunk_len as u16).to_le_bytes());
        output.extend_from_slice(&(!(chunk_len as u16)).to_le_bytes());
        output.extend_from_slice(&remaining[..chunk_len]);
        remaining = &remaining[chunk_len..];
    }
    output.extend_from_slice(&adler32(raw).to_be_bytes());
    output
}

fn png_chunk(output: &mut Vec<u8>, chunk_type: &[u8; 4], data: &[u8]) {
    output.extend_from_slice(&(data.len() as u32).to_be_bytes());
    output.extend_from_slice(chunk_type);
    output.extend_from_slice(data);
    let mut crc_input = Vec::with_capacity(chunk_type.len() + data.len());
    crc_input.extend_from_slice(chunk_type);
    crc_input.extend_from_slice(data);
    output.extend_from_slice(&crc32(&crc_input).to_be_bytes());
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = 0xffff_ffff_u32;
    for byte in bytes {
        crc ^= *byte as u32;
        for _ in 0..8 {
            let mask = 0_u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0xedb8_8320 & mask);
        }
    }
    !crc
}

fn adler32(bytes: &[u8]) -> u32 {
    const MOD: u32 = 65_521;
    let mut a = 1_u32;
    let mut b = 0_u32;
    for byte in bytes {
        a = (a + *byte as u32) % MOD;
        b = (b + a) % MOD;
    }
    (b << 16) | a
}

fn required_u32(object: &DefaultDicomObject, tag: Tag) -> Result<u32, BackendRenderError> {
    object
        .element(tag)
        .map_err(|_| BackendRenderError::Unsupported(format!("missing image tag {tag}")))?
        .value()
        .to_int::<u32>()
        .map_err(|_| BackendRenderError::Unsupported(format!("invalid image tag {tag}")))
}

fn required_usize(object: &DefaultDicomObject, tag: Tag) -> Result<usize, BackendRenderError> {
    object
        .element(tag)
        .map_err(|_| BackendRenderError::Unsupported(format!("missing image tag {tag}")))?
        .value()
        .to_int::<usize>()
        .map_err(|_| BackendRenderError::Unsupported(format!("invalid image tag {tag}")))
}

fn required_str(object: &DefaultDicomObject, tag: Tag) -> Result<String, BackendRenderError> {
    object
        .element(tag)
        .map_err(|_| BackendRenderError::Unsupported(format!("missing image tag {tag}")))?
        .value()
        .to_str()
        .map(|value| value.trim().to_string())
        .map_err(|_| BackendRenderError::Unsupported(format!("invalid image tag {tag}")))
}
