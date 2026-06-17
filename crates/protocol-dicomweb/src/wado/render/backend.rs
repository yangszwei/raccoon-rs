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
        if input.media_type != media::IMAGE_JPEG && input.media_type != media::IMAGE_PNG {
            return Err(BackendRenderError::Unsupported(
                "dcmtk fallback supports JPEG and PNG output only".to_string(),
            ));
        }
        let input = input.clone();
        let executable = self.path.clone();
        tokio::task::spawn_blocking(move || render_dcmtk(&executable, input))
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

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct Viewport {
    output_width: u32,
    output_height: u32,
    crop: Option<ViewportCrop>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct ViewportCrop {
    x: u32,
    y: u32,
    width: u32,
    height: u32,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum WindowFunction {
    Linear,
    LinearExact,
    Sigmoid,
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

fn render_dcmtk(
    executable: &Path,
    input: RenderInput,
) -> Result<RenderedImage, BackendRenderError> {
    let dicom_file = NamedTempFile::new()
        .map_err(|error| BackendRenderError::Failed(format!("temp input failed: {error}")))?;
    std::fs::write(dicom_file.path(), &input.dicom)
        .map_err(|error| BackendRenderError::Failed(format!("temp input write failed: {error}")))?;
    let output_file = NamedTempFile::new()
        .map_err(|error| BackendRenderError::Failed(format!("temp output failed: {error}")))?;
    let args = dcmtk_render_args(&input)?;
    let mut command = Command::new(executable);
    for arg in args {
        command.arg(arg);
    }
    let status = command
        .arg(dicom_file.path())
        .arg(output_file.path())
        .status()
        .map_err(|error| BackendRenderError::Failed(format!("dcmtk execution failed: {error}")))?;
    if !status.success() {
        return Err(BackendRenderError::Failed(format!(
            "dcmtk exited with status {status}"
        )));
    }
    let bytes = std::fs::read(output_file.path()).map_err(|error| {
        BackendRenderError::Failed(format!("dcmtk output read failed: {error}"))
    })?;
    let mut image = parse_pnm(&bytes)?;
    if let Some(viewport) = input.params.viewport.as_deref() {
        image = apply_viewport(image, viewport)?;
    } else if input.thumbnail {
        image = constrain_thumbnail(image);
    }
    let bytes = match input.media_type.as_str() {
        media::IMAGE_JPEG => encode_jpeg(&image, input.params.quality)?,
        media::IMAGE_PNG => encode_png(&image),
        _ => unreachable!("media type checked before DCMTK rendering"),
    };
    Ok(RenderedImage {
        media_type: input.media_type,
        bytes: Bytes::from(bytes),
    })
}

fn dcmtk_render_args(input: &RenderInput) -> Result<Vec<String>, BackendRenderError> {
    let mut args = vec!["+op".to_string()];

    match input.frames.as_deref() {
        Some([frame]) => {
            args.push("+F".to_string());
            args.push(frame.to_string());
        }
        Some([]) | None => {}
        Some(_) => {
            return Err(BackendRenderError::Unsupported(
                "dcmtk fallback renders one frame per image".to_string(),
            ));
        }
    }

    if let Some(window) = input.params.window.as_deref() {
        let (center, width, function) = parse_window(window)?;
        args.push("+Ww".to_string());
        args.push(center.to_string());
        args.push(width.to_string());
        match function {
            WindowFunction::Linear | WindowFunction::LinearExact => {
                args.push("+Wfl".to_string());
            }
            WindowFunction::Sigmoid => {
                args.push("+Wfs".to_string());
            }
        }
    }
    Ok(args)
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

fn parse_pnm(bytes: &[u8]) -> Result<RgbImage, BackendRenderError> {
    let mut cursor = PnmCursor::new(bytes);
    let magic = cursor.token()?;
    if magic != "P5" && magic != "P6" {
        return Err(BackendRenderError::Failed(format!(
            "unsupported dcmtk PNM format {magic}"
        )));
    }
    let width = cursor.u32_token("width")?;
    let height = cursor.u32_token("height")?;
    let max_value = cursor.u32_token("max value")?;
    if max_value != 255 {
        return Err(BackendRenderError::Failed(format!(
            "unsupported dcmtk PNM max value {max_value}"
        )));
    }
    cursor.skip_ascii_whitespace();
    let samples = if magic == "P6" { 3 } else { 1 };
    let expected_len = width as usize * height as usize * samples;
    let data = cursor.remaining();
    if data.len() < expected_len {
        return Err(BackendRenderError::Failed(
            "truncated dcmtk PNM output".to_string(),
        ));
    }
    let pixels = if samples == 3 {
        data[..expected_len].to_vec()
    } else {
        data[..expected_len]
            .iter()
            .flat_map(|value| [*value; 3])
            .collect()
    };
    Ok(RgbImage {
        width,
        height,
        pixels,
    })
}

struct PnmCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> PnmCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn token(&mut self) -> Result<String, BackendRenderError> {
        self.skip_ascii_whitespace_and_comments();
        let start = self.offset;
        while self
            .bytes
            .get(self.offset)
            .is_some_and(|byte| !byte.is_ascii_whitespace())
        {
            self.offset += 1;
        }
        if self.offset == start {
            return Err(BackendRenderError::Failed(
                "invalid dcmtk PNM header".to_string(),
            ));
        }
        std::str::from_utf8(&self.bytes[start..self.offset])
            .map(str::to_string)
            .map_err(|error| {
                BackendRenderError::Failed(format!("invalid dcmtk PNM token: {error}"))
            })
    }

    fn u32_token(&mut self, name: &str) -> Result<u32, BackendRenderError> {
        self.token()?
            .parse::<u32>()
            .map_err(|_| BackendRenderError::Failed(format!("invalid dcmtk PNM {name}")))
    }

    fn skip_ascii_whitespace_and_comments(&mut self) {
        loop {
            self.skip_ascii_whitespace();
            if self.bytes.get(self.offset) != Some(&b'#') {
                return;
            }
            while self
                .bytes
                .get(self.offset)
                .is_some_and(|byte| *byte != b'\n')
            {
                self.offset += 1;
            }
        }
    }

    fn skip_ascii_whitespace(&mut self) {
        while self
            .bytes
            .get(self.offset)
            .is_some_and(|byte| byte.is_ascii_whitespace())
        {
            self.offset += 1;
        }
    }

    fn remaining(&self) -> &'a [u8] {
        &self.bytes[self.offset..]
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
    let (center, width, _) = parse_window(window)?;
    let low = center - width / 2.0;
    for value in &mut image.pixels {
        let normalized = ((*value as f64 - low) / width * 255.0).clamp(0.0, 255.0);
        *value = normalized.round() as u8;
    }
    Ok(image)
}

fn apply_viewport(image: RgbImage, viewport: &str) -> Result<RgbImage, BackendRenderError> {
    let viewport = parse_viewport(viewport)?;
    let source = if let Some(crop) = viewport.crop {
        crop_image(image, crop.x, crop.y, crop.width, crop.height)?
    } else {
        image
    };
    Ok(resize_nearest(
        source,
        viewport.output_width,
        viewport.output_height,
    ))
}

fn parse_viewport(viewport: &str) -> Result<Viewport, BackendRenderError> {
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
    let crop = if values.len() == 6 {
        let source_x = non_negative_u32(values[2], "viewport source x")?;
        let source_y = non_negative_u32(values[3], "viewport source y")?;
        let source_width = positive_u32(values[4], "viewport source width")?;
        let source_height = positive_u32(values[5], "viewport source height")?;
        Some(ViewportCrop {
            x: source_x,
            y: source_y,
            width: source_width,
            height: source_height,
        })
    } else {
        None
    };
    Ok(Viewport {
        output_width,
        output_height,
        crop,
    })
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

fn crop_image(
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

fn parse_window(window: &str) -> Result<(f64, f64, WindowFunction), BackendRenderError> {
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
    let function = match parts.get(2).copied().unwrap_or("linear") {
        "linear" | "LINEAR" => WindowFunction::Linear,
        "linear-exact" | "LINEAR_EXACT" => WindowFunction::LinearExact,
        "sigmoid" | "SIGMOID" => WindowFunction::Sigmoid,
        value => {
            return Err(BackendRenderError::Unsupported(format!(
                "unsupported window function {value}"
            )));
        }
    };
    Ok((center, width, function))
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

#[cfg(test)]
mod tests {
    use raccoon_contract_dicom::{
        DicomInstanceIdentity, SeriesInstanceUid, SopClassUid, SopInstanceUid, StudyInstanceUid,
    };

    use super::*;
    use crate::wado::render::RenderParams;

    #[test]
    fn dcmtk_render_args_include_frame_and_window_controls() {
        let mut input = render_input();
        input.frames = Some(vec![2]);
        input.params.window = Some("128,256,sigmoid".to_string());

        assert_eq!(
            dcmtk_render_args(&input).expect("valid args"),
            ["+op", "+F", "2", "+Ww", "128", "256", "+Wfs"]
        );
    }

    #[test]
    fn dcmtk_render_args_omit_post_processed_controls() {
        let mut input = render_input();
        input.params.viewport = Some("320,240,10,20,100,80".to_string());
        input.params.quality = Some(100);
        input.thumbnail = true;

        assert_eq!(dcmtk_render_args(&input).expect("valid args"), ["+op"]);
    }

    #[test]
    fn dcmtk_render_args_omit_unspecified_controls() {
        assert_eq!(
            dcmtk_render_args(&render_input()).expect("valid args"),
            ["+op"]
        );
    }

    #[test]
    fn parse_pnm_decodes_grayscale_as_rgb() {
        let image = parse_pnm(b"P5\n2 1\n255\n\x00\xff").expect("valid PGM");

        assert_eq!(image.width, 2);
        assert_eq!(image.height, 1);
        assert_eq!(image.pixels, [0, 0, 0, 255, 255, 255]);
    }

    #[test]
    fn parse_pnm_decodes_rgb() {
        let image = parse_pnm(b"P6\n# comment\n1 1\n255\n\x01\x02\x03").expect("valid PPM");

        assert_eq!(image.width, 1);
        assert_eq!(image.height, 1);
        assert_eq!(image.pixels, [1, 2, 3]);
    }

    #[test]
    fn encode_jpeg_uses_quality() {
        let image = RgbImage {
            width: 16,
            height: 16,
            pixels: (0..16)
                .flat_map(|y| {
                    (0..16).flat_map(move |x| {
                        let red = x * 16;
                        let green = y * 16;
                        [red, green, red ^ green]
                    })
                })
                .collect(),
        };

        let low_quality = encode_jpeg(&image, Some(1)).expect("low quality JPEG");
        let high_quality = encode_jpeg(&image, Some(100)).expect("high quality JPEG");

        assert_ne!(low_quality, high_quality);
        assert_ne!(low_quality.len(), high_quality.len());
    }

    fn render_input() -> RenderInput {
        RenderInput {
            identity: DicomInstanceIdentity::new(
                StudyInstanceUid::new("1.2.3").expect("valid UID"),
                SeriesInstanceUid::new("1.2.3.4").expect("valid UID"),
                SopInstanceUid::new("1.2.3.4.5").expect("valid UID"),
                SopClassUid::new("1.2.840.10008.5.1.4.1.1.2").expect("valid UID"),
            ),
            transfer_syntax_uid: None,
            dicom: Bytes::new(),
            frames: None,
            media_type: media::IMAGE_JPEG.to_string(),
            params: RenderParams::default(),
            thumbnail: false,
        }
    }
}
