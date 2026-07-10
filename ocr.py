"""
Turns a screenshot of a trading signal into plain text using local OCR
(Tesseract via pytesseract) — free, runs entirely on-device, no API calls
or per-use cost.
"""
import io
import logging
import shutil
 
log = logging.getLogger("ocr")
_TESSERACT_AVAILABLE = shutil.which("tesseract") is not None
 
def is_enabled() -> bool:
    return _TESSERACT_AVAILABLE
 
def extract_text_from_image(image_bytes: bytes) -> str:
    if not _TESSERACT_AVAILABLE:
        raise RuntimeError(
            "Tesseract isn't installed. Run `pkg install tesseract` (Termux) or "
            "`sudo apt install tesseract-ocr` (Linux), then `pip install pytesseract pillow`."
        )
    import pytesseract
    from PIL import Image
 
    image = Image.open(io.BytesIO(image_bytes))
    if image.width < 1000:
        scale = 1000 / image.width
        image = image.resize((int(image.width * scale), int(image.height * scale)))
    image = image.convert("L")
 
    text = pytesseract.image_to_string(image)
    return text.strip()