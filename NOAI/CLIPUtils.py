import io
import asyncio
from PIL import Image
from typing import Optional
import torch
from transformers import CLIPProcessor, CLIPModel

# Module-level globals (initialized lazily in the executor so we don't block the event loop)
_CLIP_MODEL: Optional[CLIPModel] = None
_CLIP_PROCESSOR: Optional[CLIPProcessor] = None
_CLIP_DEVICE: str = "cpu"

def _load_clip_model():
    global _CLIP_MODEL, _CLIP_PROCESSOR, _CLIP_DEVICE
    if _CLIP_MODEL is not None and _CLIP_PROCESSOR is not None:
        return

    # Force CPU device (change if you have GPU support available and want to use it)
    _CLIP_DEVICE = "cpu"

    # Load model + processor (blocking)
    _CLIP_MODEL = CLIPModel.from_pretrained("openai/clip-vit-base-patch32").to(_CLIP_DEVICE)
    _CLIP_PROCESSOR = CLIPProcessor.from_pretrained("openai/clip-vit-base-patch32")

def _sync_score_image_bytes(image_bytes: bytes) -> float:
    global _CLIP_MODEL, _CLIP_PROCESSOR, _CLIP_DEVICE
    if _CLIP_MODEL is None or _CLIP_PROCESSOR is None:
        _load_clip_model()

    # Open image from bytes and ensure RGB
    img = None
    try:
        img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    except Exception as e:
        raise RuntimeError(f"Failed to open image: {e}")

    try:
        prompts = [
            "a real photograph taken by a human",
            "an AI generated image",
            "a digitally generated artwork",
            "a natural unedited photo"
        ]

        # Prepare inputs and move to device
        inputs = _CLIP_PROCESSOR(text=prompts, images=img, return_tensors="pt", padding=True)
        inputs = {k: v.to(_CLIP_DEVICE) for k, v in inputs.items()}

        # Inference (no grad)
        with torch.no_grad():
            outputs = _CLIP_MODEL(**inputs)

        # Softmax over prompts for the single image and combine AI-like prompts
        try:
            probs = outputs.logits_per_image.softmax(dim=1)[0]
            ai_score = (probs[1] + probs[2]).item() * 100.0
        except Exception as e:
            raise RuntimeError(f"Model output parsing failed: {e}")

        return round(ai_score, 2)
    finally:
        # Explicitly close the PIL Image to free memory
        if img is not None:
            img.close()


async def analize_image(self, data: bytes, filename: str, url: str, ctx) -> float:
    loop = asyncio.get_running_loop()

    try:
        # Run the heavy synchronous work in a threadpool
        ai_score = await loop.run_in_executor(None, _sync_score_image_bytes, data)
        return ai_score
    except Exception as e:
        # Return a readable error so the calling command can forward it to the user
        return -1

def certainty_string_generator(certainty_score) -> str:
    if (certainty_score > 80):
        return "Very likely AI generated"
    elif (certainty_score > 60):
        return "Probably AI generated"
    elif (certainty_score > 40):
        return "Uncertain if AI generated"
    elif (certainty_score > 20):
        return "Probably human made or modified"
    else:
        return "Likely human made or modified"

