import discord
from typing import List, Dict, Optional, Tuple
import re
import aiohttp

IMAGE_URL_RE = re.compile(
    r'(https?://[^\s<>"\']+\.(?:png|jpe?g|gif|webp)(?:\?\S*)?)',
    re.IGNORECASE
)
GENERIC_URL_RE = re.compile(r'(https?://[^\s<>"\']+)')

def _sniff_image_ext(data: bytes) -> Optional[str]:
    """
    Return a trusted image extension based on magic bytes, or None if not an image.
    Supports: png, jpg/jpeg, gif, webp.
    """
    if len(data) < 12:
        return None
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "png"
    if data.startswith(b"\xff\xd8\xff"):
        return "jpg"
    if data.startswith(b"GIF87a") or data.startswith(b"GIF89a"):
        return "gif"
    if data[0:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "webp"
    return None

async def extract_images_from_message(self, msg: discord.Message, maximagesizemb) -> List[Dict]:
    """
    Return a list of images found in message. Each entry:
      {'bytes': bytes, 'filename': str, 'url': str, 'source': 'attachment'|'embed'|'url'}
    """
    images = []
    max_bytes = int(maximagesizemb * 1024 * 1024)

    # 1) Attachments (these are the most reliable)
    for att in msg.attachments:
        try:
            att_size = getattr(att, "size", None)
            if att_size is not None and att_size > max_bytes:
                continue

            data = await att.read()
            if len(data) > max_bytes:
                continue
            # verify by magic bytes to avoid spoofed extensions
            ext = _sniff_image_ext(data)
            if not ext:
                continue
            images.append({
                'bytes': data,
                'filename': att.filename,
                'url': att.url,
                'source': 'attachment',
            })
        except Exception:
            # skip attachments we can't read for any reason
            continue

    # 2) Embeds (image or thumbnail)
    for emb in msg.embeds:
        url = None
        if emb.image and getattr(emb.image, 'url', None):
            url = emb.image.url
        elif emb.thumbnail and getattr(emb.thumbnail, 'url', None):
            url = emb.thumbnail.url
        if url:
            downloaded = await download_image_from_url(self, url, max_bytes)
            if downloaded:
                data, filename = downloaded
                images.append({
                    'bytes': data,
                    'filename': filename,
                    'url': url,
                    'source': 'embed',
                })

    # 3) Image URLs explicitly in content that have common image extensions
    # first, look for explicit image-extension URLs
    for m in IMAGE_URL_RE.finditer(msg.content or ""):
        url = m.group(1)
        # avoid duplicates
        if any(url == i.get('url') for i in images):
            continue
        downloaded = await download_image_from_url(self, url, max_bytes)
        if downloaded:
            data, filename = downloaded
            images.append({
                'bytes': data,
                'filename': filename,
                'url': url,
                'source': 'url',
            })

    # 4) Generic URLs: fetch and check content-type if still nothing
    # (This covers some CDNs lacking file extensions)
    if not images:
        for m in GENERIC_URL_RE.finditer(msg.content or ""):
            url = m.group(1)
            # skip if obviously not an image by extension
            if re.search(r'\.(?:css|js|html?|mp4|webm|mp3)(?:\?|$)', url, re.IGNORECASE):
                continue
            if any(url == i.get('url') for i in images):
                continue
            downloaded = await download_image_from_url(self, url, max_bytes)
            if downloaded:
                data, filename = downloaded
                images.append({
                    'bytes': data,
                    'filename': filename,
                    'url': url,
                    'source': 'url',
                })

    return images


async def download_image_from_url(self, url: str, max_bytes: int) -> Optional[Tuple[bytes, str]]:
    """
    Attempt to download the URL if it's an image and under max_bytes.
    Returns (bytes, filename) on success, or None on failure/skip.

    Strategy:
      - Try HEAD to get Content-Length and Content-Type (skip early if too large / not image)
      - If HEAD is not conclusive, stream GET and abort when accumulated > max_bytes
    """
    # create or reuse session on self if you have one (example uses a session from self._session if present)
    session: aiohttp.ClientSession = None
    session_created = False
    try:
        session = getattr(self, "_session", None)
        if session is None or session.closed:
            session = aiohttp.ClientSession()
            session_created = True

        # Try HEAD first to check content-length / type quickly
        try:
            async with session.head(url, timeout=10) as resp:
                if resp.status == 200:
                    ct = resp.headers.get('Content-Type', '')
                    if not ct.startswith('image/'):
                        return None
                    cl = resp.headers.get('Content-Length')
                    if cl:
                        try:
                            clv = int(cl)
                            if clv > max_bytes:
                                return None
                        except ValueError:
                            pass
        except Exception:
            # some servers block HEAD requests — fall back to GET
            pass

        # Stream GET and abort if size would exceed limit
        try:
            async with session.get(url, timeout=30) as resp:
                if resp.status != 200:
                    return None
                ct = resp.headers.get('Content-Type', '')
                if not ct.startswith('image/'):
                    return None

                total = 0
                chunks = []
                async for chunk in resp.content.iter_chunked(16 * 1024):
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > max_bytes:
                        return None
                    chunks.append(chunk)

                data = b"".join(chunks)
                # verify by magic bytes to avoid spoofed content-type/extension
                ext = _sniff_image_ext(data)
                if not ext:
                    return None

                filename = url.split("/")[-1].split("?")[0] or "image"
                if '.' not in filename:
                    filename = f"{filename}.{ext}"
                return data, filename
        except Exception:
            return None
    finally:
        if session_created and session and not session.closed:
            await session.close()

def value_to_rgb(value: float) -> Tuple[int, int, int]:
    """Return an (r, g, b) tuple for value in [0, 100].

    0 -> (0,255,0) green
    100 -> (255,0,0) red
    Values are clamped to [0,100] and interpolated linearly.
    """
    v = max(0.0, min(100.0, float(value)))
    t = v / 100.0
    r = int(round(255 * t))
    g = int(round(255 * (1 - t)))
    b = 0
    return (r, g, b)


def value_to_hex(value: float) -> int:
    """Return integer color 0xRRGGBB for a float value between 0 and 100.

    Example: 0 -> 0x00FF00 (green), 100 -> 0xFF0000 (red)
    """
    r, g, b = value_to_rgb(value)
    return (r << 16) | (g << 8) | b
