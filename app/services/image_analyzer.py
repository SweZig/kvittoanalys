"""Image analysis service — uses Claude Vision to interpret images and extract text."""

import base64
import io

import anthropic
import httpx
from PIL import Image

from app.config import settings

_MAX_IMAGE_DIM = 1568
_API_TIMEOUT = 120


class ImageAnalyzer:
    """Analyze images using Claude's vision capabilities."""

    def __init__(self):
        self.client = anthropic.Anthropic(
            api_key=settings.anthropic_api_key,
            timeout=httpx.Timeout(_API_TIMEOUT, connect=10),
        )
        self.model = settings.claude_model

    def analyze(
        self,
        content_blocks: list[dict],
        prompt: str | None = None,
        language: str = "swedish",
    ) -> dict:
        """
        Analyze content blocks (images/text) with Claude Vision.

        Args:
            content_blocks: List of dicts from DocumentLoader.load_file()
            prompt: Custom prompt for the analysis (optional)
            language: Response language (default: Swedish)

        Returns:
            Dict with analysis results per page and a combined summary.
        """
        if not prompt:
            prompt = self._default_prompt(language)

        messages_content = self._build_message_content(content_blocks, prompt)

        response = self.client.messages.create(
            model=self.model,
            max_tokens=settings.claude_max_tokens,
            messages=[{"role": "user", "content": messages_content}],
        )

        result_text = response.content[0].text

        return {
            "analysis": result_text,
            "pages_analyzed": len(
                [b for b in content_blocks if b["type"] == "image"]
            ),
            "model": self.model,
            "input_tokens": response.usage.input_tokens,
            "output_tokens": response.usage.output_tokens,
        }

    def extract_text(self, content_blocks: list[dict]) -> dict:
        """Extract all text (OCR) from image content blocks."""
        prompt = (
            "Extract ALL text visible in the image(s). "
            "Preserve the original layout and structure as much as possible. "
            "If there are tables, format them clearly. "
            "If text is in multiple languages, note the language for each section. "
            "Return ONLY the extracted text, no commentary."
        )
        return self.analyze(content_blocks, prompt=prompt)

    def describe_image(self, content_blocks: list[dict], language: str = "swedish") -> dict:
        """Describe what is visible in the image(s)."""
        prompt = (
            f"Describe in detail what you see in the image(s). Respond in {language}. "
            "Include: objects, people, text, colors, layout, and any notable details. "
            "If there are multiple pages/images, describe each one separately."
        )
        return self.analyze(content_blocks, prompt=prompt)

    def custom_query(
        self, content_blocks: list[dict], query: str, language: str = "swedish"
    ) -> dict:
        """Ask a custom question about the image/document content."""
        prompt = f"Respond in {language}.\n\n{query}"
        return self.analyze(content_blocks, prompt=prompt)

    # ── Private helpers ──────────────────────────────────────────────

    def _build_message_content(
        self, content_blocks: list[dict], prompt: str
    ) -> list[dict]:
        """Build the Claude API message content array."""
        message_content = []

        for block in content_blocks:
            if block["type"] == "image":
                img_data, img_type = self._resize_image(
                    block["data"], block["media_type"]
                )
                message_content.append(
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": img_type,
                            "data": img_data,
                        },
                    }
                )
            elif block["type"] == "text":
                message_content.append(
                    {
                        "type": "text",
                        "text": f"[Document text from {block['source']}]:\n{block['data']}",
                    }
                )

        # Add the analysis prompt last
        message_content.append({"type": "text", "text": prompt})

        return message_content

    @staticmethod
    def _resize_image(b64_data: str, media_type: str) -> tuple[str, str]:
        """Resize large images to reduce API payload."""
        raw = base64.standard_b64decode(b64_data)
        img = Image.open(io.BytesIO(raw))
        w, h = img.size

        if w <= _MAX_IMAGE_DIM and h <= _MAX_IMAGE_DIM:
            return b64_data, media_type

        ratio = min(_MAX_IMAGE_DIM / w, _MAX_IMAGE_DIM / h)
        new_w, new_h = int(w * ratio), int(h * ratio)
        img = img.resize((new_w, new_h), Image.LANCZOS)

        buf = io.BytesIO()
        if media_type in ("image/jpeg", "image/jpg"):
            img.convert("RGB").save(buf, format="JPEG", quality=85)
            out_type = "image/jpeg"
        else:
            img.save(buf, format="PNG", optimize=True)
            out_type = "image/png"

        return base64.standard_b64encode(buf.getvalue()).decode("utf-8"), out_type

    def _default_prompt(self, language: str) -> str:
        return (
            f"Analyze the provided image(s)/document(s). Respond in {language}. "
            "Do the following:\n"
            "1. **Text extraction**: Extract all visible text, preserving structure.\n"
            "2. **Image description**: Describe what you see — objects, layout, colors, people.\n"
            "3. **Document type**: Identify the type of document (invoice, receipt, letter, photo, etc.).\n"
            "4. **Key information**: Highlight the most important information found.\n"
            "5. **Summary**: Provide a brief summary of the content."
        )
