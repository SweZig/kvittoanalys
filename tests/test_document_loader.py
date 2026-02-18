"""Tests for document loader service."""

import tempfile
from pathlib import Path

from PIL import Image

from app.services.document_loader import DocumentLoader


def test_load_image_png():
    """Test loading a PNG image."""
    loader = DocumentLoader()

    # Create a simple test image
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
        img = Image.new("RGB", (100, 100), color="red")
        img.save(f, format="PNG")
        temp_path = Path(f.name)

    blocks = loader.load_file(temp_path)

    assert len(blocks) == 1
    assert blocks[0]["type"] == "image"
    assert blocks[0]["media_type"] == "image/png"
    assert blocks[0]["page"] == 1
    assert len(blocks[0]["data"]) > 0  # base64 data present

    temp_path.unlink()


def test_load_image_jpg():
    """Test loading a JPEG image."""
    loader = DocumentLoader()

    with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as f:
        img = Image.new("RGB", (100, 100), color="blue")
        img.save(f, format="JPEG")
        temp_path = Path(f.name)

    blocks = loader.load_file(temp_path)

    assert len(blocks) == 1
    assert blocks[0]["media_type"] == "image/jpeg"

    temp_path.unlink()


def test_unsupported_file():
    """Test that unsupported files raise ValueError."""
    loader = DocumentLoader()

    with tempfile.NamedTemporaryFile(suffix=".xyz", delete=False) as f:
        f.write(b"test")
        temp_path = Path(f.name)

    try:
        loader.load_file(temp_path)
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "Unsupported" in str(e)
    finally:
        temp_path.unlink()


if __name__ == "__main__":
    test_load_image_png()
    print("✅ test_load_image_png passed")

    test_load_image_jpg()
    print("✅ test_load_image_jpg passed")

    test_unsupported_file()
    print("✅ test_unsupported_file passed")

    print("\n🎉 All tests passed!")
