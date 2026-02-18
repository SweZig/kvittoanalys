# 🔍 DocVision

AI-powered document and image analysis API. Upload images or documents and get intelligent text extraction, image descriptions, and content analysis powered by Claude Vision.

## Features

- **OCR / Text Extraction** — Extract all text from images, PDFs, and documents
- **Image Description** — Get detailed descriptions of what's in an image
- **Document Analysis** — Identify document types, key information, and summaries
- **Custom Queries** — Ask any question about your uploaded content
- **Multi-page PDF** — Analyze entire PDF documents, page by page
- **Swedish & English** — Responses in your preferred language

## Supported File Types

| Type | Extensions |
|------|-----------|
| Images | `.png` `.jpg` `.jpeg` `.gif` `.webp` `.bmp` `.tiff` |
| Documents | `.pdf` `.docx` `.doc` |

## Quick Start

### 1. Clone and setup

```bash
cd doc-vision-app
cp .env.example .env
```

### 2. Add your API key

Edit `.env` and set your Anthropic API key:

```
ANTHROPIC_API_KEY=sk-ant-...
```

Get a key at [console.anthropic.com](https://console.anthropic.com/)

### 3. Run

```bash
chmod +x run.sh
./run.sh
```

Or manually:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

### 4. Open the API docs

Visit [http://localhost:8000/docs](http://localhost:8000/docs) for interactive Swagger UI.

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/analyze` | Full analysis (text + description + summary) |
| `POST` | `/api/v1/extract-text` | OCR — extract text only |
| `POST` | `/api/v1/describe` | Describe image content |
| `POST` | `/api/v1/query` | Ask a custom question |
| `GET` | `/health` | Health check |

### Example: Analyze an image

```bash
curl -X POST http://localhost:8000/api/v1/analyze \
  -F "file=@receipt.jpg" \
  -F "language=swedish"
```

### Example: Extract text from a PDF

```bash
curl -X POST http://localhost:8000/api/v1/extract-text \
  -F "file=@document.pdf"
```

### Example: Ask a question

```bash
curl -X POST http://localhost:8000/api/v1/query \
  -F "file=@invoice.png" \
  -F "query=Vad är totalbeloppet på fakturan?" \
  -F "language=swedish"
```

## CLI Usage

For quick analysis from the terminal:

```bash
# Full analysis
python cli.py image.png

# OCR only
python cli.py document.pdf --mode ocr

# Describe an image
python cli.py photo.jpg --mode describe

# Ask a question
python cli.py invoice.png --mode query -q "Vad är förfallodatumet?"

# JSON output
python cli.py receipt.jpg --json
```

## Project Structure

```
doc-vision-app/
├── app/
│   ├── main.py                 # FastAPI application
│   ├── config.py               # Settings & environment
│   ├── api/
│   │   └── routes.py           # API endpoints
│   └── services/
│       ├── document_loader.py  # File loading & conversion
│       └── image_analyzer.py   # Claude Vision integration
├── tests/
│   └── test_document_loader.py
├── cli.py                      # Command-line tool
├── run.sh                      # Quick start script
├── requirements.txt
├── .env.example
└── README.md
```

## Running Tests

```bash
source venv/bin/activate
python -m pytest tests/ -v
```

## Next Steps

- [ ] Add frontend (React / Streamlit)
- [ ] Batch processing for multiple files
- [ ] Result caching with Redis
- [ ] Local OCR fallback with Tesseract/EasyOCR
- [ ] Export results to PDF/DOCX
- [ ] Authentication & rate limiting
