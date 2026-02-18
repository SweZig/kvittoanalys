"""Command-line tool for quick document/image analysis."""

import argparse
import json
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from app.config import settings
from app.services.document_loader import DocumentLoader
from app.services.image_analyzer import ImageAnalyzer


def main():
    parser = argparse.ArgumentParser(
        description="DocVision CLI — Analyze documents and images with AI"
    )
    parser.add_argument("file", help="Path to the file to analyze")
    parser.add_argument(
        "--mode",
        choices=["analyze", "ocr", "describe", "query"],
        default="analyze",
        help="Analysis mode (default: analyze)",
    )
    parser.add_argument("--query", "-q", help="Custom question (for query mode)")
    parser.add_argument(
        "--language", "-l", default="swedish", help="Response language (default: swedish)"
    )
    parser.add_argument("--json", action="store_true", help="Output as JSON")

    args = parser.parse_args()

    file_path = Path(args.file)
    if not file_path.exists():
        print(f"❌ File not found: {file_path}")
        sys.exit(1)

    if not settings.anthropic_api_key or settings.anthropic_api_key == "your-api-key-here":
        print("❌ Set your ANTHROPIC_API_KEY in .env first!")
        print("   Copy .env.example to .env and add your key.")
        sys.exit(1)

    loader = DocumentLoader()
    analyzer = ImageAnalyzer()

    print(f"📄 Loading: {file_path.name}")
    content_blocks = loader.load_file(file_path)
    print(
        f"   → {len(content_blocks)} page(s) loaded"
    )

    print(f"🔍 Analyzing ({args.mode})...\n")

    if args.mode == "analyze":
        result = analyzer.analyze(content_blocks, language=args.language)
    elif args.mode == "ocr":
        result = analyzer.extract_text(content_blocks)
    elif args.mode == "describe":
        result = analyzer.describe_image(content_blocks, language=args.language)
    elif args.mode == "query":
        if not args.query:
            print("❌ --query is required for query mode")
            sys.exit(1)
        result = analyzer.custom_query(
            content_blocks, query=args.query, language=args.language
        )

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print("═" * 60)
        print(result["analysis"])
        print("═" * 60)
        print(
            f"\n📊 {result['pages_analyzed']} page(s) | "
            f"{result['input_tokens']} input tokens | "
            f"{result['output_tokens']} output tokens"
        )


if __name__ == "__main__":
    main()
