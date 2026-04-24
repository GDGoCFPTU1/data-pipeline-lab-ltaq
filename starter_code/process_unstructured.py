import re

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================

def clean_pdf_text(raw_text: str) -> str:
    # Remove noise like HEADER_PAGE_1 and FOOTER_PAGE_1
    cleaned = re.sub(r'HEADER_PAGE_\d+', '', raw_text)
    cleaned = re.sub(r'FOOTER_PAGE_\d+', '', cleaned)
    return cleaned.strip()

def process_pdf_data(raw_json: dict) -> dict:
    # Bước 1: Làm sạch nhiễu (Header/Footer) khỏi văn bản
    content = raw_json.get("extractedText", "")
    content = clean_pdf_text(content)
    
    # Bước 2: Map dữ liệu thô sang định dạng chuẩn của UnifiedDocument
    return {
        "document_id": raw_json.get("docId", "unknown_pdf"),
        "source_type": "PDF",
        "author": raw_json.get("authorName", "Unknown").strip(),
        "category": raw_json.get("docCategory", "General"),
        "content": content,
        "timestamp": raw_json.get("createdAt", "N/A")
    }

def process_video_data(raw_json: dict) -> dict:
    # Map dữ liệu thô từ Video sang định dạng chuẩn (giống PDF)
    return {
        "document_id": raw_json.get("video_id", "unknown_vid"),
        "source_type": "Video",
        "author": raw_json.get("creator_name", "Unknown"),
        "category": raw_json.get("category", "General"),
        "content": raw_json.get("transcript", ""),
        "timestamp": raw_json.get("published_timestamp", "N/A")
    }
