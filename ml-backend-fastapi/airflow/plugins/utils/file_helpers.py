import os
import re

def sanitize_filename(name, max_length=25):
    """파일명 안전하게 처리"""
    cleaned = re.sub(r'[\\/*?:"<>|]', '_', name)
    return cleaned[:max_length]

def ensure_directory(dir_path):
    """디렉토리 확인 및 생성"""
    os.makedirs(dir_path, exist_ok=True)
    return dir_path