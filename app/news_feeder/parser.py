import re
from typing import List


def extract_tags(text: str) -> List[str]:
    if not text:
        return []

    pattern = r"#(\w+)"
    tags = re.findall(pattern, text)

    unique_tags = list(set(tag.lower() for tag in tags))

    return unique_tags
