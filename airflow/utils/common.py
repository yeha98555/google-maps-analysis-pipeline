import hashlib

from fuzzywuzzy import process


def rename_place_id(place_name: str) -> str:
    """
    Rename place_id to a hash value (md5)
    (暫時定義一個編碼規則, 之後未來可以根據業務需求調整)

    Args:
        place_name (str): The name of the place

    Returns:
        str: The hash value of the place name
    """
    return hashlib.md5(place_name.encode("utf-8")).hexdigest()


def mapping_place_id(place_name: str, df) -> str:
    best_match, best_score, *_ = process.extractOne(place_name, df["attraction_name"])

    if best_match:
        return df[df["attraction_name"] == best_match]["attraction_id"].values[0]

    return None
