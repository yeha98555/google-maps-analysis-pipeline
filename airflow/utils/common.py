import hashlib


def rename_place_id(place_name):
    # place_id 使用自己設定的編碼規則, 暫時使用md5 hash
    return "place_" + hashlib.md5(place_name.encode("utf-8")).hexdigest()
