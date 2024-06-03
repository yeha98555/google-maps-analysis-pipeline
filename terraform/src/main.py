import functions_framework
import jieba
from snownlp import SnowNLP
import json
import logging
from flask import make_response
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(level=logging.INFO)


stopwords = set([
    "的", "是", "在", "我", "有", "和", "也", "了", "就", "人", "都", "而", "他", "她", "你", "們", "我們", "你們", "他們", "她們", "這", "那", "這個", "那個", "這些", "那些", "什麼", "什麼時候", "怎麼", "怎麼樣", "為什麼", "因為", "所以", "如果", "但是", "而且", "並且", "或者", "還有", "以及", "跟", "與", "從", "到", "由", "於", "在於", "之", "之後", "之前"])


def preprocess_text(text: str) -> str:
    """
    Preprocess text: cut words and remove stopwords

    Args:
        text (str): The text to be processed
    Returns:
        str: The processed text
    """
    words = jieba.cut(text)
    filtered_words = [word for word in words if word not in stopwords]
    return " ".join(filtered_words)

def analyze_text(text: str) -> json:
    """
    Analyze text sentiment and return score and processed text

    Args:
        text (str): The text to be analyzed
    Returns:
        json: The sentiment score (0 to 1), and error if error occurs
    """
    if not text.strip():
        return {"score": 0}

    try:
        processed_text = preprocess_text(text)
        score = SnowNLP(processed_text).sentiments if processed_text.strip() else 0
        return {"score": score}
    except Exception as e:
        logging.error(f"Error in analyze_text: {e}")
        return {"score": 0, "error": str(e)}

@functions_framework.http
def analyze_sentiment(request):
    try:
        request_json = request.get_json(silent=True)
        if request_json is None:
            raise ValueError("Invalid JSON payload")

        calls = request_json.get('calls', [])
        if not calls:
            raise ValueError("No calls found in the request")

        response_data = []

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(analyze_text, call[0]) for call in calls]
            for future in as_completed(futures):
                response_data.append(json.dumps(future.result()))

        response = make_response(json.dumps({"replies": response_data}))
        response.headers['Content-Type'] = 'application/json'
        return response

    except Exception as e:
        logging.error(f"Error processing request: {e}")
        error_response = make_response(json.dumps({"replies": str(e)}))
        error_response.headers['Content-Type'] = 'application/json'
        return error_response, 500
