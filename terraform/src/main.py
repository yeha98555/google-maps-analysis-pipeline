import jieba
from flask import make_response
from textblob import TextBlob

stopwords = set(["的", "是", "在", "我"])


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


def analyze_sentiment(text: str) -> float:
    try:
        text = preprocess_text(text)
        if not text.strip():
            return 0
        blob = TextBlob(text)
        polarity = blob.sentiment.polarity
        return round(polarity, 2)
    except Exception as e:
        print(f"An error occurred: {e}")
        return 0


def handler(request):
    text = request.get_data(as_text=True)
    sentiment = analyze_sentiment(text)
    headers = {"Content-Type": "text/plain"}
    response = make_response(str(sentiment), 200, headers)
    return response
