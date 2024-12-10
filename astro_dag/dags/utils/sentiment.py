from langdetect import detect
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from googletrans import Translator

def analyze_sentiment(comment):
    local_sia = SentimentIntensityAnalyzer()
    local_translator = Translator()
    sentiment_language = {}

    if isinstance(comment, str):
        try:
            language_detect = detect(comment)
            if language_detect != 'en':
                comment = local_translator.translate(comment, src=language_detect, dest='en').text
                sentiment_language['translated_comment'] = comment
            sentiment_language = local_sia.polarity_scores(comment)
            sentiment_language['language'] = language_detect
            return sentiment_language
        except Exception as e:
            print(f'Translation failed: {e}')
            pass
    return {}
