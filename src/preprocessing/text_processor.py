import re
import emoji
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TextPreprocessor:
    def __init__(self, remove_stopwords: bool = True, lemmatize: bool = True):
        self.remove_stopwords = remove_stopwords
        self.lemmatize = lemmatize
        self._ensure_nltk_data()
        
        if self.remove_stopwords:
            self.stop_words = set(stopwords.words('english'))
        
        if self.lemmatize:
            self.lemmatizer = WordNetLemmatizer()
    
    def _ensure_nltk_data(self):
        try:
            nltk.data.find('tokenizers/punkt')
        except LookupError:
            logger.info("Downloading NLTK punkt tokenizer...")
            nltk.download('punkt', quiet=True)
        
        try:
            nltk.data.find('corpora/stopwords')
        except LookupError:
            logger.info("Downloading NLTK stopwords...")
            nltk.download('stopwords', quiet=True)
        
        try:
            nltk.data.find('corpora/wordnet')
        except LookupError:
            logger.info("Downloading NLTK wordnet...")
            nltk.download('wordnet', quiet=True)
    
    def clean_text(self, text: str) -> str:
        text = emoji.demojize(text, delimiters=(" ", " "))
        
        text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
        
        text = re.sub(r'@\w+', '', text)
        
        text = re.sub(r'#(\w+)', r'\1', text)
        
        text = re.sub(r'RT[\s]+', '', text)
        
        text = re.sub(r'[^\w\s]', '', text)
        
        text = re.sub(r'\s+', ' ', text)
        
        text = text.lower().strip()
        
        return text
    
    def tokenize(self, text: str) -> list[str]:
        return word_tokenize(text)
    
    def remove_stopwords_from_tokens(self, tokens: list[str]) -> list[str]:
        if not self.remove_stopwords:
            return tokens
        return [token for token in tokens if token not in self.stop_words]
    
    def lemmatize_tokens(self, tokens: list[str]) -> list[str]:
        if not self.lemmatize:
            return tokens
        return [self.lemmatizer.lemmatize(token) for token in tokens]
    
    def preprocess(self, text: str) -> str:
        cleaned = self.clean_text(text)
        
        tokens = self.tokenize(cleaned)
        
        if self.remove_stopwords:
            tokens = self.remove_stopwords_from_tokens(tokens)
        
        if self.lemmatize:
            tokens = self.lemmatize_tokens(tokens)
        
        return ' '.join(tokens)
    
    def preprocess_batch(self, texts: list[str]) -> list[str]:
        return [self.preprocess(text) for text in texts]


class FeatureExtractor:
    def __init__(self):
        self.preprocessor = TextPreprocessor()
    
    def extract_features(self, text: str) -> dict[str, any]:
        features = {}
        
        features['original_length'] = len(text)
        
        features['num_words'] = len(text.split())
        
        features['num_uppercase'] = sum(1 for c in text if c.isupper())
        
        features['num_hashtags'] = len(re.findall(r'#\w+', text))
        
        features['num_mentions'] = len(re.findall(r'@\w+', text))
        
        features['num_urls'] = len(re.findall(r'http\S+|www\S+', text))
        
        features['num_exclamation'] = text.count('!')
        features['num_question'] = text.count('?')
        
        features['num_emojis'] = len(re.findall(r'[😀-🙏🌀-🗿🚀-🛿]', text))
        
        features['has_retweet'] = 1 if text.startswith('RT') else 0
        
        features['preprocessed_text'] = self.preprocessor.preprocess(text)
        
        return features
    
    def extract_features_batch(self, texts: list[str]) -> list[dict[str, any]]:
        return [self.extract_features(text) for text in texts]


def preprocess_for_model(text: str, max_length: int = 128) -> str:
    preprocessor = TextPreprocessor(remove_stopwords=False, lemmatize=False)
    cleaned = preprocessor.clean_text(text)
    
    words = cleaned.split()
    if len(words) > max_length:
        cleaned = ' '.join(words[:max_length])
    
    return cleaned
