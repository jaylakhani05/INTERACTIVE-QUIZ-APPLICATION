import pandas as pd
import re
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

# Download required NLTK resources
nltk.download('punkt')
nltk.download('stopwords')

# Load dataset
df = pd.read_csv("E:/Internship/TASK4/demo_imdb_reviews_1000.csv")  # Adjust path if needed

# Prepare stopwords
stop_words = set(stopwords.words("english"))

# Text cleaning function
def clean_text(text):
    text = re.sub(r"<.*?>", "", str(text))               # Remove HTML tags
    tokens = word_tokenize(text.lower())                 # Tokenize and lowercase
    filtered = [word for word in tokens if word.isalpha() and word not in stop_words]  # Remove stopwords and non-alpha
    return " ".join(filtered)

# Clean the reviews
df["clean_review"] = df["review"].apply(clean_text)

# Count sentiment votes
sentiment_counts = df["sentiment"].value_counts()

# Print sentiment votes
print("\nSentiment Vote Counts:")
print(sentiment_counts)

# Show some sample cleaned reviews with sentiment
print("\nSample Cleaned Reviews with Sentiment:")
print(df[["clean_review", "sentiment"]].head(10))
