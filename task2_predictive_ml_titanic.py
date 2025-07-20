# task2_predictive_ml_titanic.py

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score, classification_report

# Load Titanic dataset
url = "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv"
df = pd.read_csv(url)

# Step 1: Understand column structure
print("🔍 Columns in dataset:", df.columns.tolist())

# Step 2: Drop unnecessary columns (only if they exist)
columns_to_drop = ["PassengerId", "Name", "Ticket", "Cabin"]
df.drop(columns=[col for col in columns_to_drop if col in df.columns], inplace=True)

# Step 3: Handle missing values
df["Age"] = df["Age"].fillna(df["Age"].median())
df["Embarked"] = df["Embarked"].fillna(df["Embarked"].mode()[0])

# Step 4: Encode categorical variables
label_encoder = LabelEncoder()
df["Sex"] = label_encoder.fit_transform(df["Sex"])         # male=1, female=0
df["Embarked"] = label_encoder.fit_transform(df["Embarked"])  # C=0, Q=1, S=2 (varies)

# Step 5: Define features (X) and target (y)
X = df.drop("Survived", axis=1)
y = df["Survived"]

# Step 6: Split into train and test sets
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# Step 7: Train a Logistic Regression model
model = LogisticRegression(max_iter=200)
model.fit(X_train, y_train)

# Step 8: Make predictions
y_pred = model.predict(X_test)

# Step 9: Evaluate model
print(f"✅ Accuracy Score: {accuracy_score(y_test, y_pred):.2f}")
print("\n✅ Classification Report:\n", classification_report(y_test, y_pred))
