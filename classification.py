import numpy as np
import pandas as pd
import re
import argparse
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report


class CommitClassifier:

    def train(self, X_train, y_train):
        self.vectorizer = TfidfVectorizer(max_features=1000, stop_words='english')
        X = self.vectorizer.fit_transform(X_train)
        y = np.array(y_train)

        self.model = LogisticRegression()
        self.model.fit(X, y)


    def predict(self, X_test):
        X = self.vectorizer.transform(X_test)
        y_pred = self.model.predict(X)

        return y_pred

    

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", dest="data", help="Path to csv data", required=True)
    return parser.parse_args()

if __name__ == "__main__":
    args = get_args()
    df = pd.read_csv(args.data)

    messages = df["Commit Message"].tolist()
    labels = df["Keyword Label"].tolist()

    X_train, X_test, y_train, y_test = train_test_split(messages, labels, test_size=0.2, random_state=42)

    model = CommitClassifier()
    model.train(X_train, y_train)

    y_pred = model.predict(X_test)

    print(classification_report(y_test, y_pred))

