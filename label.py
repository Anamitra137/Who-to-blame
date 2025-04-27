import pandas as pd
import requests
import time
import argparse

# Config
API_URL = "https://api.groq.com/openai/v1/chat/completions"
API_KEY = None
MODEL_NAME = "llama3-8b-8192"

# Keywords for fast filtering
BUGFIX_KEYWORDS = [
    "fix", "fixed", "bug", "crash", "error", "failure", "fault", "defect", "patch", "issue", "correct", "repair", "resolve"
]
NOT_BUGFIX_KEYWORDS = [
    "feature", "add", "enhance", "improve", "update", "upgrade", "optimize", "refactor", "cleanup", "docs", "documentation"
]

# Keyword-based guess
def keyword_guess(message):
    msg = message.lower()

    if any(word in msg for word in BUGFIX_KEYWORDS):
        return True
    if any(word in msg for word in NOT_BUGFIX_KEYWORDS):
        return False
    return None  # uncertain

# LLaMA classification for a batch
def classify_batch(messages):
    batch_prompt = "Classify each commit message below as 'yes' (bug fix) or 'no' (not bug fix). Respond only with the answers like:\n\n1. yes\n2. no\n3. yes\n...\n\nCommit messages:\n"
    for idx, msg in enumerate(messages, start=1):
        batch_prompt += f"{idx}. {msg}\n"

    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": "You are a commit message classifier."},
            {"role": "user", "content": batch_prompt}
        ],
        "temperature": 0.0
    }

    response = requests.post(API_URL, headers=headers, json=payload)
    if response.status_code != 200:
        raise Exception(f"API call failed: {response.status_code}, {response.text}")
    
    result = response.json()
    reply = result['choices'][0]['message']['content'].strip().lower()

    outputs = []
    for line in reply.splitlines():
        if "yes" in line:
            outputs.append(True)
        elif "no" in line:
            outputs.append(False)

    if len(outputs) != len(messages):
        raise ValueError(f"Mismatch: Expected {len(messages)} results but got {len(outputs)}.")
    
    return outputs


def main(file_path):
    df = pd.read_csv(file_path)
    df['is_bugfix'] = None  # New column for labels

    uncertain_indices = []

    # Keyword-based local labeling
    for idx, row in df.iterrows():
        guess = keyword_guess(row['Commit Message'])
        if guess is not None:
            df.at[idx, 'is_bugfix'] = guess
        else:
            uncertain_indices.append(idx)

    print(f"Auto-labeled {len(df) - len(uncertain_indices)} commits.")
    print(f"{len(uncertain_indices)} commits need LLaMA classification.")

    # LLaMA classification for uncertain commits
    BATCH_SIZE = 5 

    for start_idx in range(0, len(uncertain_indices), BATCH_SIZE):
        batch_indices = uncertain_indices[start_idx:start_idx+BATCH_SIZE]
        batch_messages = df.loc[batch_indices, 'Commit Message'].tolist()

        success = False
        for attempt in range(2):  # try, if fail retry only once
            try:
                batch_labels = classify_batch(batch_messages)
                for idx, label in zip(batch_indices, batch_labels):
                    df.at[idx, 'is_bugfix'] = label
                success = True
                break  # success, exit retry loop
            except Exception as e:
                if attempt == 0:
                    print(f"Error during batch {start_idx}-{start_idx+BATCH_SIZE}: {e}")
                    print("Retrying after short delay...")
                    time.sleep(5)  # short pause before retry
                else:
                    print(f"Failed again after retry. Skipping batch {start_idx}-{start_idx+BATCH_SIZE}.")
        if not success:
            for idx in batch_indices:
                df.at[idx, 'is_bugfix'] = None  # Mark as failed or skip

        time.sleep(1)  # API limits

    df.to_csv(file_path[:-4] + "_labeled.csv", index=False)
    print("Labeled commits saved.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True, help="Path to the dataset")
    parser.add_argument("--key", required=True, help="Groq API key")
    args = parser.parse_args()

    API_KEY = args.key

    main(args.data)
