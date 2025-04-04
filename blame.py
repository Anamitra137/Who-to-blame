import pygit2
import argparse
import pandas as pd
import os
from tqdm import tqdm
import re

def keyword_label(message: str) -> int:
    FIX_KEYWORDS = ['fix', 'bug', 'patch', 'resolve', 'error', 'correct']

    if any(re.search(rf'\b{kw}\b', message, re.IGNORECASE) for kw in FIX_KEYWORDS):
        return 1    # 1 -> bugfix
    else:
        return 0
    
def get_previous_comparison(repo, commit, file_path, start_line, end_line):
    found = False
    parent_commit = commit
    while(not found):   # Compare with which commit? Search for parent, then parent's parent and so on
        if parent_commit.parents:
            parent_commit = parent_commit.parents[0]    # In case of merge commit choose the first one, this is a design choice
        else:   # No parent present
            break

        diff = repo.diff(parent_commit, commit)

        # check if the file is in the changes
        change_present = any(delta.new_file.path == file_path for delta in diff.deltas)

        if change_present:  # See if the changes involve `start_line` to `end_line` in `file_path` of `commit`
            for patch in diff: 
                if patch.delta.new_file.path == file_path:
                    for hunk in patch.hunks:
                        for line in hunk.lines:
                            if start_line <= line.new_lineno <= end_line:
                                found = True
                                return str(parent_commit.id)[:7], parent_commit.author.name
    return None, None       # No previous commit found, means `commit` is where the changes first appeared


def get_blame_spans(repo_path: str, file_path: str, df):
    repo = pygit2.Repository(repo_path)
    blame = repo.blame(file_path)

    # Get spans
    for hunk in blame:
        commit = repo.get(hunk.orig_commit_id)
        author = commit.author.name
        commit_hash = str(commit.id)[:7]
        commit_message = commit.message.strip().replace("\n", " ")
        start_line = hunk.final_start_line_number
        end_line = start_line + hunk.lines_in_hunk - 1

        label = keyword_label(commit_message)
        
        prev_commit_hash, prev_author = get_previous_comparison(repo, commit, file_path, start_line, end_line)

        df.loc[len(df)] = [file_path, commit_hash, author, commit_message, start_line, end_line, prev_author, prev_commit_hash, label]


def list_files_scandir(path: str = '.'):
    # file_list = []
    with os.scandir(path) as entries:
        for entry in entries:
            if entry.name.startswith('.'):
                continue    # ignore .files like .git .env etc
            if entry.is_file():
                # file_list.append(entry.path)
                yield entry.path
            elif entry.is_dir():
                # file_list = file_list + list_files_scandir(entry.path)
                yield from list_files_scandir(entry.path)
    # return file_list


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", dest="repo", help="Path to repository", required=True)
    return parser.parse_args()

if __name__ == "__main__":
    args = get_args()

    df = pd.DataFrame(columns=["File", "Commit", "Author", "Commit Message", "Start Line", "End Line", "Previous Author", "Previous Commit", "Keyword Label"])

    for file_path in tqdm(list_files_scandir(args.repo)):
        file_path = file_path[len(args.repo)+1:].replace('\\','/')
        # print(file_path)

        get_blame_spans(args.repo, file_path, df)

    df.to_csv(f"{args.repo}_blame_data.csv")
