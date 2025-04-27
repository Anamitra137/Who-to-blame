import os
import pygit2
import subprocess
import pandas as pd
import re
import concurrent.futures
import multiprocessing
from tqdm import tqdm
from datetime import datetime, timezone

def keyword_label(message: str) -> int:
    FIX_KEYWORDS = [
    "fix", "fixed", "bug", "crash", "error", "failure", "fault", "defect", "patch", "issue", "correct", "repair", "resolve"
    ]
    if any(re.search(rf'\b{kw}\b', message, re.IGNORECASE) for kw in FIX_KEYWORDS):
        return 1    # 1 -> bugfix
    else:
        return 0


def list_files_scandir(path: str = '.'):
    with os.scandir(path) as entries:
        for entry in entries:
            if entry.name.startswith('.'):
                continue
            if entry.is_file():
                yield entry.path
            elif entry.is_dir():
                yield from list_files_scandir(entry.path)


# def get_previous_comparison(repo, commit, file_path, start_line, end_line):
#     found = False
#     parent_commit = commit
#     while(not found):   # Compare with which commit? Search for parent, then parent's parent and so on
#         if parent_commit.parents:
#             parent_commit = parent_commit.parents[0]    # In case of merge commit choose the first one, this is a design choice
#         else:   # No parent present
#             break

#         diff = repo.diff(parent_commit, commit)

#         # check if the file is in the changes
#         change_present = any(delta.new_file.path == file_path for delta in diff.deltas)

#         if change_present:  # See if the changes involve `start_line` to `end_line` in `file_path` of `commit`
#             for patch in diff: 
#                 if patch.delta.new_file.path == file_path:
#                     for hunk in patch.hunks:
#                         for line in hunk.lines:
#                             if start_line <= line.new_lineno <= end_line:
#                                 found = True
#                                 return str(parent_commit.id)[:7], parent_commit.author.name
#     return None, None       # No previous commit found, means `commit` is where the changes first appeared


def get_previous_comparison(repo, commit, file_path, start_line, end_line):
    """Find previous commit affecting the hunk via git log."""
    repo_root = repo.path.rstrip('/.git')
    try:
        result = subprocess.run(
            ['git', 'log', '-n', '100', '--follow', '--format=%H', '--', file_path],    # Limiting by 100, as a design choice
            cwd=repo_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        commit_hashes = result.stdout.strip().splitlines()

        current_commit_hash = str(commit.id)
        if current_commit_hash not in commit_hashes:
            return None, None, None

        current_index = commit_hashes.index(current_commit_hash)

        for prev_hash in commit_hashes[current_index+1:]:
            prev_commit = repo.get(prev_hash)
            try:
                diff = repo.diff(prev_commit, commit)
                for patch in diff:
                    if patch.delta.new_file.path == file_path:
                        for hunk in patch.hunks:
                            hunk_start = hunk.new_start
                            hunk_end = hunk.new_start + hunk.new_lines - 1
                            if not (hunk_end < start_line or hunk_start > end_line):
                                prev_author = prev_commit.author.name
                                prev_commit_date = datetime.fromtimestamp(prev_commit.commit_time, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                                return str(prev_commit.id)[:7], prev_author, prev_commit_date
            except Exception:
                continue
    except subprocess.CalledProcessError:
        pass

    return None, None, None



def process_hunk(repo, hunk, file_path):
    """Process one hunk and return a row for the final DataFrame."""
    commit = repo.get(hunk.orig_commit_id)
    author = commit.author.name
    commit_hash = str(commit.id)[:7]
    commit_message = commit.message.strip().replace("\n", " ")
    commit_date = datetime.fromtimestamp(commit.commit_time, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

    start_line = hunk.final_start_line_number
    end_line = start_line + hunk.lines_in_hunk - 1

    label = keyword_label(commit_message)

    prev_commit_hash, prev_author, prev_commit_date = get_previous_comparison(repo, commit, file_path, start_line, end_line)

    return [
        file_path, commit_hash, author, commit_message,
        start_line, end_line,
        prev_author, prev_commit_hash,
        label,
        commit_date, prev_commit_date
    ]


def process_file(repo_path, file_path):
    """Process one file and return a list of rows (one per hunk)."""
    try:
        repo = pygit2.Repository(repo_path)
        blame = repo.blame(file_path)
    except Exception:
        return []  # In case file is missing or corrupted in Git history

    rows = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
        futures = [executor.submit(process_hunk, repo, hunk, file_path) for hunk in blame]
        for future in concurrent.futures.as_completed(futures):
            try:
                row = future.result()
                rows.append(row)
            except Exception:
                continue
    return rows

def main(repo_path):
    file_list = list(list_files_scandir(repo_path))
    relative_file_list = [
        os.path.relpath(f, repo_path).replace('\\', '/')
        for f in file_list
    ]

    all_rows = []

    with concurrent.futures.ProcessPoolExecutor(max_workers=min(os.cpu_count(), 8)) as executor:
        futures = []
        for file_path in relative_file_list:
            futures.append(executor.submit(process_file, repo_path, file_path))

        for f in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Processing files"):
            try:
                rows = f.result()
                all_rows.extend(rows)
            except Exception:
                continue

    df = pd.DataFrame(all_rows, columns=[
        "File", "Commit", "Author", "Commit Message",
        "Start Line", "End Line",
        "Previous Author", "Previous Commit",
        "Keyword Label",
        "Commit Date", "Previous Commit Date"
    ])

    df.to_csv(f"{repo_path.rstrip('/')}_blame_data.csv", index=False)
    print(f"Saved final dataset with {len(df)} entries.")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", required=True, help="Path to the Git repository")
    args = parser.parse_args()

    main(args.repo)
