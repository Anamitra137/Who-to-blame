# Who-to-blame
A study on who does bugfixes - the author or somebody else and how long a bug goes unnoticed or how much time it takes to fix.

## Data
Top 100 repositories were picked up from [Github-Ranking/Top100](https://github.com/EvanLi/Github-Ranking/tree/master/Top100) in April 2025. Among them we choose 5 languages (top 5 according to [this](https://madnight.github.io/githut/#/stars/2024/1)). The picks are kept in `data` folder.

## Output
The outputs are kept in the `output` folder

## How to Run
Generates `<path-to-repo>_blame_data.csv`
```bash
python blame.py --repo <path-to-repo>
```

<br>

Labels data for `<path-to-repo>_blame_data.csv`
```bash
python label.py --data <path-to-repo>_blame_data.csv --key <Groq API Key>
```

<br>

Generates plots for each language
```bash
python stat.py --dir <parent>/output/<LANG>
```