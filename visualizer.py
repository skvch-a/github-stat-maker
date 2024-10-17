import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

from requests import ORGANIZATION

def process_stat(stat):
    authors = []
    commits_counts = []
    for email, author_info in stat:
        authors.append(f'{author_info["name"]} ({email})')
        commits_counts.append(author_info["commits_count"])
    return authors, commits_counts


def draw_diagram(stat):
    authors, commits_counts = process_stat(stat)
    plt.figure(figsize=(8, 16))
    sns.barplot(x=commits_counts, y=authors, hue=commits_counts, palette="viridis", legend=False)
    plt.xlabel('Число коммитов')
    plt.title(f'Топ 100 авторов коммитов {ORGANIZATION}')
    plt.xticks(fontsize=9)
    plt.yticks(fontsize=9)
    plt.yticks(np.arange(len(authors)), labels=authors, rotation=0)
    plt.tight_layout()
    plt.savefig(f'top_100_{ORGANIZATION}_commiters.jpg', dpi=300, bbox_inches='tight')
    plt.show()