import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

from typing import List, Dict, Any

def process_stat(stat: Dict[str, Dict[str, Any]], organization: str) -> None:
    authors = []
    commit_counts = []
    k = 1
    print('Результаты: ')
    with open(f'{organization}_stat.txt', 'w') as file:
        for email, author_info in stat:
            author_str = f'{author_info["name"]} ({email})'
            stat_str = f'{k}. {author_str} - {author_info["commits_count"]} коммитов'
            k += 1

            file.write(stat_str + '\n')
            print(stat_str)

            authors.append(f'{author_info["name"]} ({email})')
            commit_counts.append(author_info["commits_count"])

    draw_diagram(authors, commit_counts, organization)


def draw_diagram(authors: List[str], commit_counts: List[int], organization: str) -> None:
    plt.figure(figsize=(8, 16))
    sns.barplot(x=commit_counts, y=authors, hue=commit_counts, palette="viridis", legend=False)
    plt.xlabel('Число коммитов')
    plt.title(f'Топ 100 авторов коммитов {organization}')
    plt.xticks(fontsize=9)
    plt.yticks(fontsize=9)
    plt.yticks(np.arange(len(authors)), labels=authors, rotation=0)
    plt.tight_layout()
    plt.savefig(f'top_100_{organization}_commiters.jpg', dpi=300, bbox_inches='tight')
    plt.show()