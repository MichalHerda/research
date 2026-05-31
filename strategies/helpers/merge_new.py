"""
Incremental Directory Merger (Time-Based Data Append)

Description:
    Merges the contents of a directory containing older data (<dirA>) with a directory
    containing newer data (<dirB>). For existing files, it looks at the very last
    timestamp in <dirA> and appends only the newer chronological rows from <dirB>.
    The output is saved in a completely new directory, leaving originals untouched.

Usage:
    python3 merge_new.py <dirA> <dirB>

Arguments:
    <dirA> : Path to the directory with OLDER baseline data (acts as the foundation).
    <dirB> : Path to the directory with NEWER incremental data (acts as the source of updates).

Important Constraints & Logic:
    1. Order of arguments is critical: <dirA> MUST be old data, <dirB> MUST be new data.
    2. Files must be semicolon-separated (;) and sorted chronologically (oldest to newest).
    3. The first column of each data row must be the timestamp.
    4. If a file exists in B but not in A, it will be copied entirely.
    5. Timestamps are compared as strings (lexicographically), so format consistency matters.

Output:
    Creates a new directory named "<dirA>_merged" containing the combined data.
    Aborts automatically if the target directory already exists to prevent data overwrite.
"""

import os
import sys
import shutil


def read_last_timestamp(file_path):
    with open(file_path, 'r') as f:
        lines = f.readlines()
        if len(lines) <= 1:
            return None
        return lines[-1].split(';')[0].strip()


def merge_files(file_a, file_b, out_file):
    # 🔥 KLUCZOWA LINIA – zawsze twórz katalog
    os.makedirs(os.path.dirname(out_file), exist_ok=True)

    if not os.path.exists(file_a):
        # jeśli nie ma w A, kopiujemy cały plik B
        shutil.copy2(file_b, out_file)
        return

    last_ts = read_last_timestamp(file_a)

    with open(file_a, 'r') as fa:
        a_lines = fa.readlines()

    with open(file_b, 'r') as fb:
        b_lines = fb.readlines()

    header = a_lines[0]
    a_data = a_lines[1:]

    new_lines = []
    for line in b_lines[1:]:
        ts = line.split(';')[0].strip()
        if last_ts is None or ts > last_ts:
            new_lines.append(line)

    with open(out_file, 'w') as out:
        out.write(header)
        out.writelines(a_data)
        out.writelines(new_lines)


def merge_dirs(dirA, dirB, out_dir):
    for root, dirs, files in os.walk(dirB):
        rel_path = os.path.relpath(root, dirB)
        target_dir = os.path.join(out_dir, rel_path)

        for file in files:
            file_b = os.path.join(root, file)
            file_a = os.path.join(dirA, rel_path, file)
            out_file = os.path.join(target_dir, file)

            merge_files(file_a, file_b, out_file)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 merge_new.py <dirA> <dirB>")
        sys.exit(1)

    dirA = sys.argv[1]
    dirB = sys.argv[2]
    out_dir = dirA.rstrip('/') + "_merged"

    if os.path.exists(out_dir):
        print(f"Output directory {out_dir} already exists!")
        sys.exit(1)

    merge_dirs(dirA, dirB, out_dir)

    print(f"Done. Output saved in: {out_dir}")
