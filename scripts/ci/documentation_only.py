"""Return whether a pull request's complete changed-file list is Markdown only.

Input is one JSON object per line from GitHub's paginated pull-request files API.
The caller must fail if that API request fails. Empty or malformed lists are
errors; a list at GitHub's 3,000-file limit forces the full check.
"""

import json
import sys


MAX_FILES = 3000  # GitHub's pull-request files endpoint stops here.


def documentation_only(lines):
    paths = []
    for line in lines:
        item = json.loads(line)
        if not isinstance(item, dict):
            raise ValueError("a changed-file entry must be an object")
        path = item.get("filename")
        status = item.get("status")
        if not isinstance(path, str) or not path or not isinstance(status, str):
            raise ValueError("a changed-file entry needs a path and status")
        paths.append(path)
        if status == "renamed":
            previous = item.get("previous_filename")
            if not isinstance(previous, str) or not previous:
                raise ValueError("a renamed file needs its previous path")
            paths.append(previous)
        elif item.get("previous_filename") is not None:
            previous = item["previous_filename"]
            if not isinstance(previous, str) or not previous:
                raise ValueError("an old path must be nonempty")
            paths.append(previous)
    if not paths:
        raise ValueError("the changed-file list is empty")
    if len(paths) >= MAX_FILES:
        return False
    return all(path.endswith(".md") for path in paths)


if __name__ == "__main__":
    try:
        print(str(documentation_only(sys.stdin)).lower())
    except (ValueError, json.JSONDecodeError) as error:
        print(f"Cannot classify pull-request files: {error}", file=sys.stderr)
        sys.exit(1)
