#!/usr/bin/env python3

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import json
import sys
from collections import Counter

def traverse_ids(obj, ids):
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key == "id":
                ids.append(value)
            traverse_ids(value, ids)
    elif isinstance(obj, list):
        for item in obj:
            traverse_ids(item, ids)

def check_ids(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    ids = []
    traverse_ids(data, ids)

    # Find duplicates.
    counts = Counter(ids)
    duplicates = {k: v for k, v in counts.items() if v > 1}

    # Collect numeric IDs.
    numeric_ids = []
    for id_value in ids:
        try:
            numeric_ids.append(int(id_value))
        except (ValueError, TypeError):
            continue

    # print the current maximum numeric id.
    if numeric_ids:
        current_max = max(numeric_ids)
        print(f"Current max numeric id: {current_max}")
    else:
        print("No numeric IDs found; cannot compute max numeric id.")

    if duplicates:
        print("Found duplicate id(s):")
        for dup, count in duplicates.items():
            print(f"  {dup}: {count} times")
        sys.exit(1)
    else:
        print("No duplicate id found.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 lint_grafana.py <path/to/dashboard.json>")
        sys.exit(1)
    file_path = sys.argv[1]
    print(f"Linting: {file_path}")
    check_ids(file_path)
