"""
This script validates a JSONL file to ensure each line is a valid JSON object
and that each object contains a 'data' key with a list of exactly one entry.
It also redacts specific fields ('class_5_min' and 'met') from the entries
and saves the processed entries to a new JSONL file.
"""


import json
import os
def validate_jsonl(file_path):
    valid = True
    processed_lines = []
    output_path = os.path.join(os.path.dirname(file_path), "oura_processed.jsonl")
    with open(file_path, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if 'data' not in obj:
                    print(f"Missing 'data' key on line {i}")
                    valid = False
                elif not isinstance(obj['data'], list) or len(obj['data']) != 1:
                    print(f"Warning: 'data' on line {i} is not a list with exactly one entry")
                else:
                    entry = obj['data'][0]
                    if not isinstance(entry, dict) or 'class_5_min' not in entry:
                        print(f"Warning: Entry in 'data' on line {i} is missing 'class_5_min' field")
                    if not isinstance(entry, dict) or 'met' not in entry:
                        print(f"Warning: Entry in 'data' on line {i} is missing 'met' field")
                    entry_copy = {k: v for k, v in entry.items() if k not in ('class_5_min', 'met')}
                    processed_lines.append(json.dumps(entry_copy))
            except json.JSONDecodeError as e:
                print(f"Invalid JSON on line {i}: {e}")
                valid = False
    with open(output_path, 'w', encoding='utf-8') as out_f:
        for line in processed_lines:
            out_f.write(line + '\n')
    if valid:
        print("All lines are valid JSON.")
    else:
        print("Some lines are invalid JSON.")
    print(f"Processed entries saved to {output_path}")

if __name__ == "__main__":
    validate_jsonl("./data/oura.jsonl")