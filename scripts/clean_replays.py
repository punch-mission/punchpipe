import argparse
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('file')
    args = parser.parse_args()

    input_path = Path(args.file)
    output_file = input_path.parent / f"merged_{input_path.name}"
    output_file_soc = input_path.parent / f"merged_soc_{input_path.name}"

    df = pd.read_csv(args.file)

    df = df.sort_values('start_block').reset_index(drop=True)

    df_week = df[pd.to_datetime(df['start_time']) >= (datetime.now() - timedelta(days=7))]
    df_week = df_week[pd.to_datetime(df_week['start_time']) <= datetime.now()]

    merged_blocks = []

    for _, row in df_week.iterrows():
        start = row['start_block']
        length = row['replay_length']
        end = start + length
        start_time = row['start_time']

        if merged_blocks and start <= merged_blocks[-1]['end']:
            last_block = merged_blocks[-1]

            print(f"Overlap found: Block {start}-{end} overlaps with {last_block['start_block']}-{last_block['end']}")

            new_end = max(last_block['end'], end)
            new_length = new_end - last_block['start_block'] + 1

            merged_blocks[-1]['end'] = new_end
            merged_blocks[-1]['replay_length'] = new_length

            print(f"Merged into: Block {last_block['start_block']}-{new_end} (length: {new_length})")

        else:
            # No overlap - add as new block
            merged_blocks.append({
                'start_time': start_time,
                'start_block': start,
                'replay_length': length,
                'end': end
            })

    result_df = pd.DataFrame(merged_blocks).drop('end', axis=1)

    print(f"\nOriginal blocks: {len(df)}")
    print(f"Merged blocks: {len(result_df)}")
    print(f"Blocks merged: {len(df) - len(result_df)}")

    result_df.to_csv(output_file_soc, index=False)
    print(f"Results written to {output_file_soc}")

    # Write directly to file without modifying the DataFrame
    with open(output_file, 'w') as f:
        for _, row in result_df.iterrows():
            f.write(f"start mops_fsw_start_fast_replay(xfi,{row['start_block']},{row['replay_length']})\n")
