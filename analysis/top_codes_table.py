import sys
from pathlib import Path

import pandas as pd
from study_utils import create_top_5_code_table
from variables import codelist_file, low_count_threshold, rounding_base


if len(sys.argv) > 1:
    output_dir = sys.argv[1]
else:
    output_dir = "output"


def main():
    code_df = pd.read_csv(f"{output_dir}/counts_per_code.csv")
    codelist = pd.read_csv(codelist_file)

    top_5_code_table = create_top_5_code_table(
        df=code_df,
        code_df=codelist,
        code_column="code",
        term_column="term",
        low_count_threshold=low_count_threshold,
        rounding_base
    )
    top_5_code_table.to_csv(f"{output_dir}/top_5_code_table.csv", index=False)


if __name__ == "__main__":
    main()
