import os
import glob
import pandas as pd
from pathlib import Path


def match_columns(df, scale_metadata):
    """
    Parameters
    ----------

    Returns
    ----------
    """

    # columns of the scale csv/dataframe
    data_columns  = set(df.columns)  
    # default scale item names from the dictionary
    orig_elements = set(scale_metadata.ElementName)
    # non-matching columns to search for aliases
    check_aliases = data_columns - orig_elements

    # dictionary with items and their aliases
    item_alias_dict = dict(zip(scale_metadata['ElementName'], scale_metadata['Aliases']))
    item_alias_dict = {x:y.split(',') for (x,y) in item_alias_dict.items() if pd.notna(y)}
    
    rename_dict = {}
    for orig_item in check_aliases:
        new_item = [x for (x,y) in item_alias_dict.items() if orig_item in y]
        if len(new_item) > 0:
            rename_dict[new_item[0]] = orig_item
    
    print(f'Total items:        {len(data_columns)}')
    print(f'Matched items:      {len(data_columns) - len(check_aliases)}')
    print(f'Items with aliases: {len(rename_dict)}')
    print(f'Unmatched items:    {len(check_aliases) - len(rename_dict)}')

    #new_df = df.rename(columns=rename_dict)
    scale_metadata.insert(0, 'ColumnName', scale_metadata['ElementName'].replace(rename_dict))
    return scale_metadata


def fetch_dataframe(s3_path, def_file, extra_header):
    """
    Parameters
    ----------

    Returns
    ----------
    """

    # scale metadata
    scale_metadata = pd.read_csv(def_file)

    # read data
    in_df = pd.read_csv(s3_path, sep='\t')
    if extra_header == True:
        pheno_df = in_df.iloc[1:]
    else: 
        pheno_df = in_df

    # cross-reference cols against dictionary
    matched_meta = match_columns(df=pheno_df, scale_metadata=scale_metadata)

    return pheno_df, matched_meta


def main():

    # set up directories
    # -----------------
    repo_dir = '/home/ubuntu/Projects/neumora_datasets'

    # is row=1 of the input csv an extra header?
    # -----------------
    extra_header = True

    # path to scales and item dictionaries
    # -----------------
    scale    = 'qids01'
    def_file = os.path.join(repo_dir, 'ref_files/scale_dicts', f'{scale}_definitions.csv')
    s3_path  = '/fmri-qunex/research/imaging/datasets/STARD/raw-data/clinical/stard_r1.0.0/phenotype/qids01.tsv'

    # Fetch dataframe and match to metadata
    # -----------------
    pheno_df, matched_meta = fetch_dataframe(s3_path=s3_path, 
                                             def_file=def_file, 
                                             extra_header=extra_header)


if __name__ == '__main__':
    main()












