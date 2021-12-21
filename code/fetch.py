import os
import glob
from pathlib import Path



def fetch_dataframe(s3_path, definition):

    # get your credentials from environment variables
    aws_id = os.environ['AWS_ID']
    aws_secret = os.environ['AWS_SECRET']

    s3_path = '/fmri-qunex/research/imaging/datasets/STARD/raw-data/clinical/stard_r1.0.0/phenotype/stard_r1.0.0/phenotype/qids01.tsv'