# config/aws_config.py
# Reads credentials from .env file
# Never hardcode secrets in code!

import os
from dotenv import load_dotenv

load_dotenv()  # reads .env file automatically

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_REGION     = os.getenv("AWS_REGION")
S3_BUCKET      = os.getenv("S3_BUCKET")

# S3 folder structure
S3_RAW_PREFIX  = "raw/"
S3_CLEAN_PREFIX = "clean/"
S3_ML_PREFIX   = "ml-outputs/"