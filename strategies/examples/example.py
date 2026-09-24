"""
Example OpenAlgo Strategy
This is a minimal example showing how to use the OpenAlgo Python SDK.
"""

import os
import time
from openalgo import api
from dotenv import load_dotenv
# Load the .env file into os.environ
load_dotenv()

# Get API key from environment variable
API_KEY = os.getenv('OPENALGO_API_KEY')

# Initialize the API client
client = api(
    api_key=API_KEY,
    host_url="http://127.0.0.1:5000"
)

def main():
    """Main strategy logic"""
    print("Strategy started")

    # Example: Get account funds
    funds = client.funds()
    print(f"Available funds: {funds}")

    # Your trading logic here
    while True:
        # Check market conditions
        # Place orders based on your strategy
        # ...

        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    main()
