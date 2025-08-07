import os

# Bot Configuration
BOT_TOKEN = "7419563941:AAGimZB_uPdM7mJH1QAAmRf0nov0aw4eFE8"
BOT_OWNERS = [2020690884]  # Extendable list
CCTIP_BOT_USERNAME = "@cctip_bot"
CCTIP_BOT_ID = 7047032618  # cctip bot user ID
BANK_GROUP_ID = -1002770449778

# Database Configuration
DATABASE_URL = "sqlite:///telegram_market_bot.db"

# Session Configuration
SESSIONS_DIR = "sessions"
MAX_SESSIONS_PER_USER = 5

# Market Configuration
MIN_GROUP_MESSAGES = 4
MIN_PRICE = 0.01
MAX_PRICE = 99.99
LISTING_TIMEOUT = 300  # 5 minutes
GROUPS_PER_PAGE = 10
USERS_PER_PAGE = 10

# Withdrawal Configuration
MIN_WITHDRAWAL = 1.0

# Fee Configuration
BUYING_FEE_RATE = 0.005  # 0.5% buying fee
SELLING_FEE_RATE = 0.005  # 0.5% selling fee

# Referral Configuration
REFERRAL_COMMISSION_RATE = 0.10  # 10% of fees earned by referrals

# Create sessions directory if it doesn't exist
if not os.path.exists(SESSIONS_DIR):
    os.makedirs(SESSIONS_DIR)