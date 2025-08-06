# Telegram Group Market Bot

A secure marketplace for buying and selling Telegram groups with automated ownership transfer using Telethon userbots.

## Features

### 🏪 **Core Marketplace**
- Browse groups by creation year/month
- Secure group purchasing with USDT payments
- Automated group ownership transfer
- Real-time inventory management

### 💰 **Payment System**
- USDT balance management via @cctip_bot
- Automatic tip detection and balance updates
- Secure withdrawal system to Polygon addresses
- Admin balance management tools

### 🤖 **Userbot Integration**
- Multiple userbot session management
- 2FA protected session storage
- Automated group ownership verification
- Session import/export functionality

### 🔐 **Security Features**
- 2FA required for all userbot sessions
- Phone number uniqueness validation
- Secure session encryption and storage
- Admin-only sensitive operations

### 📊 **Admin Panel**
- User statistics and management
- Session management interface
- Withdrawal request handling
- Balance adjustment tools

## Installation

### Prerequisites
- Python 3.8 or higher
- Telegram Bot Token (from @BotFather)
- Telegram API credentials (from https://my.telegram.org)

### Setup

1. **Clone the repository:**
```bash
git clone <repository-url>
cd telegram-group-market-bot
```

2. **Install dependencies:**
```bash
pip install -r requirements.txt
```

3. **Configure the bot:**
Edit `config.py` with your settings:
```python
BOT_TOKEN = "your_bot_token_here"
BOT_OWNERS = [your_user_id]  # Get from @userinfobot
BANK_GROUP_ID = -1000000000000  # Your payment group ID
```

4. **Run the bot:**
```bash
python main.py
```

## Configuration

### Bot Settings (`config.py`)
- `BOT_TOKEN`: Your Telegram bot token
- `BOT_OWNERS`: List of admin user IDs
- `BANK_GROUP_ID`: Group ID where payments are processed
- `CCTIP_BOT_ID`: CCTip bot user ID (7047032618)

### Market Settings
- `MIN_PRICE`: Minimum group price ($0.01)
- `MAX_PRICE`: Maximum group price ($99.99)
- `MIN_GROUP_MESSAGES`: Minimum messages required (4)
- `LISTING_TIMEOUT`: Userbot addition timeout (5 minutes)

## User Guide

### 🛒 **Buying Groups**

1. **Browse Market:**
   ```
   /market
   ```
   - Select year → month → price range
   - View available groups with buying IDs

2. **Purchase Groups:**
   ```
   /buy G123ABC
   /buy G123ABC, G456DEF  # Multiple groups
   ```

3. **Claim Ownership:**
   - Join the group using provided invite link
   - Type `/claim` in the group
   - Ownership will be transferred automatically

### 💰 **Managing Balance**

1. **Check Balance:**
   ```
   /balance
   ```

2. **Add Funds:**
   - Send USDT via @cctip_bot in the designated bank group
   - Balance updates automatically

3. **Withdraw Funds:**
   ```
   /withdraw
   ```
   - Enter amount and Polygon address
   - Admin approval required

### 📋 **Selling Groups**

1. **List Group for Sale:**
   - Go to your private supergroup
   - Type `/list`
   - Set price and add userbot as admin
   - Confirm listing

2. **Manage Listings:**
   ```
   /cprice 25.50     # Change price
   /refund           # Remove listing and get refund
   ```

## Admin Guide

### 🤖 **Session Management**

1. **Add Userbot Session:**
   ```
   /add
   ```
   - Provide API ID, API Hash, phone number
   - Enter OTP and 2FA password
   - Session stored securely

2. **Import Session File:**
   - Send .session file to bot
   - Enter 2FA password if required

### 👥 **User Management**

1. **View Users:**
   ```
   /users [page]
   ```

2. **Adjust Balance:**
   ```
   /add_bal <user_id> <amount>
   /add_bal 123456789 10.50     # Add $10.50
   /add_bal 123456789 -5.00     # Deduct $5.00
   ```

### 📊 **System Commands**
```
/ahelp          # Admin help
/import <type>  # Import data
/export <type>  # Export data
```

## Database Schema

### Core Tables
- **users**: User accounts and balances
- **sessions**: Userbot session data
- **groups**: Listed groups and metadata
- **transactions**: Payment and purchase history
- **withdrawal_requests**: Pending withdrawals
- **group_codes**: Permanent buying ID mappings

## Security Considerations

### 🔐 **Session Security**
- All sessions require 2FA enabled
- Session strings encrypted at rest
- Phone numbers cannot be reused
- Regular session validation

### 💰 **Payment Security**
- Only USDT payments accepted via @cctip_bot
- All transactions logged and auditable
- Withdrawal requests require admin approval
- Balance adjustments tracked

### 🛡️ **Group Transfer Security**
- Ownership verification before transfer
- User membership verification required
- Session authentication for all transfers
- Automatic rollback on failures

## API Integration

### CCTip Bot Integration
- Real-time tip detection in bank group
- Automatic balance updates
- USDT-only payment processing
- User tag verification

### Telethon Features
- Group metadata extraction
- Ownership verification
- Member participation checking
- Automated admin rights management

## Troubleshooting

### Common Issues

1. **Session Authentication Failed**
   - Verify 2FA is enabled on the account
   - Check API credentials are correct
   - Ensure phone number format is valid

2. **Group Transfer Failed**
   - Verify user has joined the group
   - Check userbot has admin rights
   - Confirm 2FA password is correct

3. **Payment Not Detected**
   - Ensure payment sent via @cctip_bot
   - Check payment was sent in correct group
   - Verify USDT currency was used

### Logs and Debugging
- Bot logs stored in `bot.log`
- Database operations logged
- Error messages sent to admins
- Session activities tracked

## File Structure

```
telegram-group-market-bot/
├── main.py                 # Main bot application
├── config.py              # Configuration settings
├── database.py            # Database management
├── session_handler.py     # Telethon session management
├── commands_features.py   # Bot commands and features
├── utils.py               # Utility functions
├── requirements.txt       # Python dependencies
├── README.md             # This file
├── sessions/             # Userbot session storage
└── telegram_market_bot.db # SQLite database
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For support and questions:
- Create an issue on GitHub
- Contact the bot administrators
- Check the troubleshooting section

## Disclaimer

This bot is for educational purposes. Users are responsible for complying with Telegram's Terms of Service and local laws. The developers are not responsible for any misuse of this software.

---

**⚠️ Important Security Notes:**
- Never share your bot token or API credentials
- Always use 2FA on userbot accounts
- Regularly backup your database
- Monitor bot logs for suspicious activity
- Keep the bot software updated