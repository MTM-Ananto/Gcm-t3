import re
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
from config import MIN_PRICE, MAX_PRICE, MIN_GROUP_MESSAGES

def validate_price(price_str: str) -> tuple[bool, float]:
    """Validate price input"""
    try:
        price = float(price_str)
        
        # Check range
        if price < MIN_PRICE or price > MAX_PRICE:
            return False, 0.0
        
        # Check decimal places (max 2)
        if '.' in price_str and len(price_str.split('.')[1]) > 2:
            return False, 0.0
        
        return True, price
    except ValueError:
        return False, 0.0

def validate_phone_number(phone: str) -> bool:
    """Validate phone number format"""
    # Remove any spaces, dashes, or plus signs
    clean_phone = re.sub(r'[^\d]', '', phone)
    
    # Should be between 10-15 digits
    return len(clean_phone) >= 10 and len(clean_phone) <= 15

def validate_api_credentials(api_id: str, api_hash: str) -> tuple[bool, int]:
    """Validate API credentials"""
    try:
        api_id_int = int(api_id)
        if api_id_int <= 0:
            return False, 0
        
        if not api_hash or len(api_hash) < 10:
            return False, 0
        
        return True, api_id_int
    except ValueError:
        return False, 0

def validate_buying_ids(buying_ids_str: str) -> List[str]:
    """Parse and validate buying IDs"""
    # Split by comma or space
    ids = re.split(r'[,\s]+', buying_ids_str.strip())
    
    valid_ids = []
    for id_str in ids:
        id_str = id_str.strip().upper()
        # Check format: G followed by 5-8 alphanumeric characters
        if re.match(r'^G[A-Z0-9]{5,8}$', id_str):
            valid_ids.append(id_str)
    
    return valid_ids

def validate_withdrawal_amount(amount_str: str, user_balance: float) -> tuple[bool, float]:
    """Validate withdrawal amount"""
    try:
        amount = float(amount_str)
        
        if amount <= 0:
            return False, 0.0
        
        if amount > user_balance:
            return False, 0.0
        
        # Check decimal places (max 2)
        if '.' in amount_str and len(amount_str.split('.')[1]) > 2:
            return False, 0.0
        
        return True, amount
    except ValueError:
        return False, 0.0

def validate_polygon_address(address: str) -> bool:
    """Validate Polygon address format"""
    if not address:
        return False
    
    # Check if it's a valid Ethereum/Polygon address (42 characters, starts with 0x)
    if re.match(r'^0x[a-fA-F0-9]{40}$', address):
        return True
    
    # Check if it's a cwallet ID (alphanumeric, 6-20 characters)
    if re.match(r'^[a-zA-Z0-9]{6,20}$', address):
        return True
    
    return False

def format_price(price: float) -> str:
    """Format price for display"""
    if price == int(price):
        return f"{int(price)}"
    else:
        return f"{price:.2f}"

def format_balance(balance: float) -> str:
    """Format balance for display"""
    return f"{balance:.2f}"

def format_user_link(user_id: int, username: str = None, first_name: str = None) -> str:
    """Format user link for display"""
    if username:
        return f"[{first_name or username}](https://t.me/{username})"
    else:
        return f"[{first_name or 'User'}](tg://user?id={user_id})"

def format_group_name(group_name: str, invite_link: str = None) -> str:
    """Format group name with link"""
    if invite_link:
        return f"[{group_name}]({invite_link})"
    else:
        return group_name

def format_buying_id(buying_id: str) -> str:
    """Format buying ID in monospace"""
    return f"`{buying_id}`"

def parse_tip_message(message_text: str) -> Optional[Dict[str, Any]]:
    """Parse cctip bot tip message"""
    try:
        # Look for tip patterns in the message
        # Example: "💰 @username tipped @recipient 5.0 USDT"
        
        patterns = [
            r'💰.*?tipped.*?(\d+\.?\d*)\s*USDT',
            r'tip.*?(\d+\.?\d*)\s*USDT',
            r'(\d+\.?\d*)\s*USDT.*?tip'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, message_text, re.IGNORECASE)
            if match:
                amount = float(match.group(1))
                return {
                    'amount': amount,
                    'currency': 'USDT',
                    'valid': True
                }
        
        return None
    except Exception:
        return None

def create_market_keyboard(years: List[int], current_page: int = 0, per_page: int = 5):
    """Create inline keyboard for market years"""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    
    start_idx = current_page * per_page
    end_idx = start_idx + per_page
    page_years = years[start_idx:end_idx]
    
    keyboard = []
    
    # Year buttons (2 per row)
    for i in range(0, len(page_years), 2):
        row = []
        for j in range(2):
            if i + j < len(page_years):
                year = page_years[i + j]
                row.append(InlineKeyboardButton(str(year), callback_data=f"year_{year}"))
        keyboard.append(row)
    
    # Navigation buttons
    nav_row = []
    if current_page > 0:
        nav_row.append(InlineKeyboardButton("◀ Previous", callback_data=f"market_page_{current_page-1}"))
    
    if end_idx < len(years):
        nav_row.append(InlineKeyboardButton("Next ▶", callback_data=f"market_page_{current_page+1}"))
    
    if nav_row:
        keyboard.append(nav_row)
    
    return InlineKeyboardMarkup(keyboard)

def create_month_keyboard(year: int):
    """Create inline keyboard for months"""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    
    months = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]
    
    keyboard = []
    
    # Month buttons (3 per row)
    for i in range(0, 12, 3):
        row = []
        for j in range(3):
            if i + j < 12:
                month_num = i + j + 1
                month_name = months[i + j]
                row.append(InlineKeyboardButton(month_name, callback_data=f"month_{year}_{month_num}"))
        keyboard.append(row)
    
    # Back button
    keyboard.append([InlineKeyboardButton("◀ Back", callback_data="market_back")])
    
    return InlineKeyboardMarkup(keyboard)

def create_groups_keyboard(groups: List[Dict], current_page: int = 0, per_page: int = 10):
    """Create inline keyboard for groups by price"""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    
    # Group by price
    price_groups = {}
    for group in groups:
        price = group['price']
        if price not in price_groups:
            price_groups[price] = []
        price_groups[price].append(group)
    
    # Sort by price
    sorted_prices = sorted(price_groups.keys())
    
    start_idx = current_page * per_page
    end_idx = start_idx + per_page
    page_prices = sorted_prices[start_idx:end_idx]
    
    keyboard = []
    
    # Price group buttons
    for i, price in enumerate(page_prices, start=1):
        quantity = len(price_groups[price])
        rate = format_price(price)
        button_text = f"{i}. Quantity: {quantity} | Rate: ${rate}"
        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"price_{price}")])
    
    # Navigation buttons
    nav_row = []
    if current_page > 0:
        nav_row.append(InlineKeyboardButton("◀ Previous", callback_data=f"groups_page_{current_page-1}"))
    
    if end_idx < len(sorted_prices):
        nav_row.append(InlineKeyboardButton("Next ▶", callback_data=f"groups_page_{current_page+1}"))
    
    if nav_row:
        keyboard.append(nav_row)
    
    # Back button
    keyboard.append([InlineKeyboardButton("◀ Back", callback_data="groups_back")])
    
    return InlineKeyboardMarkup(keyboard)

def create_group_list_keyboard(groups: List[Dict], current_page: int = 0, per_page: int = 5):
    """Create inline keyboard for individual groups"""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    
    start_idx = current_page * per_page
    end_idx = start_idx + per_page
    page_groups = groups[start_idx:end_idx]
    
    keyboard = []
    
    # Group buttons
    for group in page_groups:
        group_name = group['group_name'][:30] + "..." if len(group['group_name']) > 30 else group['group_name']
        buying_id = group['buying_id']
        button_text = f"{group_name} ({buying_id})"
        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"group_{buying_id}")])
    
    # Navigation buttons
    nav_row = []
    if current_page > 0:
        nav_row.append(InlineKeyboardButton("◀ Previous", callback_data=f"grouplist_page_{current_page-1}"))
    
    if end_idx < len(groups):
        nav_row.append(InlineKeyboardButton("Next ▶", callback_data=f"grouplist_page_{current_page+1}"))
    
    if nav_row:
        keyboard.append(nav_row)
    
    # Back button
    keyboard.append([InlineKeyboardButton("◀ Back", callback_data="grouplist_back")])
    
    return InlineKeyboardMarkup(keyboard)

def create_confirmation_keyboard(action: str, data: str = ""):
    """Create confirmation keyboard"""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    
    keyboard = [
        [
            InlineKeyboardButton("✅ Confirm", callback_data=f"confirm_{action}_{data}"),
            InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_{action}_{data}")
        ]
    ]
    
    if action == "listing":
        keyboard.append([InlineKeyboardButton("🔄 Refresh", callback_data=f"refresh_{action}_{data}")])
    
    return InlineKeyboardMarkup(keyboard)

def create_users_keyboard(users: List[Dict], current_page: int = 0, per_page: int = 10):
    """Create keyboard for users list (admin)"""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    
    keyboard = []
    
    # Navigation buttons
    nav_row = []
    if current_page > 0:
        nav_row.append(InlineKeyboardButton("◀ Previous", callback_data=f"users_page_{current_page-1}"))
    
    # Check if there are more users
    if len(users) == per_page:  # Assume there might be more if we got a full page
        nav_row.append(InlineKeyboardButton("Next ▶", callback_data=f"users_page_{current_page+1}"))
    
    if nav_row:
        keyboard.append(nav_row)
    
    return InlineKeyboardMarkup(keyboard)

def escape_markdown(text: str) -> str:
    """Escape markdown special characters"""
    special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    for char in special_chars:
        text = text.replace(char, f'\\{char}')
    return text

def truncate_text(text: str, max_length: int = 100) -> str:
    """Truncate text to maximum length"""
    if len(text) <= max_length:
        return text
    return text[:max_length-3] + "..."

def format_datetime(dt: datetime) -> str:
    """Format datetime for display"""
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def get_available_years() -> List[int]:
    """Get available years for market (2016-2025)"""
    current_year = datetime.now().year
    return list(range(2016, current_year + 2))  # Include next year

def chunk_list(lst: List, chunk_size: int) -> List[List]:
    """Split list into chunks"""
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

def is_group_valid_for_listing(group_info: Dict) -> tuple[bool, str]:
    """Check if group is valid for listing"""
    if not group_info:
        return False, "Could not get group information"
    
    # Check if private
    if not group_info.get('is_private', True):
        return False, "Group must be private"
    
    # Check if supergroup
    if not group_info.get('is_megagroup', False):
        return False, "Group must be a supergroup"
    
    # Check creation date visibility
    if not group_info.get('creation_date'):
        return False, "Group creation date is not visible"
    
    # Check message count
    if group_info.get('total_messages', 0) < MIN_GROUP_MESSAGES:
        return False, f"Group must have at least {MIN_GROUP_MESSAGES} messages"
    
    return True, "Group is valid for listing"

def generate_help_text() -> str:
    """Generate user help text"""
    return """
🤖 **Telegram Group Market Bot**

**📱 Available Commands:**

🏪 **Market Commands:**
• `/market` - Browse groups by year/month
• `/buy <buying_id>` - Purchase groups (e.g., `/buy G123ABC` or `/buy G123ABC, G456DEF`)
• `/claim` - Claim purchased groups (use in the group after joining)

💰 **Balance Commands:**
• `/balance` - Check your current balance
• `/withdraw` - Withdraw funds to Polygon/CWallet

📋 **Listing Commands:**
• `/list` - List your group for sale (use in the group you own)
• `/refund` - Get refund for listed group (use in the group)
• `/cprice <price>` - Change group price (use in the group)

❓ **Help:**
• `/help` - Show this help message

**💳 Adding Balance:**
To add balance to your account, send USDT via @cctip_bot in the designated bank group.

**🛒 How to Buy:**
1. Use `/market` to browse available groups
2. Find groups you want to buy and note their buying IDs
3. Use `/buy <buying_id>` to purchase
4. Join the group using the provided invite link
5. Type `/claim` in the group to transfer ownership

**💡 How to Sell:**
1. Go to your private supergroup that you own
2. Type `/list` and follow the instructions
3. Set a price and add the bot's userbot as admin with full rights
4. Wait for buyers!

**⚠️ Important Notes:**
• Only private supergroups can be listed
• Groups must have at least 4 messages
• Group creation date must be visible
• You must be the owner to list a group
• Transfers require 2FA to be enabled on userbot accounts

Need more help? Contact the bot administrators.
"""

def generate_admin_help_text() -> str:
    """Generate admin help text"""
    return """
🔧 **Admin Commands:**

**👥 User Management:**
• `/users` - View all users and their statistics
• `/add_bal <user_id> <amount>` - Add/remove balance from user

**🤖 Session Management:**
• `/add` - Add new userbot session
• `/add_bank` - Add bank userbot for payment processing
• `/import <type>` - Import bot data or sessions
• `/export <type>` - Export bot data or sessions

**📊 System Commands:**
• `/ahelp` - Show this admin help

**💳 Withdrawal Management:**
Withdrawal requests are automatically sent to admins for approval.

**🔐 Session Security:**
• Sessions require 2FA to be enabled
• Phone numbers cannot be reused
• Sessions are encrypted and stored securely

**⚙️ Bot Configuration:**
• Bot owners are defined in config.py
• Bank group ID is configurable
• All settings can be modified in the config file
"""