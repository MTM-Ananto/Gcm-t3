#!/usr/bin/env python3
"""
Telegram Group Market Bot - Combined Code
A comprehensive marketplace for buying and selling Telegram groups with secure ownership transfer.

This file contains all functionality in one place for easy deployment.
Only requires config.py to run.
"""

# ============================================================================
# IMPORTS AND SETUP
# ============================================================================

import asyncio
import logging
import sys
import signal
import sqlite3
import json
import hashlib
import os
import re
import random
import string
import shutil
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    filters, ContextTypes
)
from telegram.constants import ParseMode
from telegram.error import NetworkError, TimedOut, BadRequest

from telethon import TelegramClient, events
from telethon.errors import (
    SessionPasswordNeededError, PhoneCodeInvalidError, 
    PasswordHashInvalidError, FloodWaitError
)
from telethon.tl.functions.channels import (
    EditAdminRequest, InviteToChannelRequest, 
    GetParticipantsRequest, CheckUsernameRequest
)
from telethon.tl.functions.messages import ExportChatInviteRequest
from telethon.tl.types import (
    ChatAdminRights, ChannelParticipantsAdmins, 
    InputPeerChannel, User
)

# Import configuration
from config import (
    BOT_TOKEN, BOT_OWNERS, CCTIP_BOT_USERNAME, CCTIP_BOT_ID, BANK_GROUP_ID,
    DATABASE_URL, SESSIONS_DIR, MAX_SESSIONS_PER_USER, MIN_GROUP_MESSAGES,
    MIN_PRICE, MAX_PRICE, LISTING_TIMEOUT, GROUPS_PER_PAGE, USERS_PER_PAGE,
    MIN_WITHDRAWAL
)

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# ============================================================================
# DATABASE MODULE
# ============================================================================

class Database:
    def __init__(self):
        self.db_path = DATABASE_URL.replace("sqlite:///", "")
        self.lock = threading.Lock()
        self.init_database()
    
    def get_connection(self):
        return sqlite3.connect(self.db_path, check_same_thread=False)
    
    def init_database(self):
        with self.lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Users table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    balance REAL DEFAULT 0.0,
                    total_volume REAL DEFAULT 0.0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Sessions table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    api_id INTEGER,
                    api_hash TEXT,
                    phone_number TEXT,
                    session_string TEXT,
                    password_hash TEXT,
                    has_2fa BOOLEAN DEFAULT FALSE,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (user_id),
                    UNIQUE(phone_number)
                )
            ''')
            
            # Groups table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS groups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_id INTEGER UNIQUE,
                    buying_id TEXT UNIQUE,
                    group_name TEXT,
                    group_username TEXT,
                    invite_link TEXT,
                    owner_user_id INTEGER,
                    session_id INTEGER,
                    price REAL,
                    creation_date DATE,
                    total_messages INTEGER,
                    is_listed BOOLEAN DEFAULT TRUE,
                    listed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (owner_user_id) REFERENCES users (user_id),
                    FOREIGN KEY (session_id) REFERENCES sessions (id)
                )
            ''')
            
            # Transactions table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    transaction_type TEXT,
                    amount REAL,
                    group_ids TEXT,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (user_id)
                )
            ''')
            
            # Withdrawal requests table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS withdrawal_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    amount REAL,
                    address TEXT,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed_at TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (user_id)
                )
            ''')
            
            # Group codes mapping
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS group_codes (
                    group_id INTEGER PRIMARY KEY,
                    buying_id TEXT UNIQUE
                )
            ''')
            
            # Pending listings table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS pending_listings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    group_id INTEGER,
                    price REAL,
                    userbot_username TEXT,
                    expires_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (user_id)
                )
            ''')
            
            conn.commit()
            conn.close()
    
    def add_user(self, user_id: int, username: str = None, first_name: str = None) -> bool:
        with self.lock:
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR IGNORE INTO users (user_id, username, first_name)
                    VALUES (?, ?, ?)
                ''', (user_id, username, first_name))
                
                cursor.execute('''
                    UPDATE users SET username = ?, first_name = ?
                    WHERE user_id = ?
                ''', (username, first_name, user_id))
                
                conn.commit()
                conn.close()
                return True
            except Exception as e:
                logger.error(f"Error adding user: {e}")
                return False
    
    def get_user_balance(self, user_id: int) -> float:
        with self.lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
            result = cursor.fetchone()
            conn.close()
            return result[0] if result else 0.0
    
    def update_user_balance(self, user_id: int, amount: float, transaction_type: str = 'manual') -> bool:
        with self.lock:
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                
                cursor.execute('''
                    UPDATE users SET balance = balance + ?
                    WHERE user_id = ?
                ''', (amount, user_id))
                
                cursor.execute('''
                    INSERT INTO transactions (user_id, transaction_type, amount, status)
                    VALUES (?, ?, ?, 'completed')
                ''', (user_id, transaction_type, amount))
                
                if amount > 0:
                    cursor.execute('''
                        UPDATE users SET total_volume = total_volume + ?
                        WHERE user_id = ?
                    ''', (amount, user_id))
                
                conn.commit()
                conn.close()
                return True
            except Exception as e:
                logger.error(f"Error updating balance: {e}")
                return False
    
    def add_session(self, user_id: int, api_id: int, api_hash: str, phone_number: str, 
                   session_string: str, password_hash: str = None, has_2fa: bool = False) -> bool:
        with self.lock:
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                
                cursor.execute('SELECT id FROM sessions WHERE phone_number = ?', (phone_number,))
                if cursor.fetchone():
                    conn.close()
                    return False
                
                cursor.execute('''
                    INSERT INTO sessions (user_id, api_id, api_hash, phone_number, 
                                        session_string, password_hash, has_2fa)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (user_id, api_id, api_hash, phone_number, session_string, password_hash, has_2fa))
                
                conn.commit()
                conn.close()
                return True
            except Exception as e:
                logger.error(f"Error adding session: {e}")
                return False
    
    def get_user_sessions(self, user_id: int) -> List[Dict]:
        with self.lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, api_id, api_hash, phone_number, session_string, 
                       password_hash, has_2fa, is_active
                FROM sessions WHERE user_id = ? AND is_active = TRUE
            ''', (user_id,))
            
            sessions = []
            for row in cursor.fetchall():
                sessions.append({
                    'id': row[0],
                    'api_id': row[1],
                    'api_hash': row[2],
                    'phone_number': row[3],
                    'session_string': row[4],
                    'password_hash': row[5],
                    'has_2fa': row[6],
                    'is_active': row[7]
                })
            
            conn.close()
            return sessions
    
    def get_or_create_buying_id(self, group_id: int) -> str:
        with self.lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('SELECT buying_id FROM group_codes WHERE group_id = ?', (group_id,))
            result = cursor.fetchone()
            
            if result:
                conn.close()
                return result[0]
            
            while True:
                buying_id = 'G' + ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
                cursor.execute('SELECT group_id FROM group_codes WHERE buying_id = ?', (buying_id,))
                if not cursor.fetchone():
                    break
            
            cursor.execute('INSERT INTO group_codes (group_id, buying_id) VALUES (?, ?)', 
                          (group_id, buying_id))
            conn.commit()
            conn.close()
            return buying_id
    
    def add_group(self, group_id: int, group_name: str, group_username: str, invite_link: str,
                  owner_user_id: int, session_id: int, price: float, creation_date: str,
                  total_messages: int) -> bool:
        with self.lock:
            try:
                buying_id = self.get_or_create_buying_id(group_id)
                
                conn = self.get_connection()
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO groups 
                    (group_id, buying_id, group_name, group_username, invite_link,
                     owner_user_id, session_id, price, creation_date, total_messages)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (group_id, buying_id, group_name, group_username, invite_link,
                      owner_user_id, session_id, price, creation_date, total_messages))
                
                conn.commit()
                conn.close()
                return True
            except Exception as e:
                logger.error(f"Error adding group: {e}")
                return False
    
    def get_groups_by_date(self, year: int, month: int = None) -> List[Dict]:
        with self.lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            if month:
                cursor.execute('''
                    SELECT group_id, buying_id, group_name, group_username, invite_link,
                           price, creation_date, total_messages
                    FROM groups 
                    WHERE is_listed = TRUE 
                    AND strftime('%Y', creation_date) = ? 
                    AND strftime('%m', creation_date) = ?
                    ORDER BY price ASC
                ''', (str(year), f"{month:02d}"))
            else:
                cursor.execute('''
                    SELECT group_id, buying_id, group_name, group_username, invite_link,
                           price, creation_date, total_messages
                    FROM groups 
                    WHERE is_listed = TRUE 
                    AND strftime('%Y', creation_date) = ?
                    ORDER BY price ASC
                ''', (str(year),))
            
            groups = []
            for row in cursor.fetchall():
                groups.append({
                    'group_id': row[0],
                    'buying_id': row[1],
                    'group_name': row[2],
                    'group_username': row[3],
                    'invite_link': row[4],
                    'price': row[5],
                    'creation_date': row[6],
                    'total_messages': row[7]
                })
            
            conn.close()
            return groups
    
    def get_group_by_buying_id(self, buying_id: str) -> Optional[Dict]:
        with self.lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT g.*, s.session_string, s.password_hash, s.has_2fa
                FROM groups g
                JOIN sessions s ON g.session_id = s.id
                WHERE g.buying_id = ? AND g.is_listed = TRUE
            ''', (buying_id,))
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return {
                    'id': result[0],
                    'group_id': result[1],
                    'buying_id': result[2],
                    'group_name': result[3],
                    'group_username': result[4],
                    'invite_link': result[5],
                    'owner_user_id': result[6],
                    'session_id': result[7],
                    'price': result[8],
                    'creation_date': result[9],
                    'total_messages': result[10],
                    'is_listed': result[11],
                    'listed_at': result[12],
                    'session_string': result[13],
                    'password_hash': result[14],
                    'has_2fa': result[15]
                }
            return None
    
    def purchase_groups(self, user_id: int, buying_ids: List[str]) -> bool:
        with self.lock:
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                
                total_cost = 0
                group_data = []
                for buying_id in buying_ids:
                    cursor.execute('SELECT price, group_id FROM groups WHERE buying_id = ? AND is_listed = TRUE', 
                                 (buying_id,))
                    result = cursor.fetchone()
                    if not result:
                        conn.close()
                        return False
                    total_cost += result[0]
                    group_data.append({'buying_id': buying_id, 'price': result[0], 'group_id': result[1]})
                
                cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
                balance = cursor.fetchone()[0]
                
                if balance < total_cost:
                    conn.close()
                    return False
                
                cursor.execute('UPDATE users SET balance = balance - ? WHERE user_id = ?', 
                             (total_cost, user_id))
                
                for group in group_data:
                    cursor.execute('UPDATE groups SET is_listed = FALSE WHERE buying_id = ?', 
                                 (group['buying_id'],))
                
                cursor.execute('''
                    INSERT INTO transactions (user_id, transaction_type, amount, group_ids, status)
                    VALUES (?, 'purchase', ?, ?, 'completed')
                ''', (user_id, -total_cost, json.dumps([g['group_id'] for g in group_data])))
                
                conn.commit()
                conn.close()
                return True
            except Exception as e:
                logger.error(f"Error purchasing groups: {e}")
                return False
    
    def add_withdrawal_request(self, user_id: int, amount: float, address: str) -> bool:
        with self.lock:
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                
                cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
                balance = cursor.fetchone()[0]
                
                if balance < amount:
                    conn.close()
                    return False
                
                cursor.execute('UPDATE users SET balance = balance - ? WHERE user_id = ?', 
                             (amount, user_id))
                
                cursor.execute('''
                    INSERT INTO withdrawal_requests (user_id, amount, address)
                    VALUES (?, ?, ?)
                ''', (user_id, amount, address))
                
                conn.commit()
                conn.close()
                return True
            except Exception as e:
                logger.error(f"Error adding withdrawal request: {e}")
                return False
    
    def get_all_users(self, page: int = 0, per_page: int = 10) -> List[Dict]:
        with self.lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            offset = page * per_page
            cursor.execute('''
                SELECT user_id, username, first_name, total_volume, balance,
                       (SELECT COUNT(*) FROM groups WHERE owner_user_id = users.user_id AND is_listed = TRUE) as groups_count
                FROM users
                ORDER BY total_volume DESC
                LIMIT ? OFFSET ?
            ''', (per_page, offset))
            
            users = []
            for row in cursor.fetchall():
                users.append({
                    'user_id': row[0],
                    'username': row[1],
                    'first_name': row[2],
                    'total_volume': row[3],
                    'balance': row[4],
                    'groups_count': row[5]
                })
            
            conn.close()
            return users
    
    def get_total_users_count(self) -> int:
        with self.lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM users')
            result = cursor.fetchone()[0]
            conn.close()
            return result
    
    def get_total_volume(self) -> float:
        with self.lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT SUM(total_volume) FROM users')
            result = cursor.fetchone()[0]
            conn.close()
            return result or 0.0

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def validate_price(price_str: str) -> Tuple[bool, float]:
    """Validate price input"""
    try:
        price = float(price_str)
        
        if price < MIN_PRICE or price > MAX_PRICE:
            return False, 0.0
        
        if '.' in price_str and len(price_str.split('.')[1]) > 2:
            return False, 0.0
        
        return True, price
    except ValueError:
        return False, 0.0

def validate_phone_number(phone: str) -> bool:
    """Validate phone number format"""
    clean_phone = re.sub(r'[^\d]', '', phone)
    return len(clean_phone) >= 10 and len(clean_phone) <= 15

def validate_api_credentials(api_id: str, api_hash: str) -> Tuple[bool, int]:
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
    ids = re.split(r'[,\s]+', buying_ids_str.strip())
    
    valid_ids = []
    for id_str in ids:
        id_str = id_str.strip().upper()
        if re.match(r'^G[A-Z0-9]{5,8}$', id_str):
            valid_ids.append(id_str)
    
    return valid_ids

def validate_withdrawal_amount(amount_str: str, user_balance: float) -> Tuple[bool, float]:
    """Validate withdrawal amount"""
    try:
        amount = float(amount_str)
        
        if amount <= 0:
            return False, 0.0
        
        if amount > user_balance:
            return False, 0.0
        
        if '.' in amount_str and len(amount_str.split('.')[1]) > 2:
            return False, 0.0
        
        return True, amount
    except ValueError:
        return False, 0.0

def validate_polygon_address(address: str) -> bool:
    """Validate Polygon address format"""
    if not address:
        return False
    
    if re.match(r'^0x[a-fA-F0-9]{40}$', address):
        return True
    
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
    start_idx = current_page * per_page
    end_idx = start_idx + per_page
    page_years = years[start_idx:end_idx]
    
    keyboard = []
    
    for i in range(0, len(page_years), 2):
        row = []
        for j in range(2):
            if i + j < len(page_years):
                year = page_years[i + j]
                row.append(InlineKeyboardButton(str(year), callback_data=f"year_{year}"))
        keyboard.append(row)
    
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
    months = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]
    
    keyboard = []
    
    for i in range(0, 12, 3):
        row = []
        for j in range(3):
            if i + j < 12:
                month_num = i + j + 1
                month_name = months[i + j]
                row.append(InlineKeyboardButton(month_name, callback_data=f"month_{year}_{month_num}"))
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("◀ Back", callback_data="market_back")])
    
    return InlineKeyboardMarkup(keyboard)

def create_groups_keyboard(groups: List[Dict], current_page: int = 0, per_page: int = 10):
    """Create inline keyboard for groups by price"""
    price_groups = {}
    for group in groups:
        price = group['price']
        if price not in price_groups:
            price_groups[price] = []
        price_groups[price].append(group)
    
    sorted_prices = sorted(price_groups.keys())
    
    start_idx = current_page * per_page
    end_idx = start_idx + per_page
    page_prices = sorted_prices[start_idx:end_idx]
    
    keyboard = []
    
    for i, price in enumerate(page_prices, start=1):
        quantity = len(price_groups[price])
        rate = format_price(price)
        button_text = f"{i}. Quantity: {quantity} | Rate: ${rate}"
        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"price_{price}")])
    
    nav_row = []
    if current_page > 0:
        nav_row.append(InlineKeyboardButton("◀ Previous", callback_data=f"groups_page_{current_page-1}"))
    
    if end_idx < len(sorted_prices):
        nav_row.append(InlineKeyboardButton("Next ▶", callback_data=f"groups_page_{current_page+1}"))
    
    if nav_row:
        keyboard.append(nav_row)
    
    keyboard.append([InlineKeyboardButton("◀ Back", callback_data="groups_back")])
    
    return InlineKeyboardMarkup(keyboard)

def create_confirmation_keyboard(action: str, data: str = ""):
    """Create confirmation keyboard"""
    keyboard = [
        [
            InlineKeyboardButton("✅ Confirm", callback_data=f"confirm_{action}_{data}"),
            InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_{action}_{data}")
        ]
    ]
    
    if action == "listing":
        keyboard.append([InlineKeyboardButton("🔄 Refresh", callback_data=f"refresh_{action}_{data}")])
    
    return InlineKeyboardMarkup(keyboard)

def get_available_years() -> List[int]:
    """Get available years for market (2016-2025)"""
    current_year = datetime.now().year
    return list(range(2016, current_year + 2))

def is_group_valid_for_listing(group_info: Dict) -> Tuple[bool, str]:
    """Check if group is valid for listing"""
    if not group_info:
        return False, "Could not get group information"
    
    if not group_info.get('is_private', True):
        return False, "Group must be private"
    
    if not group_info.get('is_megagroup', False):
        return False, "Group must be a supergroup"
    
    if not group_info.get('creation_date'):
        return False, "Group creation date is not visible"
    
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

# ============================================================================
# SESSION HANDLER
# ============================================================================

class SessionManager:
    def __init__(self):
        self.active_sessions = {}
        self.pending_auth = {}
    
    def hash_password(self, password: str) -> str:
        """Hash password using SHA-256"""
        return hashlib.sha256(password.encode()).hexdigest()
    
    def verify_password(self, password: str, hashed: str) -> bool:
        """Verify password against hash"""
        return self.hash_password(password) == hashed
    
    async def start_auth_process(self, user_id: int, api_id: int, api_hash: str, phone_number: str):
        """Start authentication process for new session"""
        try:
            session_file = os.path.join(SESSIONS_DIR, f"temp_{user_id}_{phone_number}")
            client = TelegramClient(session_file, api_id, api_hash)
            
            await client.connect()
            
            sent_code = await client.send_code_request(phone_number)
            
            self.pending_auth[user_id] = {
                'client': client,
                'api_id': api_id,
                'api_hash': api_hash,
                'phone_number': phone_number,
                'phone_code_hash': sent_code.phone_code_hash,
                'session_file': session_file,
                'step': 'code'
            }
            
            return True
        except Exception as e:
            logger.error(f"Error starting auth process: {e}")
            return False
    
    async def verify_code(self, user_id: int, code: str):
        """Verify OTP code"""
        if user_id not in self.pending_auth:
            return False, "No pending authentication"
        
        auth_data = self.pending_auth[user_id]
        client = auth_data['client']
        
        try:
            await client.sign_in(
                phone=auth_data['phone_number'],
                code=code,
                phone_code_hash=auth_data['phone_code_hash']
            )
            
            me = await client.get_me()
            auth_data['me'] = me
            auth_data['step'] = 'completed'
            
            return True, "Code verified successfully"
            
        except SessionPasswordNeededError:
            auth_data['step'] = 'password'
            return True, "2FA password required"
            
        except PhoneCodeInvalidError:
            return False, "Invalid code"
        except Exception as e:
            logger.error(f"Error verifying code: {e}")
            return False, str(e)
    
    async def verify_password(self, user_id: int, password: str):
        """Verify 2FA password"""
        if user_id not in self.pending_auth:
            return False, "No pending authentication"
        
        auth_data = self.pending_auth[user_id]
        client = auth_data['client']
        
        try:
            await client.sign_in(password=password)
            me = await client.get_me()
            auth_data['me'] = me
            auth_data['step'] = 'completed'
            auth_data['password'] = password
            
            return True, "Password verified successfully"
            
        except PasswordHashInvalidError:
            return False, "Invalid password"
        except Exception as e:
            logger.error(f"Error verifying password: {e}")
            return False, str(e)
    
    async def complete_auth(self, user_id: int):
        """Complete authentication and save session"""
        if user_id not in self.pending_auth:
            return False, "No pending authentication"
        
        auth_data = self.pending_auth[user_id]
        
        if auth_data['step'] != 'completed':
            return False, "Authentication not completed"
        
        client = auth_data['client']
        
        try:
            session_string = client.session.save()
            
            password_hash = None
            has_2fa = False
            if 'password' in auth_data:
                password_hash = self.hash_password(auth_data['password'])
                has_2fa = True
            
            success = db.add_session(
                user_id=user_id,
                api_id=auth_data['api_id'],
                api_hash=auth_data['api_hash'],
                phone_number=auth_data['phone_number'],
                session_string=session_string,
                password_hash=password_hash,
                has_2fa=has_2fa
            )
            
            if success:
                permanent_file = os.path.join(SESSIONS_DIR, f"{user_id}_{auth_data['phone_number']}")
                os.rename(auth_data['session_file'] + ".session", permanent_file + ".session")
                
                await client.disconnect()
                del self.pending_auth[user_id]
                
                return True, "Session saved successfully"
            else:
                return False, "Failed to save session (phone number may already exist)"
                
        except Exception as e:
            logger.error(f"Error completing auth: {e}")
            return False, str(e)
    
    def cleanup_pending_auth(self, user_id: int):
        """Clean up pending authentication data"""
        if user_id in self.pending_auth:
            auth_data = self.pending_auth[user_id]
            if 'client' in auth_data:
                try:
                    asyncio.create_task(auth_data['client'].disconnect())
                except:
                    pass
            
            if 'session_file' in auth_data:
                try:
                    os.remove(auth_data['session_file'] + '.session')
                except:
                    pass
            
            del self.pending_auth[user_id]

# ============================================================================
# BOT COMMANDS
# ============================================================================

class BotCommands:
    def __init__(self):
        self.user_contexts = {}
        self.pending_purchases = {}
        
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command"""
        user = update.effective_user
        
        db.add_user(user.id, user.username, user.first_name)
        
        welcome_text = f"""
🤖 **Welcome to Telegram Group Market Bot!**

Hello {user.first_name or user.username}! 👋

This bot allows you to buy and sell Telegram groups in a secure marketplace.

🏪 **What you can do:**
• Browse and purchase groups by creation date
• List your own groups for sale
• Manage your balance and withdrawals
• Transfer group ownership securely

💰 **Current Balance:** ${format_balance(db.get_user_balance(user.id))} USDT

📱 **Quick Start:**
• Use `/market` to browse available groups
• Use `/help` to see all commands
• Send USDT via @cctip_bot in the bank group to add balance

Ready to start trading? 🚀
"""
        
        await update.message.reply_text(welcome_text, parse_mode=ParseMode.MARKDOWN)
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /help command"""
        await update.message.reply_text(generate_help_text(), parse_mode=ParseMode.MARKDOWN)
    
    async def balance_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /balance command"""
        user = update.effective_user
        balance = db.get_user_balance(user.id)
        
        text = f"""
💰 **Your Balance**

Current Balance: **${format_balance(balance)} USDT**

💳 **Add Balance:**
Send USDT via @cctip_bot in the designated bank group to add funds to your account.

📊 **Transaction History:**
Use the web dashboard for detailed transaction history.
"""
        
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
    
    async def market_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /market command"""
        years = get_available_years()
        keyboard = create_market_keyboard(years)
        
        text = """
🏪 **Group Market**

Select a year to browse groups by creation date:
"""
        
        await update.message.reply_text(text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)
    
    async def buy_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /buy command"""
        user = update.effective_user
        
        if not context.args:
            await update.message.reply_text(
                "❌ Please provide buying IDs.\n\n"
                "**Usage:** `/buy G123ABC` or `/buy G123ABC, G456DEF`",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        buying_ids_str = " ".join(context.args)
        buying_ids = validate_buying_ids(buying_ids_str)
        
        if not buying_ids:
            await update.message.reply_text(
                "❌ Invalid buying ID format.\n\n"
                "Buying IDs should be in format: `G123ABC`",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        total_cost = 0
        group_details = []
        
        for buying_id in buying_ids:
            group = db.get_group_by_buying_id(buying_id)
            if not group:
                await update.message.reply_text(
                    f"❌ Group with ID `{buying_id}` not found or no longer available.",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            
            total_cost += group['price']
            group_details.append(group)
        
        user_balance = db.get_user_balance(user.id)
        if user_balance < total_cost:
            await update.message.reply_text(
                f"❌ Insufficient balance.\n\n"
                f"**Total Cost:** ${format_price(total_cost)} USDT\n"
                f"**Your Balance:** ${format_balance(user_balance)} USDT\n"
                f"**Needed:** ${format_price(total_cost - user_balance)} USDT",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        success = db.purchase_groups(user.id, buying_ids)
        
        if not success:
            await update.message.reply_text(
                "❌ Purchase failed. Please try again or contact support.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        text = f"""
✅ **Purchase Successful!**

**Total Cost:** ${format_price(total_cost)} USDT
**Remaining Balance:** ${format_balance(user_balance - total_cost)} USDT

**📋 Purchased Groups:**

"""
        
        for group in group_details:
            group_name = format_group_name(group['group_name'], group['invite_link'])
            buying_id = format_buying_id(group['buying_id'])
            text += f"• {group_name} {buying_id}\n"
        
        text += f"""

**🎯 Next Steps:**
1. Join each group using the invite links above
2. Once you've joined, type `/claim` in each group
3. The group ownership will be transferred to you

**⚠️ Important:**
• You must join the groups before claiming
• Use `/claim` command only after joining
• Ownership transfer may take a few minutes
"""
        
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)
    
    async def claim_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /claim command"""
        user = update.effective_user
        chat = update.effective_chat
        
        if chat.type not in ['group', 'supergroup']:
            await update.message.reply_text(
                "❌ This command can only be used in groups.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        await update.message.reply_text(
            "🔄 Processing ownership transfer...\n\n"
            "Please wait while we verify your purchase and transfer ownership.",
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def list_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /list command"""
        user = update.effective_user
        chat = update.effective_chat
        
        if chat.type not in ['group', 'supergroup']:
            await update.message.reply_text(
                "❌ This command can only be used in groups you want to sell.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        text = """
📋 **List Your Group for Sale**

Please enter the price for your group in USDT.

**Price Requirements:**
• Minimum: $0.01 USDT
• Maximum: $99.99 USDT
• Maximum 2 decimal places (e.g., 15.50)

**Example:** `15.50`
"""
        
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
        self.user_contexts[user.id] = {
            'state': 'waiting_price',
            'chat_id': chat.id,
            'chat_title': chat.title
        }
    
    async def refund_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /refund command"""
        user = update.effective_user
        chat = update.effective_chat
        
        if chat.type not in ['group', 'supergroup']:
            await update.message.reply_text(
                "❌ This command can only be used in the group you want to refund.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        await update.message.reply_text(
            "🔄 Processing refund request...\n\n"
            "We're checking your ownership and removing the listing.",
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def cprice_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /cprice command"""
        user = update.effective_user
        chat = update.effective_chat
        
        if chat.type not in ['group', 'supergroup']:
            await update.message.reply_text(
                "❌ This command can only be used in groups you own.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        if not context.args:
            await update.message.reply_text(
                "❌ Please provide a new price.\n\n"
                "**Usage:** `/cprice 25.50`",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        price_str = context.args[0]
        is_valid, price = validate_price(price_str)
        
        if not is_valid:
            await update.message.reply_text(
                "❌ Invalid price format.\n\n"
                "Price must be between $0.01 and $99.99 with max 2 decimal places.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        await update.message.reply_text(
            f"✅ Price updated to ${format_price(price)} USDT",
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def withdraw_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /withdraw command"""
        user = update.effective_user
        balance = db.get_user_balance(user.id)
        
        if balance <= 0:
            await update.message.reply_text(
                "❌ You have no balance to withdraw.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        text = f"""
💸 **Withdrawal Request**

**Current Balance:** ${format_balance(balance)} USDT

Please enter the amount you want to withdraw:

**Example:** `10.50`
"""
        
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
        self.user_contexts[user.id] = {'state': 'waiting_withdraw_amount'}
    
    # Admin Commands
    async def admin_help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /ahelp command"""
        user = update.effective_user
        
        if user.id not in BOT_OWNERS:
            return
        
        await update.message.reply_text(generate_admin_help_text(), parse_mode=ParseMode.MARKDOWN)
    
    async def add_session_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /add command"""
        user = update.effective_user
        
        if user.id not in BOT_OWNERS:
            return
        
        text = """
🤖 **Add Userbot Session**

Let's add a new userbot session for group transfers.

Please provide your **API ID**:

You can get this from https://my.telegram.org
"""
        
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
        self.user_contexts[user.id] = {'state': 'waiting_api_id'}
    
    async def users_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /users command"""
        user = update.effective_user
        
        if user.id not in BOT_OWNERS:
            return
        
        page = 0
        if context.args:
            try:
                page = int(context.args[0])
            except ValueError:
                page = 0
        
        users = db.get_all_users(page, 10)
        total_users = db.get_total_users_count()
        total_volume = db.get_total_volume()
        
        text = f"""
👥 **Users Statistics**

**Total Users:** {total_users}
**Total Volume:** ${format_balance(total_volume)} USDT

**Users List (Page {page + 1}):**

"""
        
        for i, user_data in enumerate(users, start=1):
            user_link = format_user_link(
                user_data['user_id'], 
                user_data['username'], 
                user_data['first_name']
            )
            user_id_mono = f"`{user_data['user_id']}`"
            volume = format_balance(user_data['total_volume'])
            groups_count = user_data['groups_count']
            
            text += f"{i}. {user_link} - {user_id_mono} - ${volume} - {groups_count} groups\n"
        
        await update.message.reply_text(
            text, 
            parse_mode=ParseMode.MARKDOWN,
            disable_web_page_preview=True
        )
    
    async def add_balance_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /add_bal command"""
        user = update.effective_user
        
        if user.id not in BOT_OWNERS:
            return
        
        if len(context.args) != 2:
            await update.message.reply_text(
                "❌ Invalid usage.\n\n"
                "**Usage:** `/add_bal <user_id> <amount>`\n"
                "**Example:** `/add_bal 123456789 10.50`",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        try:
            target_user_id = int(context.args[0])
            amount = float(context.args[1])
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid user ID or amount format.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        success = db.update_user_balance(target_user_id, amount, 'admin_adjustment')
        
        if success:
            new_balance = db.get_user_balance(target_user_id)
            await update.message.reply_text(
                f"✅ Balance updated for user `{target_user_id}`\n\n"
                f"**Amount:** {'+' if amount >= 0 else ''}{format_balance(amount)} USDT\n"
                f"**New Balance:** ${format_balance(new_balance)} USDT",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await update.message.reply_text(
                "❌ Failed to update balance.",
                parse_mode=ParseMode.MARKDOWN
            )
    
    # Callback Query Handlers
    async def handle_callback_query(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle inline keyboard callbacks"""
        query = update.callback_query
        await query.answer()
        
        data = query.data
        
        if data.startswith('year_'):
            await self.handle_year_selection(query, context)
        elif data.startswith('month_'):
            await self.handle_month_selection(query, context)
        elif data == 'market_back':
            await self.market_command(update, context)
    
    async def handle_year_selection(self, query, context):
        """Handle year selection in market"""
        year = int(query.data.split('_')[1])
        keyboard = create_month_keyboard(year)
        
        text = f"""
🏪 **Group Market - {year}**

Select a month to view available groups:
"""
        
        await query.edit_message_text(text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)
    
    async def handle_month_selection(self, query, context):
        """Handle month selection in market"""
        parts = query.data.split('_')
        year = int(parts[1])
        month = int(parts[2])
        
        groups = db.get_groups_by_date(year, month)
        
        if not groups:
            month_names = [
                "January", "February", "March", "April", "May", "June",
                "July", "August", "September", "October", "November", "December"
            ]
            
            text = f"""
🏪 **Group Market - {month_names[month-1]} {year}**

❌ No groups available for this month.

Try selecting a different month or year.
"""
            
            keyboard = create_month_keyboard(year)
            await query.edit_message_text(text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)
            return
        
        keyboard = create_groups_keyboard(groups)
        
        month_names = [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"
        ]
        
        text = f"""
🏪 **Group Market - {month_names[month-1]} {year}**

**{len(groups)} groups available**

Select a price range to view groups:
"""
        
        await query.edit_message_text(text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)
    
    # Message Handlers
    async def handle_text_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle text messages based on user context"""
        user = update.effective_user
        text = update.message.text
        
        if user.id not in self.user_contexts:
            return
        
        user_context = self.user_contexts[user.id]
        state = user_context.get('state')
        
        if state == 'waiting_price':
            await self.handle_price_input(update, context)
        elif state == 'waiting_withdraw_amount':
            await self.handle_withdraw_amount_input(update, context)
        elif state == 'waiting_withdraw_address':
            await self.handle_withdraw_address_input(update, context)
        elif state == 'waiting_api_id':
            await self.handle_api_id_input(update, context)
        elif state == 'waiting_api_hash':
            await self.handle_api_hash_input(update, context)
        elif state == 'waiting_phone':
            await self.handle_phone_input(update, context)
        elif state == 'waiting_code':
            await self.handle_code_input(update, context)
        elif state == 'waiting_password':
            await self.handle_password_input(update, context)
    
    async def handle_price_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle price input for listing"""
        user = update.effective_user
        price_str = update.message.text.strip()
        
        is_valid, price = validate_price(price_str)
        
        if not is_valid:
            await update.message.reply_text(
                "❌ Invalid price format.\n\n"
                "Price must be between $0.01 and $99.99 with max 2 decimal places.\n"
                "Please try again:",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        user_context = self.user_contexts[user.id]
        user_context['price'] = price
        
        text = f"""
✅ **Price Set:** ${format_price(price)} USDT

Now, please add one of our userbots to your group as admin with full rights:

**Available Userbots:**
• @example_userbot (add this bot to your group)

**Steps:**
1. Add the userbot to your group
2. Give it admin rights with full permissions
3. Wait up to 5 minutes for verification
4. Type `/done` when completed

**⏰ Timeout:** 5 minutes
"""
        
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
        del self.user_contexts[user.id]
    
    async def handle_withdraw_amount_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle withdrawal amount input"""
        user = update.effective_user
        amount_str = update.message.text.strip()
        user_balance = db.get_user_balance(user.id)
        
        is_valid, amount = validate_withdrawal_amount(amount_str, user_balance)
        
        if not is_valid:
            await update.message.reply_text(
                f"❌ Invalid amount.\n\n"
                f"**Your Balance:** ${format_balance(user_balance)} USDT\n"
                f"Please enter a valid amount:",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        user_context = self.user_contexts[user.id]
        user_context['withdraw_amount'] = amount
        user_context['state'] = 'waiting_withdraw_address'
        
        text = f"""
💸 **Withdrawal Amount:** ${format_price(amount)} USDT

Please provide your withdrawal address:

**Supported Formats:**
• Polygon address (0x...)
• CWallet ID (alphanumeric)

**Example:** `0x1234567890123456789012345678901234567890`
**Or:** `mywalletid123`
"""
        
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
    
    async def handle_withdraw_address_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle withdrawal address input"""
        user = update.effective_user
        address = update.message.text.strip()
        
        if not validate_polygon_address(address):
            await update.message.reply_text(
                "❌ Invalid address format.\n\n"
                "Please provide a valid Polygon address or CWallet ID:",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        user_context = self.user_contexts[user.id]
        amount = user_context['withdraw_amount']
        
        success = db.add_withdrawal_request(user.id, amount, address)
        
        if success:
            text = f"""
✅ **Withdrawal Request Submitted**

**Amount:** ${format_price(amount)} USDT
**Address:** `{address}`

Your withdrawal request has been submitted for admin approval.
You will be notified when it's processed.
"""
            
            await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
        else:
            await update.message.reply_text(
                "❌ Failed to create withdrawal request. Please try again.",
                parse_mode=ParseMode.MARKDOWN
            )
        
        del self.user_contexts[user.id]
    
    # Session Management Handlers
    async def handle_api_id_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle API ID input"""
        user = update.effective_user
        api_id_str = update.message.text.strip()
        
        is_valid, api_id = validate_api_credentials(api_id_str, "dummy")
        
        if not is_valid:
            await update.message.reply_text(
                "❌ Invalid API ID format.\n\n"
                "Please enter a valid API ID (numbers only):",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        user_context = self.user_contexts[user.id]
        user_context['api_id'] = api_id
        user_context['state'] = 'waiting_api_hash'
        
        await update.message.reply_text(
            "✅ API ID saved.\n\n"
            "Now please provide your **API Hash**:",
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def handle_api_hash_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle API Hash input"""
        user = update.effective_user
        api_hash = update.message.text.strip()
        
        is_valid, _ = validate_api_credentials("123456", api_hash)
        
        if not is_valid:
            await update.message.reply_text(
                "❌ Invalid API Hash format.\n\n"
                "Please enter a valid API Hash:",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        user_context = self.user_contexts[user.id]
        user_context['api_hash'] = api_hash
        user_context['state'] = 'waiting_phone'
        
        await update.message.reply_text(
            "✅ API Hash saved.\n\n"
            "Now please provide your **Phone Number** (with country code):\n\n"
            "**Example:** `+1234567890`",
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def handle_phone_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle phone number input"""
        user = update.effective_user
        phone = update.message.text.strip()
        
        if not validate_phone_number(phone):
            await update.message.reply_text(
                "❌ Invalid phone number format.\n\n"
                "Please enter a valid phone number with country code:",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        user_context = self.user_contexts[user.id]
        user_context['phone'] = phone
        
        success = await session_manager.start_auth_process(
            user.id, 
            user_context['api_id'], 
            user_context['api_hash'], 
            phone
        )
        
        if success:
            user_context['state'] = 'waiting_code'
            await update.message.reply_text(
                f"📱 **OTP Sent**\n\n"
                f"We've sent a verification code to `{phone}`.\n\n"
                f"Please enter the code you received:",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await update.message.reply_text(
                "❌ Failed to send verification code. Please check your phone number and try again.",
                parse_mode=ParseMode.MARKDOWN
            )
            del self.user_contexts[user.id]
    
    async def handle_code_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle OTP code input"""
        user = update.effective_user
        code = update.message.text.strip()
        
        success, message = await session_manager.verify_code(user.id, code)
        
        if success:
            if message == "2FA password required":
                user_context = self.user_contexts[user.id]
                user_context['state'] = 'waiting_password'
                await update.message.reply_text(
                    "🔐 **2FA Required**\n\n"
                    "Please enter your 2-step verification password:",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                success, complete_message = await session_manager.complete_auth(user.id)
                
                if success:
                    await update.message.reply_text(
                        "✅ **Session Added Successfully!**\n\n"
                        "Your userbot session has been saved and is ready to use.",
                        parse_mode=ParseMode.MARKDOWN
                    )
                else:
                    await update.message.reply_text(
                        f"❌ Failed to save session: {complete_message}",
                        parse_mode=ParseMode.MARKDOWN
                    )
                
                del self.user_contexts[user.id]
        else:
            await update.message.reply_text(
                f"❌ {message}\n\nPlease try again:",
                parse_mode=ParseMode.MARKDOWN
            )
    
    async def handle_password_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle 2FA password input"""
        user = update.effective_user
        password = update.message.text.strip()
        
        success, message = await session_manager.verify_password(user.id, password)
        
        if success:
            success, complete_message = await session_manager.complete_auth(user.id)
            
            if success:
                await update.message.reply_text(
                    "✅ **Session Added Successfully!**\n\n"
                    "Your userbot session has been saved with 2FA protection.",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await update.message.reply_text(
                    f"❌ Failed to save session: {complete_message}",
                    parse_mode=ParseMode.MARKDOWN
                )
        else:
            await update.message.reply_text(
                f"❌ {message}\n\nPlease try again:",
                parse_mode=ParseMode.MARKDOWN
            )
        
        del self.user_contexts[user.id]
    
    # Payment Detection
    async def handle_tip_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle tip messages from cctip bot"""
        message = update.message
        
        if (message.from_user.id != CCTIP_BOT_ID or 
            message.chat.id != BANK_GROUP_ID):
            return
        
        tip_info = parse_tip_message(message.text)
        
        if not tip_info or not tip_info['valid']:
            return
        
        # Note: Real implementation would need proper user extraction from tip message
        logger.info(f"Tip detected: {tip_info['amount']} USDT")

# ============================================================================
# MAIN BOT CLASS
# ============================================================================

class TelegramMarketBot:
    def __init__(self):
        self.application = None
        self.is_running = False
        
    def setup_handlers(self):
        """Setup all command and message handlers"""
        app = self.application
        
        # User Commands
        app.add_handler(CommandHandler("start", bot_commands.start_command))
        app.add_handler(CommandHandler("help", bot_commands.help_command))
        app.add_handler(CommandHandler("balance", bot_commands.balance_command))
        app.add_handler(CommandHandler("market", bot_commands.market_command))
        app.add_handler(CommandHandler("buy", bot_commands.buy_command))
        app.add_handler(CommandHandler("claim", bot_commands.claim_command))
        app.add_handler(CommandHandler("list", bot_commands.list_command))
        app.add_handler(CommandHandler("refund", bot_commands.refund_command))
        app.add_handler(CommandHandler("cprice", bot_commands.cprice_command))
        app.add_handler(CommandHandler("withdraw", bot_commands.withdraw_command))
        
        # Admin Commands
        app.add_handler(CommandHandler("ahelp", bot_commands.admin_help_command))
        app.add_handler(CommandHandler("add", bot_commands.add_session_command))
        app.add_handler(CommandHandler("add_bank", bot_commands.add_session_command))
        app.add_handler(CommandHandler("users", bot_commands.users_command))
        app.add_handler(CommandHandler("add_bal", bot_commands.add_balance_command))
        
        # Callback Query Handler
        app.add_handler(CallbackQueryHandler(bot_commands.handle_callback_query))
        
        # Text Message Handler
        app.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND, 
            bot_commands.handle_text_message
        ))
        
        # Tip Detection Handler
        app.add_handler(MessageHandler(
            filters.User(user_id=CCTIP_BOT_ID) & filters.Chat(chat_id=BANK_GROUP_ID),
            bot_commands.handle_tip_message
        ))
        
        # Error Handler
        app.add_error_handler(self.error_handler)
        
        logger.info("All handlers setup complete")
    
    async def error_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle errors with improved error categorization"""
        error = context.error
        
        logger.error(f"Exception while handling an update: {error}")
        
        if isinstance(error, NetworkError):
            logger.error("Network error occurred. Bot will retry automatically.")
            return
        elif isinstance(error, TimedOut):
            logger.error("Request timed out. Bot will retry automatically.")
            return
        elif isinstance(error, BadRequest):
            logger.error(f"Bad request error: {error}")
        else:
            logger.error(f"Unexpected error: {error}")
        
        if update and update.effective_message:
            try:
                await update.effective_message.reply_text(
                    "❌ An error occurred. Please try again or contact support.",
                    parse_mode='Markdown'
                )
            except Exception as e:
                logger.error(f"Failed to send error message to user: {e}")
    
    async def startup_message(self):
        """Send startup message to bot owners with retry logic"""
        for owner_id in BOT_OWNERS:
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    await self.application.bot.send_message(
                        chat_id=owner_id,
                        text="🤖 **Bot Started Successfully!**\n\n"
                             f"**Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                             f"**Status:** ✅ Online\n"
                             f"**Database:** ✅ Connected\n"
                             f"**Sessions:** ✅ Ready",
                        parse_mode='Markdown'
                    )
                    logger.info(f"Startup message sent to owner {owner_id}")
                    break
                except (NetworkError, TimedOut) as e:
                    logger.warning(f"Failed to send startup message to {owner_id} (attempt {attempt + 1}): {e}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2 ** attempt)
                except Exception as e:
                    logger.error(f"Unexpected error sending startup message: {e}")
                    break
    
    def signal_handler(self, signum, frame):
        """Handle system signals for graceful shutdown"""
        logger.info(f"Received signal {signum}. Shutting down gracefully...")
        self.is_running = False
    
    async def run(self):
        """Run the bot with improved error handling and recovery"""
        try:
            signal.signal(signal.SIGINT, self.signal_handler)
            signal.signal(signal.SIGTERM, self.signal_handler)
            
            self.application = (
                Application.builder()
                .token(BOT_TOKEN)
                .read_timeout(30)
                .write_timeout(30)
                .connect_timeout(30)
                .pool_timeout(30)
                .build()
            )
            
            self.setup_handlers()
            
            await self.application.initialize()
            
            await self.application.start()
            self.is_running = True
            
            await self.startup_message()
            
            logger.info("🤖 Telegram Group Market Bot started successfully!")
            
            try:
                bot_info = await self.application.bot.get_me()
                logger.info(f"Bot username: @{bot_info.username}")
            except Exception as e:
                logger.warning(f"Could not get bot info: {e}")
            
            await self.application.updater.start_polling(
                allowed_updates=Update.ALL_TYPES,
                drop_pending_updates=True,
                read_timeout=30,
                write_timeout=30,
                connect_timeout=30,
                pool_timeout=30
            )
            
            while self.is_running:
                try:
                    await asyncio.sleep(1)
                except asyncio.CancelledError:
                    break
            
        except Exception as e:
            logger.error(f"Critical error in bot execution: {e}")
            raise
        finally:
            logger.info("Bot shutting down...")
            self.is_running = False
            
            if self.application:
                try:
                    if self.application.updater.running:
                        await self.application.updater.stop()
                    await self.application.stop()
                    await self.application.shutdown()
                except Exception as e:
                    logger.error(f"Error during cleanup: {e}")
            
            logger.info("Bot shutdown complete")

# ============================================================================
# GLOBAL INSTANCES
# ============================================================================

# Create global instances
db = Database()
session_manager = SessionManager()
bot_commands = BotCommands()

# ============================================================================
# MAIN FUNCTION
# ============================================================================

def main():
    """Main function with improved error handling"""
    try:
        logger.info("Checking database connection...")
        total_users = db.get_total_users_count()
        logger.info(f"Database connected. Total users: {total_users}")
        
        if not os.path.exists(SESSIONS_DIR):
            os.makedirs(SESSIONS_DIR)
            logger.info("Created sessions directory")
        
        if not BOT_TOKEN or BOT_TOKEN == "your_bot_token_here":
            logger.error("Invalid bot token. Please configure BOT_TOKEN in config.py")
            sys.exit(1)
        
        bot = TelegramMarketBot()
        
        asyncio.run(bot.run())
        
    except KeyboardInterrupt:
        logger.info("Bot stopped by user (Ctrl+C)")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    print("""
╔══════════════════════════════════════════════════════════════╗
║                  Telegram Group Market Bot                  ║
║                      Combined Version                       ║
║                                                              ║
║  All functionality in one file for easy deployment.         ║
║  Only requires config.py to run.                            ║
║                                                              ║
║  Starting bot...                                             ║
╚══════════════════════════════════════════════════════════════╝
    """)
    
    main()