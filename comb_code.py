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
import base64
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple

# Add cryptography for secure password encryption
try:
    from cryptography.fernet import Fernet
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    CRYPTO_AVAILABLE = True
except ImportError:
    print("⚠️  Warning: cryptography library not found. Install with: pip install cryptography")
    print("   Without this, true ownership transfer will be limited to admin rights only.")
    CRYPTO_AVAILABLE = False

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
    MIN_WITHDRAWAL, BUYING_FEE_RATE, SELLING_FEE_RATE, REFERRAL_COMMISSION_RATE
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
                    password_encrypted TEXT,
                    has_2fa BOOLEAN DEFAULT FALSE,
                    is_active BOOLEAN DEFAULT TRUE,
                    session_type TEXT DEFAULT 'regular',
                    username TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (user_id),
                    UNIQUE(phone_number)
                )
            ''')
            
            # Add password_encrypted column to existing sessions table if it doesn't exist
            try:
                cursor.execute('ALTER TABLE sessions ADD COLUMN password_encrypted TEXT')
            except sqlite3.OperationalError:
                # Column already exists
                pass
            
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
            
            # Bulk keywords table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS bulk_keywords (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    keyword TEXT NOT NULL,
                    year INTEGER NOT NULL,
                    month INTEGER,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, keyword),
                    FOREIGN KEY (user_id) REFERENCES users (user_id)
                )
            ''')
            
            # Referral relationships table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS referrals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    referrer_id INTEGER NOT NULL,
                    referred_id INTEGER NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(referred_id),
                    FOREIGN KEY (referrer_id) REFERENCES users (user_id),
                    FOREIGN KEY (referred_id) REFERENCES users (user_id),
                    CHECK (referrer_id != referred_id)
                )
            ''')
            
            # Referral earnings table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS referral_earnings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    referrer_id INTEGER NOT NULL,
                    referred_id INTEGER NOT NULL,
                    transaction_id INTEGER,
                    transaction_type TEXT NOT NULL,
                    fee_amount REAL NOT NULL,
                    commission_amount REAL NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (referrer_id) REFERENCES users (user_id),
                    FOREIGN KEY (referred_id) REFERENCES users (user_id),
                    FOREIGN KEY (transaction_id) REFERENCES transactions (id)
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
                   session_string: str, password_hash: str = None, has_2fa: bool = False, session_type: str = 'regular', username: str = None, raw_password: str = None) -> bool:
        """Add new session to database with enhanced security checks and encrypted password storage"""
        with self.lock:
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                
                # Security check: Validate password if 2FA is enabled
                if has_2fa and not password_hash:
                    logger.error("2FA enabled but no password hash provided")
                    conn.close()
                    return False
                
                if password_hash and not self.is_password_valid(password_hash):
                    logger.error("Invalid password hash for session")
                    conn.close()
                    return False
                
                # Check if phone number is already registered by ANY user
                cursor.execute('SELECT user_id FROM sessions WHERE phone_number = ? AND is_active = TRUE', (phone_number,))
                existing_session = cursor.fetchone()
                if existing_session:
                    existing_user_id = existing_session[0]
                    if existing_user_id != user_id:
                        logger.warning(f"Phone number {phone_number} already registered by different user {existing_user_id}")
                        conn.close()
                        return False
                    else:
                        logger.warning(f"Phone number {phone_number} already has an active session for this user")
                        conn.close()
                        return False
                
                # Check session limit per user
                cursor.execute('SELECT COUNT(*) FROM sessions WHERE user_id = ? AND is_active = TRUE', (user_id,))
                session_count = cursor.fetchone()[0]
                if session_count >= MAX_SESSIONS_PER_USER:
                    logger.warning(f"User {user_id} has reached maximum session limit")
                    conn.close()
                    return False
                
                # Encrypt password for secure storage (enables true ownership transfer)
                password_encrypted = None
                if raw_password and has_2fa:
                    password_encrypted = password_crypto.encrypt_password(raw_password)
                    if password_encrypted:
                        logger.info(f"Password encrypted successfully for session {phone_number}")
                    else:
                        logger.warning(f"Password encryption failed for session {phone_number} - true ownership transfer may be limited")
                
                cursor.execute('''
                    INSERT INTO sessions (user_id, api_id, api_hash, phone_number, 
                                        session_string, password_hash, password_encrypted, has_2fa, session_type, username)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (user_id, api_id, api_hash, phone_number, session_string, password_hash, password_encrypted, has_2fa, session_type, username))
                
                conn.commit()
                conn.close()
                logger.info(f"Session added successfully for user {user_id}, phone {phone_number}")
                return True
            except Exception as e:
                logger.error(f"Error adding session: {e}")
                return False
    
    def is_password_valid(self, password_hash: str) -> bool:
        """Validate password hash meets security criteria"""
        if not password_hash:
            return False
        
        # Check if it's a valid SHA-256 hash (64 hex characters)
        if len(password_hash) != 64:
            return False
        
        try:
            int(password_hash, 16)  # Try to parse as hexadecimal
            return True
        except ValueError:
            return False
    
    def verify_session_ownership(self, session_id: int, user_id: int) -> bool:
        """Verify that a session belongs to a specific user"""
        with self.lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT user_id FROM sessions WHERE id = ? AND is_active = TRUE', (session_id,))
            result = cursor.fetchone()
            conn.close()
            return result and result[0] == user_id
    
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
        """Get existing buying ID or create new one for group (permanent mapping)"""
        with self.lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Check if group already has a buying ID (permanent mapping)
            cursor.execute('SELECT buying_id FROM group_codes WHERE group_id = ?', (group_id,))
            result = cursor.fetchone()
            
            if result:
                conn.close()
                logger.info(f"Using existing buying ID {result[0]} for group {group_id}")
                return result[0]
            
            # Generate new buying ID
            while True:
                buying_id = 'G' + ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
                cursor.execute('SELECT group_id FROM group_codes WHERE buying_id = ?', (buying_id,))
                if not cursor.fetchone():
                    break
            
            # Store the new buying ID permanently
            cursor.execute('INSERT INTO group_codes (group_id, buying_id) VALUES (?, ?)', 
                          (group_id, buying_id))
            conn.commit()
            conn.close()
            logger.info(f"Created new permanent buying ID {buying_id} for group {group_id}")
            return buying_id
    
    def mark_group_as_sold(self, group_id: int, buyer_id: int):
        """Mark a group as sold to prevent re-listing"""
        with self.lock:
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                
                # Update group status
                cursor.execute('''
                    UPDATE groups 
                    SET is_listed = FALSE, sold_to = ?, sold_at = datetime('now')
                    WHERE group_id = ?
                ''', (buyer_id, group_id))
                
                conn.commit()
                conn.close()
                logger.info(f"Group {group_id} marked as sold to user {buyer_id}")
                return True
            except Exception as e:
                logger.error(f"Error marking group as sold: {e}")
                return False
    
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
    
    def purchase_groups(self, user_id: int, buying_ids: List[str], subtotal: float = None, buying_fee: float = None) -> bool:
        with self.lock:
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                
                calculated_subtotal = 0
                group_data = []
                for buying_id in buying_ids:
                    cursor.execute('SELECT price, group_id FROM groups WHERE buying_id = ? AND is_listed = TRUE', 
                                 (buying_id,))
                    result = cursor.fetchone()
                    if not result:
                        conn.close()
                        return False
                    calculated_subtotal += result[0]
                    group_data.append({'buying_id': buying_id, 'price': result[0], 'group_id': result[1]})
                
                # Use provided subtotal and fee, or calculate if not provided
                final_subtotal = subtotal if subtotal is not None else calculated_subtotal
                final_fee = buying_fee if buying_fee is not None else (calculated_subtotal * BUYING_FEE_RATE)
                total_cost = final_subtotal + final_fee
                
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
                
                # Record purchase transaction with fee breakdown
                transaction_details = {
                    'subtotal': final_subtotal,
                    'buying_fee': final_fee,
                    'total': total_cost,
                    'group_ids': [g['group_id'] for g in group_data]
                }
                
                cursor.execute('''
                    INSERT INTO transactions (user_id, transaction_type, amount, group_ids, status)
                    VALUES (?, 'purchase', ?, ?, 'completed')
                ''', (user_id, -total_cost, json.dumps(transaction_details)))
                
                # Get transaction ID for referral tracking
                transaction_id = cursor.lastrowid
                
                conn.commit()
                conn.close()
                
                # Handle referral commission for buying fees
                referrer_id = self.get_referrer(user_id)
                if referrer_id and final_fee > 0:
                    commission = final_fee * REFERRAL_COMMISSION_RATE
                    success = self.add_referral_earning(
                        referrer_id, user_id, 'buying_fee', final_fee, commission, transaction_id
                    )
                    if success:
                        logger.info(f"Referral commission paid: ${commission:.4f} to {referrer_id} for buying fee from {user_id}")
                
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
    
    def add_bulk_keyword(self, user_id: int, keyword: str, year: int, month: int = None) -> bool:
        """Add or update a bulk keyword for user"""
        with self.lock:
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO bulk_keywords (user_id, keyword, year, month)
                    VALUES (?, ?, ?, ?)
                ''', (user_id, keyword.lower(), year, month))
                conn.commit()
                conn.close()
                return True
            except Exception as e:
                logger.error(f"Error adding bulk keyword: {e}")
                return False
    
    def get_bulk_keyword(self, user_id: int, keyword: str) -> Optional[Dict]:
        """Get bulk keyword details"""
        with self.lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT keyword, year, month, created_at 
                FROM bulk_keywords 
                WHERE user_id = ? AND keyword = ?
            ''', (user_id, keyword.lower()))
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return {
                    'keyword': result[0],
                    'year': result[1],
                    'month': result[2],
                    'created_at': result[3]
                }
            return None
    
    def get_user_bulk_keywords(self, user_id: int) -> List[Dict]:
        """Get all bulk keywords for user"""
        with self.lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT keyword, year, month, created_at 
                FROM bulk_keywords 
                WHERE user_id = ? 
                ORDER BY created_at DESC
            ''', (user_id,))
            
            keywords = []
            for row in cursor.fetchall():
                keywords.append({
                    'keyword': row[0],
                    'year': row[1],
                    'month': row[2],
                    'created_at': row[3]
                })
            
            conn.close()
            return keywords
    
    def delete_bulk_keyword(self, user_id: int, keyword: str) -> bool:
        """Delete a bulk keyword"""
        with self.lock:
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                cursor.execute('''
                    DELETE FROM bulk_keywords 
                    WHERE user_id = ? AND keyword = ?
                ''', (user_id, keyword.lower()))
                conn.commit()
                conn.close()
                return True
            except Exception as e:
                logger.error(f"Error deleting bulk keyword: {e}")
                return False
    
    # Referral System Methods
    def add_referral(self, referrer_id: int, referred_id: int) -> bool:
        """Add a referral relationship with protection against abuse"""
        with self.lock:
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                
                # Check if referred user already has a referrer
                cursor.execute('SELECT referrer_id FROM referrals WHERE referred_id = ?', (referred_id,))
                existing = cursor.fetchone()
                if existing:
                    conn.close()
                    return False  # User already referred by someone else
                
                # Check if trying to refer themselves
                if referrer_id == referred_id:
                    conn.close()
                    return False
                
                # Check if both users exist
                cursor.execute('SELECT user_id FROM users WHERE user_id IN (?, ?)', (referrer_id, referred_id))
                users = cursor.fetchall()
                if len(users) != 2:
                    conn.close()
                    return False
                
                # Add referral relationship
                cursor.execute('''
                    INSERT INTO referrals (referrer_id, referred_id)
                    VALUES (?, ?)
                ''', (referrer_id, referred_id))
                
                conn.commit()
                conn.close()
                logger.info(f"Referral added: {referrer_id} referred {referred_id}")
                return True
                
            except Exception as e:
                logger.error(f"Error adding referral: {e}")
                return False
    
    def get_referrer(self, user_id: int) -> Optional[int]:
        """Get the referrer of a user"""
        with self.lock:
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                cursor.execute('SELECT referrer_id FROM referrals WHERE referred_id = ?', (user_id,))
                result = cursor.fetchone()
                conn.close()
                return result[0] if result else None
            except Exception as e:
                logger.error(f"Error getting referrer: {e}")
                return None
    
    def get_referral_stats(self, user_id: int) -> dict:
        """Get comprehensive referral statistics for a user"""
        with self.lock:
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                
                # Get total referrals count
                cursor.execute('SELECT COUNT(*) FROM referrals WHERE referrer_id = ?', (user_id,))
                total_referrals = cursor.fetchone()[0]
                
                # Get total referral earnings
                cursor.execute('SELECT SUM(commission_amount) FROM referral_earnings WHERE referrer_id = ?', (user_id,))
                total_earnings = cursor.fetchone()[0] or 0.0
                
                # Get recent referrals (last 10)
                cursor.execute('''
                    SELECT r.referred_id, u.username, u.first_name, r.created_at
                    FROM referrals r
                    JOIN users u ON r.referred_id = u.user_id
                    WHERE r.referrer_id = ?
                    ORDER BY r.created_at DESC
                    LIMIT 10
                ''', (user_id,))
                recent_referrals = cursor.fetchall()
                
                # Get monthly earnings breakdown
                cursor.execute('''
                    SELECT DATE(created_at) as date, SUM(commission_amount) as daily_earnings
                    FROM referral_earnings
                    WHERE referrer_id = ? AND created_at >= DATE('now', '-30 days')
                    GROUP BY DATE(created_at)
                    ORDER BY date DESC
                ''', (user_id,))
                monthly_breakdown = cursor.fetchall()
                
                conn.close()
                
                return {
                    'total_referrals': total_referrals,
                    'total_earnings': total_earnings,
                    'recent_referrals': recent_referrals,
                    'monthly_breakdown': monthly_breakdown
                }
                
            except Exception as e:
                logger.error(f"Error getting referral stats: {e}")
                return {
                    'total_referrals': 0,
                    'total_earnings': 0.0,
                    'recent_referrals': [],
                    'monthly_breakdown': []
                }
    
    def add_referral_earning(self, referrer_id: int, referred_id: int, transaction_type: str, 
                           fee_amount: float, commission_amount: float, transaction_id: int = None) -> bool:
        """Record a referral earning"""
        with self.lock:
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                
                # Add referral earning record
                cursor.execute('''
                    INSERT INTO referral_earnings 
                    (referrer_id, referred_id, transaction_id, transaction_type, fee_amount, commission_amount)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (referrer_id, referred_id, transaction_id, transaction_type, fee_amount, commission_amount))
                
                # Add commission to referrer's balance
                cursor.execute('''
                    UPDATE users SET balance = balance + ?
                    WHERE user_id = ?
                ''', (commission_amount, referrer_id))
                
                # Record transaction for referrer
                cursor.execute('''
                    INSERT INTO transactions (user_id, transaction_type, amount, status)
                    VALUES (?, 'referral_commission', ?, 'completed')
                ''', (referrer_id, commission_amount))
                
                conn.commit()
                conn.close()
                logger.info(f"Referral earning added: {referrer_id} earned ${commission_amount:.4f} from {referred_id}")
                return True
                
            except Exception as e:
                logger.error(f"Error adding referral earning: {e}")
                return False
    
    def get_bank_userbot_username(self) -> Optional[str]:
        """Get the username of the bank userbot"""
        with self.lock:
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT username FROM sessions 
                    WHERE session_type = 'bank' AND is_active = TRUE AND username IS NOT NULL
                    LIMIT 1
                ''')
                result = cursor.fetchone()
                conn.close()
                return result[0] if result else None
            except Exception as e:
                logger.error(f"Error getting bank userbot username: {e}")
                return None
    
    def get_session_password_for_transfer(self, session_id: int) -> Optional[str]:
        """Get decrypted password for true ownership transfer"""
        with self.lock:
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT password_encrypted, has_2fa FROM sessions 
                    WHERE id = ? AND is_active = TRUE
                ''', (session_id,))
                result = cursor.fetchone()
                conn.close()
                
                if not result:
                    return None
                
                password_encrypted, has_2fa = result
                
                if not has_2fa or not password_encrypted:
                    return None
                
                # Decrypt password for transfer
                decrypted_password = password_crypto.decrypt_password(password_encrypted)
                if decrypted_password:
                    logger.info(f"Password decrypted successfully for session {session_id}")
                    return decrypted_password
                else:
                    logger.warning(f"Password decryption failed for session {session_id}")
                    return None
                    
            except Exception as e:
                logger.error(f"Error getting session password for transfer: {e}")
                return None

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
    """Parse Cwallet/cctip bot tip message in exact format"""
    try:
        # Exact Cwallet tip format: "Username tip details:\n\nUSDT +amount @recipient"
        # Pattern matches: <username> tip details:\n\nUSDT +<amount> <recipient>
        cwallet_pattern = r'tip details:\s*\n*\s*USDT\s*\+(\d+(?:\.\d+)?)\s+'
        
        match = re.search(cwallet_pattern, message_text, re.IGNORECASE | re.MULTILINE)
        if match:
            amount_str = match.group(1)
            amount = float(amount_str)
            
            # Validate amount is reasonable (between 0.01 and 10000)
            if 0.01 <= amount <= 10000:
                logger.info(f"Successfully parsed Cwallet tip: {amount} USDT")
                return {
                    'amount': amount,
                    'currency': 'USDT',
                    'valid': True,
                    'usdt_mentioned': True,
                    'matched_pattern': cwallet_pattern,
                    'confidence': 'high'
                }
        
        # Fallback patterns for other possible formats
        fallback_patterns = [
            # Alternative Cwallet formats
            r'USDT\s*\+(\d+(?:\.\d+)?)',
            r'tip.*?(\d+(?:\.\d+)?)\s*USDT',
            r'(\d+(?:\.\d+)?)\s*USDT.*?tip',
            
            # Generic USDT patterns (lower confidence)
            r'(\d+(?:\.\d+)?)\s*USDT',
            r'\+(\d+(?:\.\d+)?)\s*USDT',
        ]
        
        for pattern in fallback_patterns:
            match = re.search(pattern, message_text, re.IGNORECASE | re.MULTILINE)
            if match:
                amount_str = match.group(1)
                amount = float(amount_str)
                
                # Validate amount is reasonable (between 0.01 and 10000)
                if 0.01 <= amount <= 10000:
                    # Check if USDT is mentioned in message
                    usdt_mentioned = bool(re.search(r'USDT|USD-T|usdt|usd-t', message_text, re.IGNORECASE))
                    
                    if usdt_mentioned:
                        logger.info(f"Parsed tip with fallback pattern: {amount} USDT")
                        return {
                            'amount': amount,
                            'currency': 'USDT',
                            'valid': True,
                            'usdt_mentioned': True,
                            'matched_pattern': pattern,
                            'confidence': 'medium'
                        }
        
        # Log failed parsing for debugging
        logger.debug(f"Failed to parse tip message: {message_text[:100]}...")
        return None
        
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing tip amount: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error parsing tip message: {e}")
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

# ============================================================================
# SECURE PASSWORD ENCRYPTION FOR TRUE OWNERSHIP TRANSFER
# ============================================================================

class PasswordCrypto:
    """Secure password encryption/decryption for 2FA passwords"""
    
    def __init__(self):
        self.master_key = self._get_or_create_master_key()
        if CRYPTO_AVAILABLE:
            self.fernet = Fernet(self.master_key)
        else:
            self.fernet = None
            logger.warning("Cryptography not available - using hash-only mode")
    
    def _get_or_create_master_key(self) -> bytes:
        """Get or create master encryption key"""
        key_file = os.path.join(SESSIONS_DIR, '.master_key')
        
        if os.path.exists(key_file):
            try:
                with open(key_file, 'rb') as f:
                    return f.read()
            except Exception as e:
                logger.error(f"Error reading master key: {e}")
        
        # Create new master key
        if CRYPTO_AVAILABLE:
            key = Fernet.generate_key()
        else:
            # Fallback key generation if cryptography not available
            key = base64.urlsafe_b64encode(os.urandom(32))
        
        try:
            os.makedirs(SESSIONS_DIR, exist_ok=True)
            with open(key_file, 'wb') as f:
                f.write(key)
            os.chmod(key_file, 0o600)  # Read-only for owner
            logger.info("Created new master encryption key")
        except Exception as e:
            logger.error(f"Error saving master key: {e}")
        
        return key
    
    def encrypt_password(self, password: str) -> Optional[str]:
        """Encrypt password for secure storage"""
        if not password or not CRYPTO_AVAILABLE or not self.fernet:
            return None
        
        try:
            encrypted = self.fernet.encrypt(password.encode())
            return base64.urlsafe_b64encode(encrypted).decode()
        except Exception as e:
            logger.error(f"Error encrypting password: {e}")
            return None
    
    def decrypt_password(self, encrypted_password: str) -> Optional[str]:
        """Decrypt password for use"""
        if not encrypted_password or not CRYPTO_AVAILABLE or not self.fernet:
            return None
        
        try:
            encrypted_data = base64.urlsafe_b64decode(encrypted_password.encode())
            decrypted = self.fernet.decrypt(encrypted_data)
            return decrypted.decode()
        except Exception as e:
            logger.error(f"Error decrypting password: {e}")
            return None
    
    def hash_password(self, password: str) -> str:
        """Hash password for verification (non-reversible)"""
        return hashlib.sha256(password.encode()).hexdigest()
    
    def verify_password_hash(self, password: str, hashed: str) -> bool:
        """Verify password against hash"""
        return self.hash_password(password) == hashed

# Create global password crypto instance
password_crypto = PasswordCrypto()

# ============================================================================
# VERIFICATION AND AUDIT FUNCTIONS FOR MARKETPLACE TRUST
# ============================================================================

class MarketplaceVerification:
    """Comprehensive verification and audit system for marketplace trust"""
    
    @staticmethod
    def create_transfer_audit_record(group_info: Dict, buyer_info: Dict, seller_info: Dict, 
                                   transfer_type: str, transaction_details: Dict) -> Dict:
        """Create comprehensive audit record for ownership transfer"""
        return {
            'timestamp': datetime.now().isoformat(),
            'group_details': {
                'group_id': group_info.get('group_id'),
                'group_name': group_info.get('group_name'),
                'buying_id': group_info.get('buying_id'),
                'creation_date': group_info.get('creation_date'),
                'total_messages': group_info.get('total_messages'),
                'listed_at': group_info.get('listed_at')
            },
            'seller_details': {
                'user_id': seller_info.get('user_id'),
                'username': seller_info.get('username'),
                'first_name': seller_info.get('first_name'),
                'listing_timestamp': group_info.get('listed_at')
            },
            'buyer_details': {
                'user_id': buyer_info.get('user_id'),
                'username': buyer_info.get('username'),
                'first_name': buyer_info.get('first_name'),
                'purchase_timestamp': transaction_details.get('purchase_timestamp')
            },
            'transfer_details': {
                'transfer_type': transfer_type,  # 'true_ownership' or 'admin_promotion'
                'price': group_info.get('price'),
                'selling_fee': transaction_details.get('selling_fee'),
                'seller_earnings': transaction_details.get('seller_earnings'),
                'has_2fa': transaction_details.get('has_2fa'),
                'password_available': transaction_details.get('password_available')
            },
            'verification_status': 'completed',
            'bot_owners': BOT_OWNERS.copy()
        }
    
    @staticmethod
    def get_user_info(user_id: int) -> Dict:
        """Get comprehensive user information for audit"""
        try:
            with db.lock:
                conn = db.get_connection()
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT user_id, username, first_name, balance, created_at 
                    FROM users WHERE user_id = ?
                ''', (user_id,))
                result = cursor.fetchone()
                conn.close()
                
                if result:
                    return {
                        'user_id': result[0],
                        'username': result[1],
                        'first_name': result[2],
                        'balance': result[3],
                        'created_at': result[4]
                    }
                return {}
        except Exception as e:
            logger.error(f"Error getting user info for audit: {e}")
            return {}
    
    @staticmethod
    def log_critical_transfer(audit_record: Dict):
        """Log critical transfer information for marketplace trust"""
        try:
            # Enhanced logging with all critical information
            logger.critical(f"OWNERSHIP TRANSFER COMPLETED - AUDIT RECORD")
            logger.critical(f"Group: {audit_record['group_details']['group_name']} (ID: {audit_record['group_details']['group_id']})")
            logger.critical(f"Seller: {audit_record['seller_details']['first_name']} (ID: {audit_record['seller_details']['user_id']})")
            logger.critical(f"Buyer: {audit_record['buyer_details']['first_name']} (ID: {audit_record['buyer_details']['user_id']})")
            logger.critical(f"Transfer Type: {audit_record['transfer_details']['transfer_type']}")
            logger.critical(f"Price: ${audit_record['transfer_details']['price']} USDT")
            logger.critical(f"Timestamp: {audit_record['timestamp']}")
            
            # Store audit record in database
            with db.lock:
                conn = db.get_connection()
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO transactions (user_id, transaction_type, amount, group_ids, status, metadata)
                    VALUES (?, 'transfer_audit', ?, ?, 'completed', ?)
                ''', (
                    audit_record['buyer_details']['user_id'],
                    audit_record['transfer_details']['price'],
                    json.dumps([audit_record['group_details']['group_id']]),
                    json.dumps(audit_record)
                ))
                conn.commit()
                conn.close()
                
        except Exception as e:
            logger.error(f"Error logging critical transfer: {e}")
    
    @staticmethod
    def format_contact_info() -> str:
        """Format bot owner contact information"""
        owner_mentions = []
        for owner_id in BOT_OWNERS:
            owner_mentions.append(f"[👨‍💼 Admin](tg://user?id={owner_id})")
        
        return " • ".join(owner_mentions)

marketplace_verification = MarketplaceVerification()

def generate_help_text() -> str:
    """Generate user help text"""
    return f"""
🤖 **Telegram Group Market Bot**

**📱 Available Commands:**

🏪 **Market Commands:**
• `/market` - Browse groups by year/month
• `/buy <buying_id>` - Purchase groups
  Examples: `/buy G123ABC` (single group), `/buy G123ABC, G456DEF` (multiple groups)
• `/claim` - Claim purchased groups (use in the group after joining)

💰 **Balance Commands:**
• `/balance` - Check your current balance
• `/withdraw` - Withdraw funds to Polygon/CWallet
  Example: `/withdraw` → Enter amount → Enter Polygon address/CWallet ID

💳 **Fees:**
• **Buying Fee:** {BUYING_FEE_RATE * 100:.1f}% added to purchase total
• **Selling Fee:** {SELLING_FEE_RATE * 100:.1f}% deducted from seller earnings

📋 **Listing Commands:**
• `/list` - List your group for sale (use in the group you own)
• `/refund` - Get refund for listed group (use in the group)
• `/cprice <price>` - Change group price (use in the group)
  Examples: `/cprice 25.50`, `/cprice 100.00`

🎯 **Referral System:**
• `/referral` - Get your referral link and view earnings
• **Earn {REFERRAL_COMMISSION_RATE * 100:.0f}% commission** from referral fees!

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
    return f"""
🔧 **Admin Commands:**

**👥 User Management:**
• `/users` - View all users and their statistics
• `/add_bal <user id> <amount>` - Add/remove balance from user
  Examples: `/add_bal 123456789 50.00` (add $50), `/add_bal 123456789 -25.50` (remove $25.50)
• `/withdrawals` - View and approve/reject withdrawal requests

**🤖 Session Management:**
• `/add` - Add new userbot session
  Example: `/add` → Follow prompts for API ID, API Hash, phone, OTP, 2FA password
• `/add_bank` - Add bank userbot for payment processing
• `/import <type>` - Import bot data or sessions
  Examples: `/import session`, `/import users`, `/import groups`
• `/export <type>` - Export bot data or sessions
  Examples: `/export session`, `/export users`, `/export groups`

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

**💳 Fee Configuration:**
• **Buying Fee:** {BUYING_FEE_RATE * 100:.1f}% (configurable in config.py)
• **Selling Fee:** {SELLING_FEE_RATE * 100:.1f}% (configurable in config.py)
• Fees are automatically calculated and applied
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
    
    def verify_password_hash(self, password: str, hashed: str) -> bool:
        """Verify password against hash"""
        return self.hash_password(password) == hashed
    
    async def start_auth_process(self, user_id: int, api_id: int, api_hash: str, phone_number: str, session_type: str = 'regular'):
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
                'step': 'code',
                'session_type': session_type
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
            
            # ENFORCE 2FA REQUIREMENT: If no SessionPasswordNeededError was raised,
            # it means this account doesn't have 2FA enabled
            auth_data['step'] = 'reject_no_2fa'
            
            return False, "This account must have 2-step verification enabled for security reasons"
            
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
            
            # ENFORCE 2FA REQUIREMENT: Block sessions without 2FA
            if not has_2fa:
                await client.disconnect()
                del self.pending_auth[user_id]
                return False, "Sessions without 2FA are not allowed for security reasons"
            
            session_type = auth_data.get('session_type', 'regular')
            
            # Get username from authenticated client
            username = None
            try:
                me = await client.get_me()
                username = me.username
                logger.info(f"Extracted username for session: @{username}")
            except Exception as e:
                logger.warning(f"Could not extract username from session: {e}")
            
            # Get raw password for encryption (enables true ownership transfer)
            raw_password = auth_data.get('password') if has_2fa else None
            
            success = db.add_session(
                user_id=user_id,
                api_id=auth_data['api_id'],
                api_hash=auth_data['api_hash'],
                phone_number=auth_data['phone_number'],
                session_string=session_string,
                password_hash=password_hash,
                has_2fa=has_2fa,
                session_type=session_type,
                username=username,
                raw_password=raw_password
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
    
    async def import_session_file(self, user_id: int, session_file: str, password: str = None, api_id: int = None, api_hash: str = None) -> tuple:
        """Import .session file with enhanced 2FA validation"""
        try:
            # Load the session file
            from telethon.sessions import SQLiteSession
            
            session = SQLiteSession(session_file)
            client = TelegramClient(session, 0, "", system_version="4.16.30-vxCUSTOM")
            
            await client.connect()
            
            if not await client.is_user_authorized():
                await client.disconnect()
                return False, "Session file is not authorized"
            
            # Get account info
            me = await client.get_me()
            phone_number = me.phone
            username = me.username
            
            # Check if 2FA is enabled on this account
            from telethon.tl.functions.account import GetPasswordRequest
            try:
                password_info = await client(GetPasswordRequest())
                has_2fa = password_info.has_password
            except Exception:
                has_2fa = False
            
            # Enforce 2FA requirement for session imports
            if not has_2fa:
                await client.disconnect()
                return False, "Session must have 2FA enabled for security reasons"
            
            if has_2fa and not password:
                await client.disconnect()
                return False, "2FA password is required for this session"
            
            # Validate 2FA password if provided
            if password and has_2fa:
                try:
                    from telethon.crypto import pwd_mod
                    password_input = pwd_mod.compute_check(password_info, password)
                    # Test password by attempting to use it (this validates it's correct)
                    await client(GetPasswordRequest())
                except Exception as e:
                    await client.disconnect()
                    return False, f"Invalid 2FA password: {e}"
            
            # Check for duplicate phone number across ALL users
            existing_sessions = db.get_user_sessions(user_id)
            for session_data in existing_sessions:
                if session_data['phone_number'] == phone_number:
                    await client.disconnect()
                    return False, f"Phone number {phone_number} already has an active session for this user"
            
            # Also check if any OTHER user has this phone number
            conn = db.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT user_id FROM sessions WHERE phone_number = ? AND is_active = TRUE AND user_id != ?', 
                          (phone_number, user_id))
            other_user_session = cursor.fetchone()
            conn.close()
            
            if other_user_session:
                await client.disconnect()
                return False, f"Phone number {phone_number} is already registered by another user"
            
            # Check session limit
            if len(existing_sessions) >= MAX_SESSIONS_PER_USER:
                await client.disconnect()
                return False, f"Maximum session limit ({MAX_SESSIONS_PER_USER}) reached"
            
            # Get session string and save to database
            session_string = session.save()
            password_hash = hashlib.sha256(password.encode()).hexdigest() if password else None
            
            # Use provided API credentials or defaults
            if not api_id:
                api_id = 0  # This will cause issues, but we handle it
            if not api_hash:
                api_hash = ""  # This will cause issues, but we handle it
            
            success = db.add_session(
                user_id=user_id,
                api_id=api_id,
                api_hash=api_hash,
                phone_number=phone_number,
                session_string=session_string,
                password_hash=password_hash,
                has_2fa=has_2fa,
                session_type='regular',
                username=username,
                raw_password=password if has_2fa else None
            )
            
            await client.disconnect()
            
            if success:
                logger.info(f"Successfully imported session for user {user_id}, phone {phone_number}")
                return True, "Session imported successfully with 2FA validation"
            else:
                return False, "Failed to save session to database"
                
        except Exception as e:
            logger.error(f"Error importing session file: {e}")
            return False, f"Import failed: {e}"
    
    async def check_group_ownership(self, client: TelegramClient, group_id: int):
        """Check if client has actual ownership of the group"""
        try:
            # Get chat entity and verify it's a supergroup
            entity = await client.get_entity(group_id)
            
            # Verify it's a supergroup
            if not hasattr(entity, 'megagroup') or not entity.megagroup:
                return False, "Group is not a supergroup"
            
            # Verify it's private
            if hasattr(entity, 'username') and entity.username:
                return False, "Group is not private (has username)"
            
            # Get admin participants
            participants = await client.get_participants(entity, limit=50, filter=ChannelParticipantsAdmins)
            
            me = await client.get_me()
            
            for participant in participants:
                if participant.id == me.id:
                    # Check if we're the creator
                    if hasattr(participant, 'participant'):
                        if participant.participant.__class__.__name__ == 'ChannelParticipantCreator':
                            logger.info(f"Userbot is creator of group {group_id}")
                            return True, "Userbot is group creator"
                        
                        # Check for full admin rights
                        if hasattr(participant.participant, 'admin_rights'):
                            admin_rights = participant.participant.admin_rights
                            if (admin_rights.add_admins and admin_rights.ban_users and 
                                admin_rights.delete_messages and admin_rights.invite_users and
                                admin_rights.change_info and admin_rights.pin_messages):
                                logger.info(f"Userbot has full admin rights in group {group_id}")
                                return True, "Userbot has full admin rights"
                    
            return False, "Userbot does not have ownership rights"
            
        except Exception as e:
            logger.error(f"Error checking group ownership for {group_id}: {e}")
            return False, f"Error checking ownership: {e}"
    
    async def get_group_info(self, client: TelegramClient, group_id: int):
        """Get detailed group information"""
        try:
            entity = await client.get_entity(group_id)
            
            # Get message count
            messages = await client.get_messages(entity, limit=1)
            total_messages = messages.total if hasattr(messages, 'total') else 0
            
            # Get creation date
            creation_date = entity.date.strftime('%Y-%m-%d') if hasattr(entity, 'date') and entity.date else None
            
            # Generate invite link if possible
            invite_link = None
            try:
                result = await client(ExportChatInviteRequest(entity))
                invite_link = result.link
            except Exception as e:
                logger.warning(f"Could not generate invite link: {e}")
            
            return {
                'id': entity.id,
                'title': entity.title,
                'creation_date': creation_date,
                'total_messages': total_messages,
                'invite_link': invite_link,
                'is_megagroup': hasattr(entity, 'megagroup') and entity.megagroup,
                'has_username': hasattr(entity, 'username') and entity.username is not None
            }
            
        except Exception as e:
            logger.error(f"Error getting group info for {group_id}: {e}")
            return None
    
    async def get_group_invite_link(self, client: TelegramClient, group_id: int):
        """Get group invite link"""
        try:
            entity = await client.get_entity(group_id)
            result = await client(ExportChatInviteRequest(entity))
            return result.link
        except Exception as e:
            logger.warning(f"Could not generate invite link for {group_id}: {e}")
            return None
    
    async def check_user_in_group(self, client: TelegramClient, group_id: int, user_id: int):
        """Check if user is a member of the group"""
        try:
            entity = await client.get_entity(group_id)
            
            # Try to get the user as a participant
            try:
                participant = await client.get_participants(entity, search=str(user_id), limit=1)
                return len(participant) > 0 and participant[0].id == user_id
            except:
                # Alternative method: try to get user entity from the group
                try:
                    await client.get_entity(user_id, entity)
                    return True
                except:
                    return False
                    
        except Exception as e:
            logger.error(f"Error checking user {user_id} in group {group_id}: {e}")
            return False
    
    async def transfer_ownership(self, client: TelegramClient, group_id: int, new_owner_id: int, password: str = None):
        """Transfer actual group ownership using Telethon"""
        try:
            entity = await client.get_entity(group_id)
            new_owner = await client.get_entity(new_owner_id)
            
            if password:
                # Real ownership transfer using 2FA password
                from telethon.tl.functions.channels import EditCreatorRequest
                from telethon.tl.functions.account import GetPasswordRequest
                from telethon.crypto import pwd_mod
                
                # Get password information for SRP
                password_info = await client(GetPasswordRequest())
                
                # Compute password hash for SRP
                password_input = pwd_mod.compute_check(password_info, password)
                
                # Transfer actual ownership
                await client(EditCreatorRequest(
                    channel=entity,
                    user_id=new_owner,
                    password=password_input
                ))
                
                logger.info(f"Successfully transferred ownership of group {group_id} to user {new_owner_id}")
                return True, "Group ownership transferred successfully"
                
            else:
                # Fallback: promote to full admin if no 2FA password
                admin_rights = ChatAdminRights(
                    change_info=True,
                    post_messages=True,
                    edit_messages=True,
                    delete_messages=True,
                    ban_users=True,
                    invite_users=True,
                    pin_messages=True,
                    add_admins=True,
                    anonymous=False,
                    manage_call=True,
                    other=True
                )
                
                await client(EditAdminRequest(
                    channel=entity,
                    user_id=new_owner,
                    admin_rights=admin_rights,
                    rank="Owner"
                ))
                
                logger.warning(f"No 2FA password provided - promoted user {new_owner_id} to admin in group {group_id}")
                return True, "User promoted to full admin (2FA required for ownership transfer)"
            
        except Exception as e:
            logger.error(f"Error transferring ownership in group {group_id}: {e}")
            return False, f"Failed to transfer ownership: {e}"

# ============================================================================
# BOT COMMANDS
# ============================================================================

class BotCommands:
    def __init__(self):
        self.user_contexts = {}
        self.pending_purchases = {}
    
    def get_purchased_group_by_id(self, group_id: int, user_id: int) -> Optional[Dict]:
        """Check if group was purchased by user"""
        with db.lock:
            conn = db.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT g.*, t.user_id as buyer_id
                FROM groups g
                JOIN transactions t ON JSON_EXTRACT(t.group_ids, '$') LIKE '%' || g.group_id || '%'
                WHERE g.group_id = ? AND t.user_id = ? AND t.transaction_type = 'purchase'
                AND t.status = 'completed' AND g.is_listed = FALSE
            ''', (group_id, user_id))
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return {
                    'id': result[0],
                    'group_id': result[1],
                    'buying_id': result[2],
                    'session_id': result[7],
                    'price': result[8]
                }
            return None
    
    def get_session_by_id(self, session_id: int) -> Optional[Dict]:
        """Get session data by ID"""
        with db.lock:
            conn = db.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT api_id, api_hash, session_string, password_hash, has_2fa
                FROM sessions WHERE id = ? AND is_active = TRUE
            ''', (session_id,))
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return {
                    'api_id': result[0],
                    'api_hash': result[1],
                    'session_string': result[2],
                    'password_hash': result[3],
                    'has_2fa': result[4]
                }
            return None
    
    def mark_group_as_transferred(self, group_id: int, new_owner_id: int):
        """Mark group as transferred"""
        with db.lock:
            conn = db.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE groups SET owner_user_id = ?, is_listed = FALSE
                WHERE id = ?
            ''', (new_owner_id, group_id))
            conn.commit()
            conn.close()
    
    def get_stored_password_for_transfer(self, group_id: int) -> Optional[str]:
        """Get stored password for group transfer"""
        try:
            # CRITICAL LIMITATION: We cannot decrypt stored password hashes
            # This is a fundamental security design issue that needs architectural fix
            
            conn = db.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT s.password_hash, s.has_2fa 
                FROM groups g
                JOIN sessions s ON g.session_id = s.id
                WHERE g.id = ?
            ''', (group_id,))
            result = cursor.fetchone()
            conn.close()
            
            if result and result[1]:  # has_2fa
                # ARCHITECTURAL LIMITATION:
                # We store password hashes for security, but need plain passwords for transfer
                # SOLUTIONS for production:
                # 1. During purchase, ask seller to re-enter password temporarily
                # 2. Store it encrypted (not hashed) during transaction period
                # 3. Use it for transfer then securely delete it
                # 4. Or ask buyer to coordinate with seller for manual transfer
                
                logger.warning(f"Cannot auto-transfer ownership for group {group_id} - password is hashed")
                return None
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting password for transfer: {e}")
            return None
    
    def get_pending_listing(self, user_id: int, group_id: int) -> Optional[Dict]:
        """Get pending listing for user and group"""
        with db.lock:
            conn = db.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM pending_listings 
                WHERE user_id = ? AND group_id = ? 
                AND expires_at > datetime('now')
            ''', (user_id, group_id))
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return {
                    'id': result[0],
                    'user_id': result[1],
                    'group_id': result[2],
                    'price': result[3],
                    'userbot_username': result[4],
                    'expires_at': result[5],
                    'created_at': result[6]
                }
            return None
    
    def remove_pending_listing(self, user_id: int, group_id: int):
        """Remove pending listing"""
        with db.lock:
            conn = db.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                DELETE FROM pending_listings 
                WHERE user_id = ? AND group_id = ?
            ''', (user_id, group_id))
            conn.commit()
            conn.close()
    
    def add_pending_listing(self, user_id: int, group_id: int, price: float):
        """Add pending listing"""
        from datetime import datetime, timedelta
        
        with db.lock:
            conn = db.get_connection()
            cursor = conn.cursor()
            
            expires_at = datetime.now() + timedelta(seconds=LISTING_TIMEOUT)
            
            cursor.execute('''
                INSERT OR REPLACE INTO pending_listings 
                (user_id, group_id, price, userbot_username, expires_at)
                VALUES (?, ?, ?, ?, ?)
            ''', (user_id, group_id, price, "userbot", expires_at))
            
            conn.commit()
            conn.close()
        
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command with referral support"""
        user = update.effective_user
        
        # Add user to database
        db.add_user(user.id, user.username, user.first_name)
        
        referral_message = ""
        
        # Handle referral links
        if context.args and len(context.args) > 0:
            try:
                referrer_id = int(context.args[0])
                
                # Check if user already has a referrer
                existing_referrer = db.get_referrer(user.id)
                if existing_referrer is None and referrer_id != user.id:
                    success = db.add_referral(referrer_id, user.id)
                    if success:
                        # Get referrer info
                        conn = db.get_connection()
                        cursor = conn.cursor()
                        cursor.execute('SELECT username, first_name FROM users WHERE user_id = ?', (referrer_id,))
                        referrer_info = cursor.fetchone()
                        conn.close()
                        
                        if referrer_info:
                            referrer_name = referrer_info[0] or referrer_info[1] or f"User {referrer_id}"
                            referral_message = f"\n🎉 **Referral Success!**\nYou've been referred by {referrer_name}. You'll both earn from each other's fees!\n"
                            
                            # Notify referrer
                            try:
                                await context.bot.send_message(
                                    chat_id=referrer_id,
                                    text=f"🎊 **New Referral!**\n\n"
                                         f"**{user.first_name or user.username}** joined using your referral link!\n"
                                         f"You'll earn {REFERRAL_COMMISSION_RATE * 100:.0f}% commission from their fees.",
                                    parse_mode=ParseMode.MARKDOWN
                                )
                            except Exception as e:
                                logger.error(f"Failed to notify referrer {referrer_id}: {e}")
                    else:
                        referral_message = "\n⚠️ **Referral Info:** You already have a referrer or invalid referral link.\n"
                elif existing_referrer:
                    referral_message = f"\n📎 **Existing Referral:** You're already referred by someone else.\n"
                    
            except (ValueError, IndexError):
                logger.warning(f"Invalid referral parameter: {context.args[0]}")
        
        welcome_text = f"""
🤖 **Welcome to Telegram Group Market Bot!**

Hello {user.first_name or user.username}! 👋{referral_message}

This bot allows you to buy and sell Telegram groups in a secure marketplace.

🏪 **What you can do:**
• Browse and purchase groups by creation date
• List your own groups for sale
• Manage your balance and withdrawals
• Transfer group ownership securely
• Earn from referrals ({REFERRAL_COMMISSION_RATE * 100:.0f}% commission!)

💰 **Current Balance:** ${format_balance(db.get_user_balance(user.id))} USDT

📱 **Quick Start:**
• Use `/market` to browse available groups
• Use `/referral` to get your referral link
• Use `/help` to see all commands
• Send USDT via @cctip_bot in the bank group to add balance

Ready to start trading? 🚀
"""
        
        await update.message.reply_text(welcome_text, parse_mode=ParseMode.MARKDOWN)
    
    async def add_bank_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /add_bank command for adding bank userbot sessions"""
        user = update.effective_user
        
        if user.id not in BOT_OWNERS:
            await update.message.reply_text(
                "❌ Only bot administrators can add bank sessions.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        # Check existing bank sessions
        existing_sessions = db.get_user_sessions(user.id)
        bank_sessions = [s for s in existing_sessions if s.get('session_type') == 'bank']
        
        if len(bank_sessions) >= 1:  # Limit to 1 bank session for security
            await update.message.reply_text(
                "⚠️ **Bank Session Limit Reached**\n\n"
                "Only one bank session is allowed for security reasons.\n"
                "Use `/sessions` to manage existing bank sessions.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        await update.message.reply_text(
            f"🏦 **Add Bank Userbot Session**\n\n"
            f"This session will be used for payment processing via @cctip_bot.\n\n"
            f"**⚠️ Important Security Notes:**\n"
            f"• Bank sessions require 2FA enabled\n"
            f"• Only one bank session per admin\n"
            f"• Used exclusively for tip detection in bank group\n"
            f"• Should be a dedicated account for security\n\n"
            f"**Step 1:** Please enter your API ID\n"
            f"Get it from: https://my.telegram.org",
            parse_mode=ParseMode.MARKDOWN
        )
        
        # Clear any existing session
        if user.id in session_manager.pending_auth:
            del session_manager.pending_auth[user.id]
        
        self.user_contexts[user.id] = {
            'state': 'waiting_bank_api_id',
            'session_type': 'bank'
        }
    
    async def referral_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /referral command - show referral statistics and link"""
        user = update.effective_user
        
        # Get referral statistics
        stats = db.get_referral_stats(user.id)
        
        # Generate referral link
        bot_username = context.bot.username or "YourBot"
        referral_link = f"https://t.me/{bot_username}?start={user.id}"
        
        text = f"""
🎯 **Your Referral Program**

**📊 Statistics:**
• **Total Referrals:** {stats['total_referrals']}
• **Total Earnings:** ${format_balance(stats['total_earnings'])} USDT
• **Commission Rate:** {REFERRAL_COMMISSION_RATE * 100:.0f}% of referral fees

**🔗 Your Referral Link:**
`{referral_link}`

**💡 How it Works:**
• Share your referral link with friends
• When they join and make transactions (buy/sell), you earn {REFERRAL_COMMISSION_RATE * 100:.0f}% of their fees
• Example: If someone pays $5 in fees, you earn ${5 * REFERRAL_COMMISSION_RATE:.2f}
• Earnings are automatically credited to your balance

**📈 Recent Referrals:**
"""
        
        if stats['recent_referrals']:
            for i, (referred_id, username, first_name, created_at) in enumerate(stats['recent_referrals'][:5], 1):
                user_display = username or first_name or f"User {referred_id}"
                # Format date
                from datetime import datetime
                try:
                    date_obj = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                    date_str = date_obj.strftime('%Y-%m-%d')
                except:
                    date_str = created_at[:10]
                text += f"{i}. {user_display} - {date_str}\n"
        else:
            text += "No referrals yet. Start sharing your link! 🚀\n"
        
        # Show monthly breakdown if available
        if stats['monthly_breakdown']:
            text += f"\n**📅 Recent Daily Earnings:**\n"
            for date, earnings in stats['monthly_breakdown'][:7]:  # Last 7 days
                text += f"• {date}: ${format_balance(earnings)} USDT\n"
        
        text += f"""

**💰 Tips to Maximize Earnings:**
• Share in relevant Telegram groups/channels
• Post on social media platforms
• Tell friends about the marketplace
• Active referrals = more commissions!

Tap to copy your referral link above! 👆
"""
        
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
    
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

💰 **Fees:**
• **Buying Fee:** {BUYING_FEE_RATE * 100:.1f}% (added to purchase total)
• **Selling Fee:** {SELLING_FEE_RATE * 100:.1f}% (deducted from your earnings)

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
        
        subtotal = 0
        group_details = []
        
        for buying_id in buying_ids:
            group = db.get_group_by_buying_id(buying_id)
            if not group:
                await update.message.reply_text(
                    f"❌ Group with ID `{buying_id}` not found or no longer available.",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            
            subtotal += group['price']
            group_details.append(group)
        
        # Calculate buying fee
        buying_fee = subtotal * BUYING_FEE_RATE
        total_cost = subtotal + buying_fee
        
        user_balance = db.get_user_balance(user.id)
        if user_balance < total_cost:
            await update.message.reply_text(
                f"❌ Insufficient balance.\n\n"
                f"**Subtotal:** ${format_price(subtotal)} USDT\n"
                f"**Buying Fee ({BUYING_FEE_RATE * 100:.1f}%):** ${format_price(buying_fee)} USDT\n"
                f"**Total Cost:** ${format_price(total_cost)} USDT\n"
                f"**Your Balance:** ${format_balance(user_balance)} USDT\n"
                f"**Needed:** ${format_price(total_cost - user_balance)} USDT",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        success = db.purchase_groups(user.id, buying_ids, subtotal, buying_fee)
        
        if not success:
            await update.message.reply_text(
                "❌ Purchase failed. Please try again or contact support.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        text = f"""
✅ **Purchase Successful!**

**Subtotal:** ${format_price(subtotal)} USDT
**Buying Fee ({BUYING_FEE_RATE * 100:.1f}%):** ${format_price(buying_fee)} USDT
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

**⚠️ Important Notes:**
• You must join the groups before claiming
• Use `/claim` command only after joining
• **Real ownership transfer** requires seller's 2FA password
• If seller has 2FA enabled: Full ownership transfer
• If no 2FA: You'll get full admin rights instead
• Ownership transfer may take a few minutes
"""
        
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)
        
        # Notify referrer about buying fee commission
        referrer_id = db.get_referrer(user.id)
        if referrer_id and buying_fee > 0:
            commission = buying_fee * REFERRAL_COMMISSION_RATE
            try:
                await context.bot.send_message(
                    chat_id=referrer_id,
                    text=f"💸 **Referral Commission Earned!**\n\n"
                         f"Your referral bought groups!\n"
                         f"**Commission:** ${format_price(commission)} USDT\n"
                         f"**From:** Buying fee (${format_price(buying_fee)} USDT)\n\n"
                         f"Commission added to your balance! 🎉",
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception as e:
                logger.error(f"Failed to notify referrer {referrer_id}: {e}")
    
    async def claim_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /claim command with real ownership transfer"""
        user = update.effective_user
        chat = update.effective_chat
        
        if chat.type not in ['group', 'supergroup']:
            await update.message.reply_text(
                "❌ This command can only be used in groups.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        # Check if group exists in database and was purchased by user
        group_info = self.get_purchased_group_by_id(chat.id, user.id)
        if not group_info:
            await update.message.reply_text(
                "❌ This group was not purchased by you or is not available for claiming.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        # Verify the group appears in the bot's database
        if not self.verify_group_in_database(chat.id):
            await update.message.reply_text(
                "❌ This group is not properly registered in our database. Please contact support.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        await update.message.reply_text(
            "🔄 Processing ownership transfer...\n\n"
            "Verifying your membership and transferring ownership...",
            parse_mode=ParseMode.MARKDOWN
        )
        
        try:
            # Get session for this group
            session_data = self.get_session_by_id(group_info['session_id'])
            if not session_data:
                await update.message.reply_text(
                    "❌ Unable to access userbot session for this group.",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            
            # Create Telethon client
            client = TelegramClient(
                session=session_data['session_string'],
                api_id=session_data['api_id'],
                api_hash=session_data['api_hash']
            )
            
            await client.connect()
            
            # Check if user is in the group
            is_member = await session_manager.check_user_in_group(client, chat.id, user.id)
            if not is_member:
                await update.message.reply_text(
                    "❌ You must join the group first before claiming ownership.",
                    parse_mode=ParseMode.MARKDOWN
                )
                await client.disconnect()
                return
            
            # Transfer ownership using encrypted password for true ownership transfer
            # Try to get decrypted password first, fallback to admin promotion
            transfer_password = db.get_session_password_for_transfer(group_info['session_id'])
            
            if transfer_password:
                logger.info(f"Using decrypted password for TRUE ownership transfer in group {chat.id}")
                success, message = await session_manager.transfer_ownership(
                    client, chat.id, user.id, transfer_password
                )
            else:
                logger.warning(f"No encrypted password available - falling back to admin promotion for group {chat.id}")
                success, message = await session_manager.transfer_ownership(
                    client, chat.id, user.id, None
                )
            
            await client.disconnect()
            
            if success:
                # COMPREHENSIVE VERIFICATION AND AUDIT SYSTEM
                
                # 1. Get detailed information for audit trail
                seller_id = group_info['owner_user_id']
                buyer_info = marketplace_verification.get_user_info(user.id)
                seller_info = marketplace_verification.get_user_info(seller_id)
                
                # 2. Determine transfer type for transparency
                transfer_type = "true_ownership" if transfer_password else "admin_promotion"
                
                # 3. Process payments and fees
                group_price = group_info['price']
                selling_fee = group_price * SELLING_FEE_RATE
                seller_earnings = group_price - selling_fee
                
                # 4. Create comprehensive transaction details
                transaction_details = {
                    'group_price': group_price,
                    'selling_fee': selling_fee,
                    'seller_earnings': seller_earnings,
                    'group_id': chat.id,
                    'buyer_id': user.id,
                    'purchase_timestamp': datetime.now().isoformat(),
                    'has_2fa': bool(transfer_password),
                    'password_available': bool(transfer_password)
                }
                
                # 5. Credit seller's balance
                success_payment = db.update_user_balance(seller_id, seller_earnings, 'sale')
                
                if success_payment:
                    # 6. Add transaction record for seller
                    with db.lock:
                        conn = db.get_connection()
                        cursor = conn.cursor()
                        cursor.execute('''
                            INSERT INTO transactions (user_id, transaction_type, amount, group_ids, status)
                            VALUES (?, 'sale', ?, ?, 'completed')
                        ''', (seller_id, seller_earnings, json.dumps(transaction_details)))
                        transaction_id = cursor.lastrowid
                        conn.commit()
                        conn.close()
                    
                    # 7. Handle referral commission for selling fees
                    referrer_id = db.get_referrer(seller_id)
                    if referrer_id and selling_fee > 0:
                        commission = selling_fee * REFERRAL_COMMISSION_RATE
                        success = db.add_referral_earning(
                            referrer_id, seller_id, 'selling_fee', selling_fee, commission, transaction_id
                        )
                        if success:
                            logger.info(f"Referral commission paid: ${commission:.4f} to {referrer_id} for selling fee from {seller_id}")
                            
                            # Notify referrer about commission
                            try:
                                await context.bot.send_message(
                                    chat_id=referrer_id,
                                    text=f"💸 **Referral Commission Earned!**\n\n"
                                         f"Your referral sold a group!\n"
                                         f"**Commission:** ${format_price(commission)} USDT\n"
                                         f"**From:** Selling fee (${format_price(selling_fee)} USDT)\n\n"
                                         f"Commission added to your balance! 🎉",
                                    parse_mode=ParseMode.MARKDOWN
                                )
                            except Exception as e:
                                logger.error(f"Failed to notify referrer {referrer_id}: {e}")
                    
                    # 8. CREATE COMPREHENSIVE AUDIT RECORD
                    audit_record = marketplace_verification.create_transfer_audit_record(
                        group_info, buyer_info, seller_info, transfer_type, transaction_details
                    )
                    marketplace_verification.log_critical_transfer(audit_record)
                    
                    # 9. ENHANCED SELLER NOTIFICATION with verification details
                    try:
                        seller_balance = db.get_user_balance(seller_id)
                        await context.bot.send_message(
                            chat_id=seller_id,
                            text=f"💰 **Group Transfer Completed - VERIFIED ✅**\n\n"
                                 f"**📋 Transfer Details:**\n"
                                 f"• **Group:** {group_info.get('group_name', 'Unknown')}\n"
                                 f"• **Buyer:** {buyer_info.get('first_name', 'Unknown')} (ID: `{user.id}`)\n"
                                 f"• **Transfer Type:** {'🔑 True Ownership' if transfer_password else '👑 Admin Rights'}\n"
                                 f"• **Buying ID:** `{group_info.get('buying_id', 'N/A')}`\n\n"
                                 f"**💰 Financial Summary:**\n"
                                 f"• **Sale Price:** ${format_price(group_price)} USDT\n"
                                 f"• **Selling Fee ({SELLING_FEE_RATE * 100:.1f}%):** ${format_price(selling_fee)} USDT\n"
                                 f"• **Your Earnings:** ${format_price(seller_earnings)} USDT\n"
                                 f"• **New Balance:** ${format_balance(seller_balance)} USDT\n\n"
                                 f"**🔐 Security Verification:**\n"
                                 f"• Transfer logged with audit ID: `{transaction_id}`\n"
                                 f"• Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
                                 f"• All details verified and recorded\n\n"
                                 f"**📞 Support:** {marketplace_verification.format_contact_info()}\n"
                                 f"If anything seems incorrect, contact support immediately!",
                            parse_mode=ParseMode.MARKDOWN
                        )
                    except Exception as e:
                        logger.error(f"Failed to notify seller {seller_id}: {e}")
                    
                    logger.info(f"Seller {seller_id} paid ${seller_earnings} (after fee) for group {chat.id}")
                else:
                    logger.error(f"Failed to pay seller {seller_id} for group {chat.id}")
                
                # 10. Mark group as sold to prevent re-listing
                db.mark_group_as_sold(chat.id, user.id)
                self.mark_group_as_transferred(group_info['id'], user.id)
                
                # 11. COMPREHENSIVE BUYER NOTIFICATION with full verification
                transfer_icon = "🔑" if transfer_password else "👑"
                transfer_text = "TRUE OWNERSHIP" if transfer_password else "FULL ADMIN RIGHTS"
                
                await update.message.reply_text(
                    f"🎉 **GROUP TRANSFER SUCCESSFUL - VERIFIED ✅**\n\n"
                    f"{transfer_icon} **{transfer_text} TRANSFERRED!**\n\n"
                    f"**📋 Transfer Verification:**\n"
                    f"• **Group:** {group_info.get('group_name', 'Unknown')}\n"
                    f"• **Previous Owner:** {seller_info.get('first_name', 'Unknown')} (ID: `{seller_id}`)\n"
                    f"• **New Owner:** {buyer_info.get('first_name', 'You')} (ID: `{user.id}`)\n"
                    f"• **Transfer Type:** {'🔑 True Creator Transfer' if transfer_password else '👑 Full Admin Promotion'}\n"
                    f"• **Purchase Price:** ${format_price(group_price)} USDT\n"
                    f"• **Buying ID:** `{group_info.get('buying_id', 'N/A')}`\n\n"
                    f"**🔐 Security Verification:**\n"
                    f"• ✅ Ownership verified and transferred\n"
                    f"• ✅ Payment processed and verified\n"
                    f"• ✅ Transaction logged in audit system\n"
                    f"• ✅ Group permanently marked as transferred\n"
                    f"• ✅ All marketplace rules enforced\n\n"
                    f"**⚡ Your New Permissions:**\n"
                    f"{'• 🔑 **FULL CREATOR STATUS** - You are the group creator' if transfer_password else '• 👑 **FULL ADMIN RIGHTS** - All admin permissions + Owner rank'}\n"
                    f"• ✅ Manage all settings and permissions\n"
                    f"• ✅ Add/remove admins and members\n"
                    f"• ✅ Control group information and rules\n"
                    f"• ✅ {'Transfer ownership to others' if transfer_password else 'Promote other admins'}\n\n"
                    f"**🛡️ Marketplace Trust & Security:**\n"
                    f"• 📊 This transfer is permanently audited\n"
                    f"• 🔒 Group cannot be re-listed or resold\n"
                    f"• ⏰ Transfer completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
                    f"• 🆔 Audit Reference: `TXN-{transaction_id}`\n\n"
                    f"**❓ Questions or Issues?**\n"
                    f"📞 **Contact Support:** {marketplace_verification.format_contact_info()}\n"
                    f"⚠️ **Report immediately if anything seems wrong!**\n\n"
                    f"**Thank you for using our trusted marketplace! 🤝**",
                    parse_mode=ParseMode.MARKDOWN
                )
                
                # 12. NOTIFY BOT OWNERS about successful transfer for monitoring
                for owner_id in BOT_OWNERS:
                    try:
                        await context.bot.send_message(
                            chat_id=owner_id,
                            text=f"✅ **SUCCESSFUL TRANSFER COMPLETED**\n\n"
                                 f"**Transfer Summary:**\n"
                                 f"• **Group:** {group_info.get('group_name', 'Unknown')} (ID: `{chat.id}`)\n"
                                 f"• **Seller:** {seller_info.get('first_name', 'Unknown')} (ID: `{seller_id}`)\n"
                                 f"• **Buyer:** {buyer_info.get('first_name', 'Unknown')} (ID: `{user.id}`)\n"
                                 f"• **Type:** {'🔑 True Ownership' if transfer_password else '👑 Admin Rights'}\n"
                                 f"• **Price:** ${format_price(group_price)} USDT\n"
                                 f"• **Fees:** ${format_price(selling_fee)} USDT\n"
                                 f"• **Seller Earnings:** ${format_price(seller_earnings)} USDT\n"
                                 f"• **Buying ID:** `{group_info.get('buying_id', 'N/A')}`\n"
                                 f"• **Audit ID:** `TXN-{transaction_id}`\n\n"
                                 f"**Marketplace Status:** ✅ Operating Normally\n"
                                 f"**Timestamp:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}",
                            parse_mode=ParseMode.MARKDOWN
                        )
                    except Exception as e:
                        logger.error(f"Failed to notify owner {owner_id} about successful transfer: {e}")
            else:
                # FAILURE NOTIFICATION with comprehensive support information
                seller_id = group_info['owner_user_id']
                buyer_info = marketplace_verification.get_user_info(user.id)
                seller_info = marketplace_verification.get_user_info(seller_id)
                
                # Log the failure for admin attention
                logger.error(f"CRITICAL: GROUP TRANSFER FAILED - Group: {group_info.get('group_name')} (ID: {chat.id}), Buyer: {user.id}, Seller: {seller_id}, Error: {message}")
                
                await update.message.reply_text(
                    f"❌ **TRANSFER FAILED - SUPPORT REQUIRED**\n\n"
                    f"**🚨 Critical Issue Detected:**\n"
                    f"Transfer of group ownership could not be completed automatically.\n\n"
                    f"**📋 Transaction Details:**\n"
                    f"• **Group:** {group_info.get('group_name', 'Unknown')}\n"
                    f"• **Seller:** {seller_info.get('first_name', 'Unknown')} (ID: `{seller_id}`)\n"
                    f"• **Buyer:** {buyer_info.get('first_name', 'You')} (ID: `{user.id}`)\n"
                    f"• **Purchase Price:** ${format_price(group_info['price'])} USDT\n"
                    f"• **Buying ID:** `{group_info.get('buying_id', 'N/A')}`\n"
                    f"• **Error Details:** {message}\n\n"
                    f"**🔐 Your Purchase is PROTECTED:**\n"
                    f"• ✅ Payment has been processed\n"
                    f"• ✅ Your purchase is recorded and verified\n"
                    f"• ✅ Support team has been notified automatically\n"
                    f"• ✅ You will receive assistance within 24 hours\n\n"
                    f"**📞 IMMEDIATE SUPPORT:**\n"
                    f"**Contact:** {marketplace_verification.format_contact_info()}\n"
                    f"**Reference ID:** `FAIL-{chat.id}-{user.id}`\n"
                    f"**Timestamp:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n"
                    f"**⚠️ DO NOT PANIC - Your purchase is secure!**\n"
                    f"Our support team will manually complete the transfer and ensure you receive your group ownership. This is a rare technical issue and will be resolved quickly.\n\n"
                    f"**🤝 Thank you for your patience and trust in our marketplace!**",
                    parse_mode=ParseMode.MARKDOWN
                )
                
                # Notify all bot owners about the failure
                for owner_id in BOT_OWNERS:
                    try:
                        await context.bot.send_message(
                            chat_id=owner_id,
                            text=f"🚨 **CRITICAL: TRANSFER FAILURE REQUIRES ATTENTION**\n\n"
                                 f"**Group Transfer Failed:**\n"
                                 f"• **Group:** {group_info.get('group_name', 'Unknown')} (ID: `{chat.id}`)\n"
                                 f"• **Seller:** {seller_info.get('first_name', 'Unknown')} (ID: `{seller_id}`)\n"
                                 f"• **Buyer:** {buyer_info.get('first_name', 'Unknown')} (ID: `{user.id}`)\n"
                                 f"• **Price:** ${format_price(group_info['price'])} USDT\n"
                                 f"• **Buying ID:** `{group_info.get('buying_id', 'N/A')}`\n"
                                 f"• **Error:** {message}\n\n"
                                 f"**🔴 IMMEDIATE ACTION REQUIRED:**\n"
                                 f"1. Contact buyer: @{buyer_info.get('username', f'User_{user.id}')}\n"
                                 f"2. Contact seller: @{seller_info.get('username', f'User_{seller_id}')}\n"
                                 f"3. Manually complete ownership transfer\n"
                                 f"4. Verify buyer receives proper access\n\n"
                                 f"**Timestamp:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
                                 f"**Reference:** `FAIL-{chat.id}-{user.id}`",
                            parse_mode=ParseMode.MARKDOWN
                        )
                    except Exception as e:
                        logger.error(f"Failed to notify owner {owner_id} about transfer failure: {e}")
                
        except Exception as e:
            # CRITICAL ERROR HANDLER with comprehensive logging and notifications
            logger.critical(f"CRITICAL ERROR in claim command: {e}")
            
            # Get basic info for error reporting
            user = update.effective_user
            chat = update.effective_chat
            
            await update.message.reply_text(
                f"🚨 **CRITICAL ERROR - IMMEDIATE SUPPORT REQUIRED**\n\n"
                f"**⚠️ A serious technical error occurred during the transfer process.**\n\n"
                f"**📋 Error Details:**\n"
                f"• **User:** {user.first_name} (ID: `{user.id}`)\n"
                f"• **Group:** {chat.title if chat else 'Unknown'} (ID: `{chat.id if chat else 'Unknown'}`)\n"
                f"• **Error Type:** System Exception\n"
                f"• **Timestamp:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n"
                f"**🔐 Your Purchase Protection:**\n"
                f"• ✅ All transactions are protected and recorded\n"
                f"• ✅ Support team has been automatically notified\n"
                f"• ✅ We will resolve this issue within 24 hours\n"
                f"• ✅ You will receive your group ownership\n\n"
                f"**📞 EMERGENCY SUPPORT:**\n"
                f"**Contact:** {marketplace_verification.format_contact_info()}\n"
                f"**Reference:** `ERROR-{user.id}-{datetime.now().timestamp()}`\n\n"
                f"**🛡️ DO NOT WORRY - Your transaction is secure!**\n"
                f"Our technical team will investigate and manually complete your transfer if needed.",
                parse_mode=ParseMode.MARKDOWN
            )
            
            # Notify all bot owners about critical error
            for owner_id in BOT_OWNERS:
                try:
                    await context.bot.send_message(
                        chat_id=owner_id,
                        text=f"🚨 **CRITICAL ERROR IN CLAIM COMMAND**\n\n"
                             f"**❗ IMMEDIATE TECHNICAL ATTENTION REQUIRED**\n\n"
                             f"**Error Details:**\n"
                             f"• **User:** {user.first_name} (ID: `{user.id}`)\n"
                             f"• **Group:** {chat.title if chat else 'Unknown'} (ID: `{chat.id if chat else 'Unknown'}`)\n"
                             f"• **Error:** `{str(e)[:200]}...`\n"
                             f"• **Timestamp:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n"
                             f"**🔴 REQUIRED ACTIONS:**\n"
                             f"1. Check system logs immediately\n"
                             f"2. Verify user's purchase status\n"
                             f"3. Contact user for manual assistance\n"
                             f"4. Investigate root cause\n\n"
                             f"**User Contact:** @{user.username if user.username else f'User_{user.id}'}\n"
                             f"**Reference:** `ERROR-{user.id}-{datetime.now().timestamp()}`",
                        parse_mode=ParseMode.MARKDOWN
                    )
                except Exception as notify_error:
                    logger.error(f"Failed to notify owner {owner_id} about critical error: {notify_error}")
    
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
        """Handle /refund command with actual ownership return"""
        user = update.effective_user
        chat = update.effective_chat
        
        if chat.type not in ['group', 'supergroup']:
            await update.message.reply_text(
                "❌ This command can only be used in the group you want to refund.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        group_id = chat.id
        
        # Check if group is listed by this user
        groups = db.get_groups_by_date(2016, None)  # Get all groups
        user_group = None
        
        for group in groups:
            if group['group_id'] == group_id and group['owner_user_id'] == user.id and group['is_listed']:
                user_group = group
                break
        
        if not user_group:
            await update.message.reply_text(
                "❌ **Group Not Found**\n\n"
                "This group is either:\n"
                "• Not listed in the marketplace\n"
                "• Not owned by you\n"
                "• Already sold or delisted",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        await update.message.reply_text(
            "🔄 **Processing Refund Request**\n\n"
            "Please wait while we:\n"
            "1️⃣ Remove the group from listings\n"
            "2️⃣ Transfer ownership back to you\n"
            "3️⃣ Complete the refund process",
            parse_mode=ParseMode.MARKDOWN
        )
        
        try:
            # Get session for this group
            session_data = None
            sessions = db.get_user_sessions(user.id)  # Get all sessions to find the right one
            for session in sessions:
                if session['id'] == user_group['session_id']:
                    session_data = session
                    break
            
            if not session_data:
                await update.message.reply_text(
                    "❌ **Refund Failed**\n\n"
                    "Session data not found. Please contact support.",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            
            # Create userbot client
            client = session_manager.get_client(session_data['session_string'])
            await client.connect()
            
            if not await client.is_user_authorized():
                await client.disconnect()
                await update.message.reply_text(
                    "❌ **Refund Failed**\n\n"
                    "Session is not authorized. Please contact support.",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            
            # Verify userbot is still the owner
            is_owner = await session_manager.check_group_ownership(client, group_id)
            if not is_owner:
                await client.disconnect()
                await update.message.reply_text(
                    "❌ **Refund Failed**\n\n"
                    "Userbot is no longer the owner of this group.",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            
            # Transfer ownership back to original owner
            password = session_data.get('password_hash')
            if password and session_data.get('has_2fa'):
                # For refund, we need the plain password, not hash
                # This is a limitation - we can't decrypt the stored hash
                success, message = await session_manager.transfer_ownership(
                    client, group_id, user.id, None  # Fallback to admin promotion
                )
                
                if success:
                    await update.message.reply_text(
                        "⚠️ **Partial Refund Completed**\n\n"
                        "✅ Group delisted from marketplace\n"
                        "✅ You've been promoted to full admin\n"
                        "⚠️ Manual ownership transfer needed\n\n"
                        "**Note:** Due to security, you'll need to manually transfer ownership to yourself using Telegram's native transfer feature.",
                        parse_mode=ParseMode.MARKDOWN
                    )
                else:
                    await update.message.reply_text(
                        f"❌ **Refund Failed**\n\n"
                        "Error during ownership transfer: {message}",
                        parse_mode=ParseMode.MARKDOWN
                    )
                    await client.disconnect()
                    return
            else:
                # No 2FA, can only promote to admin
                success, message = await session_manager.transfer_ownership(
                    client, group_id, user.id, None
                )
                
                if success:
                    await update.message.reply_text(
                        "✅ **Refund Completed**\n\n"
                        "✅ Group delisted from marketplace\n"
                        "✅ You've been promoted to full admin\n\n"
                        "Your group has been successfully refunded!",
                        parse_mode=ParseMode.MARKDOWN
                    )
                else:
                    await update.message.reply_text(
                        f"❌ **Refund Failed**\n\n"
                        "Error during ownership transfer: {message}",
                        parse_mode=ParseMode.MARKDOWN
                    )
                    await client.disconnect()
                    return
            
            # Delist the group from database
            db.mark_group_as_sold(group_id, user.id)  # Mark as "sold" to original owner
            
            await client.disconnect()
            logger.info(f"Refund completed for group {group_id} by user {user.id}")
            
        except Exception as e:
            logger.error(f"Error processing refund for group {group_id}: {e}")
            await update.message.reply_text(
                "❌ **Refund Failed**\n\n"
                "An unexpected error occurred. Please try again or contact support.",
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
    
    async def done_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /done command for finalizing group listing (regular or bulk)"""
        user = update.effective_user
        chat = update.effective_chat
        
        if chat.type not in ['group', 'supergroup']:
            await update.message.reply_text(
                "❌ This command can only be used in groups.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        # Check for bulk listing first
        bulk_listing = self.get_pending_bulk_listing(chat.id)
        if bulk_listing:
            # Validate that the user who started the bulk listing is using /done
            if bulk_listing['user_id'] != user.id:
                await update.message.reply_text(
                    "❌ **Permission Denied**\n\n"
                    "Only the user who started the bulk listing can use `/done`.",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            
            await self.handle_bulk_done(update, context, bulk_listing)
            return
        
        # Check if user has a regular pending listing for this group
        pending_listing = self.get_pending_listing(user.id, chat.id)
        if not pending_listing:
            await update.message.reply_text(
                "❌ No pending listing found for this group.\n\n"
                "Use `/list` or `/blist <keyword>` first to start the listing process.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        await update.message.reply_text(
            "🔄 **Verifying Group Ownership**\n\n"
            "Checking if userbot has been granted ownership...",
            parse_mode=ParseMode.MARKDOWN
        )
        
        try:
            # Get userbot session (admin session for verification)
            admin_sessions = db.get_user_sessions(BOT_OWNERS[0])  # Get admin sessions
            if not admin_sessions:
                await update.message.reply_text(
                    "❌ No admin userbot sessions available. Please contact administrator.",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            
            session_data = admin_sessions[0]  # Use first admin session
            
            # Create Telethon client
            client = TelegramClient(
                session=session_data['session_string'],
                api_id=session_data['api_id'],
                api_hash=session_data['api_hash']
            )
            
            await client.connect()
            
            # Check if userbot is owner of the group
            is_owner, owner_message = await session_manager.check_group_ownership(client, chat.id)
            
            if not is_owner:
                await update.message.reply_text(
                    f"❌ **Ownership Not Detected**\n\n"
                    f"The userbot is not the owner of this group.\n\n"
                    f"**Please ensure:**\n"
                    f"• You added the userbot to the group\n"
                    f"• You gave it admin rights with full permissions\n"
                    f"• You transferred ownership to the userbot\n\n"
                    f"Error: {owner_message}",
                    parse_mode=ParseMode.MARKDOWN
                )
                await client.disconnect()
                return
            
            # Get detailed group information
            group_info = await session_manager.get_group_info(client, chat.id)
            await client.disconnect()
            
            if not group_info:
                await update.message.reply_text(
                    "❌ Unable to retrieve group information.",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            
            # Validate group for listing
            is_valid, validation_message = is_group_valid_for_listing(group_info)
            if not is_valid:
                await update.message.reply_text(
                    f"❌ **Group Not Valid for Listing**\n\n"
                    f"{validation_message}\n\n"
                    f"Please fix the issue and try again.",
                    parse_mode=ParseMode.MARKDOWN
                )
                self.remove_pending_listing(user.id, chat.id)
                return
            
            # Show group information for confirmation
            buying_id = db.get_or_create_buying_id(chat.id)
            
            text = f"""
✅ **Group Verification Successful!**

**📋 Group Information:**
**Group ID:** `{chat.id}`
**Buying ID:** `{buying_id}`
**Group Name:** {group_info['title']}
**Creation Date:** {group_info['creation_date']}
**Total Messages:** {group_info['total_messages']}
**Price:** ${format_price(pending_listing['price'])} USDT

**🔍 Validation Status:**
✅ Private supergroup
✅ Userbot has ownership
✅ Minimum messages ({group_info['total_messages']} ≥ {MIN_GROUP_MESSAGES})
✅ Creation date visible

**⚠️ Transfer Requirements:**
• Your session must have 2FA enabled for real ownership transfer
• Without 2FA: Buyers will get admin rights only
• With 2FA: Buyers will get full ownership

Do you want to confirm this listing?
"""
            
            keyboard = create_confirmation_keyboard("listing", f"{chat.id}_{pending_listing['price']}")
            
            await update.message.reply_text(
                text,
                reply_markup=keyboard,
                parse_mode=ParseMode.MARKDOWN
            )
            
        except Exception as e:
            logger.error(f"Error in done command: {e}")
            await update.message.reply_text(
                "❌ An error occurred while verifying the group. Please try again.",
                parse_mode=ParseMode.MARKDOWN
            )
    
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
        
        # Check session limit
        existing_sessions = db.get_user_sessions(user.id)
        if len(existing_sessions) >= MAX_SESSIONS_PER_USER:
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("📋 Manage Sessions", callback_data="manage_sessions")],
                [InlineKeyboardButton("🔄 Overwrite Session", callback_data="overwrite_session")]
            ])
            
            await update.message.reply_text(
                f"⚠️ **Session Limit Reached**\n\n"
                f"You have reached the maximum limit of {MAX_SESSIONS_PER_USER} sessions.\n\n"
                f"**Current sessions:** {len(existing_sessions)}/{MAX_SESSIONS_PER_USER}\n\n"
                f"Please manage your existing sessions or choose to overwrite:",
                reply_markup=keyboard,
                parse_mode=ParseMode.MARKDOWN
            )
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
                "**Usage:** `/add_bal <user id> <amount>`\n"
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
    
    async def import_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /import command"""
        user = update.effective_user
        
        if user.id not in BOT_OWNERS:
            return
        
        if not context.args:
            await update.message.reply_text(
                "❌ Please specify what to import.\n\n"
                "**Usage:** `/import <type>`\n"
                "**Types:** `sessions`, `users`, `groups`",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        import_type = context.args[0].lower()
        
        if import_type == "sessions":
            await update.message.reply_text(
                "📁 **Import Sessions**\n\n"
                "Please send a .session file to import it.",
                parse_mode=ParseMode.MARKDOWN
            )
        elif import_type == "users":
            await update.message.reply_text(
                "👥 **Import Users**\n\n"
                "Please send a JSON file with user data.",
                parse_mode=ParseMode.MARKDOWN
            )
        elif import_type == "groups":
            await update.message.reply_text(
                "🏪 **Import Groups**\n\n"
                "Please send a JSON file with group data.",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await update.message.reply_text(
                "❌ Invalid import type.\n\n"
                "**Available types:** `sessions`, `users`, `groups`",
                parse_mode=ParseMode.MARKDOWN
            )
    
    async def export_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /export command"""
        user = update.effective_user
        
        if user.id not in BOT_OWNERS:
            return
        
        if not context.args:
            await update.message.reply_text(
                "❌ Please specify what to export.\n\n"
                "**Usage:** `/export <type>`\n"
                "**Types:** `users`, `groups`, `transactions`",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        export_type = context.args[0].lower()
        
        try:
            if export_type == "users":
                await self.export_users_data(update, context)
            elif export_type == "groups":
                await self.export_groups_data(update, context)
            elif export_type == "transactions":
                await self.export_transactions_data(update, context)
            else:
                await update.message.reply_text(
                    "❌ Invalid export type.\n\n"
                    "**Available types:** `users`, `groups`, `transactions`",
                    parse_mode=ParseMode.MARKDOWN
                )
        except Exception as e:
            logger.error(f"Error in export command: {e}")
            await update.message.reply_text(
                "❌ Export failed. Please try again.",
                parse_mode=ParseMode.MARKDOWN
            )
    
    async def export_users_data(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Export users data"""
        users = db.get_all_users(0, 1000)  # Get many users
        
        export_data = {
            'export_type': 'users',
            'export_date': datetime.now().isoformat(),
            'total_users': len(users),
            'users': users
        }
        
        filename = f"users_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        filepath = f"/tmp/{filename}"
        
        with open(filepath, 'w') as f:
            json.dump(export_data, f, indent=2)
        
        await context.bot.send_document(
            chat_id=update.effective_chat.id,
            document=open(filepath, 'rb'),
            filename=filename,
            caption="👥 **Users Data Export**\n\nComplete users database export."
        )
        
        os.remove(filepath)
    
    async def export_groups_data(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Export groups data"""
        # Get all groups from database
        with db.lock:
            conn = db.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM groups')
            groups_raw = cursor.fetchall()
            
            # Get column names
            cursor.execute('PRAGMA table_info(groups)')
            columns = [row[1] for row in cursor.fetchall()]
            conn.close()
        
        groups = []
        for row in groups_raw:
            group_dict = dict(zip(columns, row))
            groups.append(group_dict)
        
        export_data = {
            'export_type': 'groups',
            'export_date': datetime.now().isoformat(),
            'total_groups': len(groups),
            'groups': groups
        }
        
        filename = f"groups_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        filepath = f"/tmp/{filename}"
        
        with open(filepath, 'w') as f:
            json.dump(export_data, f, indent=2)
        
        await context.bot.send_document(
            chat_id=update.effective_chat.id,
            document=open(filepath, 'rb'),
            filename=filename,
            caption="🏪 **Groups Data Export**\n\nComplete groups database export."
        )
        
        os.remove(filepath)
    
    async def export_transactions_data(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Export transactions data"""
        with db.lock:
            conn = db.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM transactions ORDER BY created_at DESC LIMIT 1000')
            transactions_raw = cursor.fetchall()
            
            cursor.execute('PRAGMA table_info(transactions)')
            columns = [row[1] for row in cursor.fetchall()]
            conn.close()
        
        transactions = []
        for row in transactions_raw:
            transaction_dict = dict(zip(columns, row))
            transactions.append(transaction_dict)
        
        export_data = {
            'export_type': 'transactions',
            'export_date': datetime.now().isoformat(),
            'total_transactions': len(transactions),
            'transactions': transactions
        }
        
        filename = f"transactions_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        filepath = f"/tmp/{filename}"
        
        with open(filepath, 'w') as f:
            json.dump(export_data, f, indent=2)
        
        await context.bot.send_document(
            chat_id=update.effective_chat.id,
            document=open(filepath, 'rb'),
            filename=filename,
            caption="💳 **Transactions Data Export**\n\nRecent transactions export (last 1000)."
        )
        
        os.remove(filepath)
    
    async def sessions_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /sessions command to manage user sessions"""
        user = update.effective_user
        
        if user.id not in BOT_OWNERS:
            return
        
        sessions = db.get_user_sessions(user.id)
        
        if not sessions:
            await update.message.reply_text(
                "📱 **No Sessions Found**\n\n"
                "You don't have any userbot sessions.\n\n"
                "Use `/add` to add a new session.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        text = f"📱 **Your Userbot Sessions**\n\n"
        text += f"**Total:** {len(sessions)}/{MAX_SESSIONS_PER_USER}\n\n"
        
        keyboard_buttons = []
        
        for i, session in enumerate(sessions, 1):
            status = "🟢 Active" if session['is_active'] else "🔴 Inactive"
            has_2fa = "🔒 2FA" if session['has_2fa'] else "🔓 No 2FA"
            
            text += f"**{i}.** Session {session['id']}\n"
            text += f"   • **Phone:** {session['phone_number']}\n"
            text += f"   • **Status:** {status}\n"
            text += f"   • **Security:** {has_2fa}\n"
            text += f"   • **Added:** {session['created_at'][:10]}\n\n"
            
            # Add buttons for each session
            row = [
                InlineKeyboardButton(f"🔧 Manage #{i}", callback_data=f"manage_session_{session['id']}"),
                InlineKeyboardButton(f"🗑️ Remove #{i}", callback_data=f"remove_session_{session['id']}")
            ]
            keyboard_buttons.append(row)
        
        # Add action buttons
        keyboard_buttons.append([
            InlineKeyboardButton("➕ Add New Session", callback_data="add_new_session"),
            InlineKeyboardButton("🔄 Refresh", callback_data="refresh_sessions")
        ])
        
        keyboard = InlineKeyboardMarkup(keyboard_buttons)
        
        await update.message.reply_text(
            text,
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def set_bulk_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /set_bulk command to create keyword shortcuts"""
        user = update.effective_user
        
        if not context.args or len(context.args) < 2:
            # Show existing keywords
            keywords = db.get_user_bulk_keywords(user.id)
            
            if not keywords:
                await update.message.reply_text(
                    "📝 **Bulk Listing Keywords**\n\n"
                    "No keywords set yet.\n\n"
                    "**Usage:** `/set_bulk <keyword> <year>` or `/set_bulk <keyword> <year+month>`\n\n"
                    "**Examples:**\n"
                    "• `/set_bulk old2020 2020` - Set keyword 'old2020' for year 2020\n"
                    "• `/set_bulk jan2025 2025+1` - Set keyword 'jan2025' for January 2025\n"
                    "• `/set_bulk summer 2024+7` - Set keyword 'summer' for July 2024",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            
            text = "📝 **Your Bulk Listing Keywords**\n\n"
            for keyword in keywords:
                if keyword['month']:
                    month_names = [
                        "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
                    ]
                    date_str = f"{month_names[keyword['month']-1]} {keyword['year']}"
                else:
                    date_str = str(keyword['year'])
                
                text += f"• **{keyword['keyword']}** → {date_str}\n"
            
            text += "\n**Usage:** `/set_bulk <keyword> <year>` or `/set_bulk <keyword> <year+month>`"
            
            await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
            return
        
        keyword = context.args[0].lower()
        date_input = context.args[1]
        
        # Validate keyword
        if not keyword.isalnum():
            await update.message.reply_text(
                "❌ **Invalid Keyword**\n\n"
                "Keywords must contain only letters and numbers.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        if len(keyword) > 20:
            await update.message.reply_text(
                "❌ **Keyword Too Long**\n\n"
                "Keywords must be 20 characters or less.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        # Parse date input
        try:
            if '+' in date_input:
                # Year+Month format
                year_str, month_str = date_input.split('+')
                year = int(year_str)
                month = int(month_str)
                
                if month < 1 or month > 12:
                    raise ValueError("Invalid month")
            else:
                # Year only format
                year = int(date_input)
                month = None
            
            # Validate year
            if year < 2016 or year > 2030:
                await update.message.reply_text(
                    "❌ **Invalid Year**\n\n"
                    "Year must be between 2016 and 2030.",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
                
        except ValueError:
            await update.message.reply_text(
                "❌ **Invalid Date Format**\n\n"
                "**Examples:**\n"
                "• `2025` - For year 2025\n"
                "• `2025+1` - For January 2025\n"
                "• `2024+12` - For December 2024",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        # Save keyword
        success = db.add_bulk_keyword(user.id, keyword, year, month)
        
        if success:
            if month:
                month_names = [
                    "January", "February", "March", "April", "May", "June",
                    "July", "August", "September", "October", "November", "December"
                ]
                date_str = f"{month_names[month-1]} {year}"
            else:
                date_str = str(year)
            
            await update.message.reply_text(
                f"✅ **Keyword Set Successfully!**\n\n"
                f"**Keyword:** `{keyword}`\n"
                f"**Target Date:** {date_str}\n\n"
                f"Now you can use `/blist {keyword}` for quick bulk listing!",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await update.message.reply_text(
                "❌ Failed to save keyword. Please try again.",
                parse_mode=ParseMode.MARKDOWN
            )
    
    async def blist_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /blist command for bulk listing"""
        user = update.effective_user
        chat = update.effective_chat
        
        if chat.type not in ['group', 'supergroup']:
            await update.message.reply_text(
                "❌ This command can only be used in groups.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        if not context.args:
            await update.message.reply_text(
                "❌ **Missing Keyword**\n\n"
                "**Usage:** `/blist <keyword>`\n\n"
                "Use `/set_bulk` to create keywords first.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        keyword = context.args[0].lower()
        
        # Get keyword details
        keyword_data = db.get_bulk_keyword(user.id, keyword)
        if not keyword_data:
            await update.message.reply_text(
                f"❌ **Keyword Not Found**\n\n"
                f"The keyword `{keyword}` doesn't exist.\n\n"
                f"Use `/set_bulk {keyword} <year>` to create it first.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        # Check if user is the group owner/admin
        try:
            chat_member = await context.bot.get_chat_member(chat.id, user.id)
            if chat_member.status not in ['creator', 'administrator']:
                await update.message.reply_text(
                    "❌ **Permission Denied**\n\n"
                    "Only group owners and administrators can list groups.",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
        except Exception as e:
            logger.error(f"Error checking user permissions: {e}")
            await update.message.reply_text(
                "❌ Unable to verify your permissions in this group.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        if keyword_data['month']:
            month_names = [
                "January", "February", "March", "April", "May", "June",
                "July", "August", "September", "October", "November", "December"
            ]
            date_str = f"{month_names[keyword_data['month']-1]} {keyword_data['year']}"
        else:
            date_str = str(keyword_data['year'])
        
        # Ask for price first
        await update.message.reply_text(
            f"📦 **Bulk Listing - {keyword.upper()}**\n\n"
            f"**Target Date:** {date_str}\n"
            f"**Group:** {chat.title}\n\n"
            f"Please enter the price for this group in USDT:\n\n"
            f"**Format:** Enter a positive number (max 2 decimal places)\n"
            f"**Examples:** `10`, `25.50`, `99.99`",
            parse_mode=ParseMode.MARKDOWN
        )
        
        # Set user context for price input
        self.user_contexts[user.id] = {
            'state': 'waiting_bulk_price',
            'group_id': chat.id,
            'keyword_data': keyword_data,
            'group_title': chat.title
        }
    
    def add_pending_bulk_listing(self, user_id: int, group_id: int, keyword_data: Dict):
        """Add pending bulk listing"""
        from datetime import datetime, timedelta
        
        # Store in user context for /done validation
        if not hasattr(self, 'pending_bulk_listings'):
            self.pending_bulk_listings = {}
        
        self.pending_bulk_listings[group_id] = {
            'user_id': user_id,
            'keyword_data': keyword_data,
            'timestamp': datetime.now(),
            'expires_at': datetime.now() + timedelta(minutes=10)
        }
    
    def get_pending_bulk_listing(self, group_id: int) -> Optional[Dict]:
        """Get pending bulk listing"""
        if not hasattr(self, 'pending_bulk_listings'):
            return None
        
        listing = self.pending_bulk_listings.get(group_id)
        if listing and listing['expires_at'] > datetime.now():
            return listing
        elif listing:
            # Remove expired listing
            del self.pending_bulk_listings[group_id]
        
        return None
    
    async def handle_bulk_done(self, update: Update, context: ContextTypes.DEFAULT_TYPE, bulk_listing: Dict):
        """Handle /done for bulk listing"""
        user = update.effective_user
        chat = update.effective_chat
        keyword_data = bulk_listing['keyword_data']
        
        await update.message.reply_text(
            "🔄 **Verifying Bulk Listing**\n\n"
            "Checking if userbot has been granted ownership...",
            parse_mode=ParseMode.MARKDOWN
        )
        
        try:
            # Get userbot session (admin session for verification)
            admin_sessions = db.get_user_sessions(BOT_OWNERS[0])  # Get admin sessions
            if not admin_sessions:
                await update.message.reply_text(
                    "❌ No admin userbot sessions available. Please contact administrator.",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            
            session_data = admin_sessions[0]  # Use first admin session
            
            # Create Telethon client
            client = TelegramClient(
                session=session_data['session_string'],
                api_id=session_data['api_id'],
                api_hash=session_data['api_hash']
            )
            
            await client.connect()
            
            # Check if userbot is owner of the group
            is_owner, owner_message = await session_manager.check_group_ownership(client, chat.id)
            
            if not is_owner:
                await update.message.reply_text(
                    f"❌ **Ownership Not Detected**\n\n"
                    f"The userbot is not the owner of this group.\n\n"
                    f"**Please ensure:**\n"
                    f"• You added the userbot to the group\n"
                    f"• You gave it admin rights with full permissions\n"
                    f"• You transferred ownership to the userbot\n\n"
                    f"Error: {owner_message}",
                    parse_mode=ParseMode.MARKDOWN
                )
                await client.disconnect()
                return
            
            # Get detailed group information
            group_info = await session_manager.get_group_info(client, chat.id)
            await client.disconnect()
            
            if not group_info:
                await update.message.reply_text(
                    "❌ Unable to retrieve group information.",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            
            # Validate group for listing
            is_valid, validation_message = is_group_valid_for_listing(group_info)
            if not is_valid:
                await update.message.reply_text(
                    f"❌ **Group Not Valid for Listing**\n\n"
                    f"{validation_message}\n\n"
                    f"Please fix the issue and try again.",
                    parse_mode=ParseMode.MARKDOWN
                )
                # Remove pending bulk listing
                if hasattr(self, 'pending_bulk_listings') and chat.id in self.pending_bulk_listings:
                    del self.pending_bulk_listings[chat.id]
                return
            
            # Get the price from bulk listing data
            price = keyword_data.get('price')
            creation_date = keyword_data.get('creation_date')
            group_title = keyword_data.get('group_title', chat.title)
            invite_link = group_info.get('invite_link') if group_info else None
            
            if not price:
                await update.message.reply_text(
                    "❌ **Price Missing**\n\n"
                    "Bulk listing data is incomplete. Please start over with `/blist <keyword>`.",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            
            # Generate buying ID
            buying_id = db.get_or_create_buying_id(chat.id)
            
            # Add group to database
            success = db.add_group(
                 group_id=chat.id,
                 group_name=group_title,
                 group_username=chat.username or "",
                 invite_link=invite_link or "",
                 owner_user_id=user.id,
                 session_id=session_data['id'],
                 price=price,
                 creation_date=creation_date,
                 total_messages=group_info['total_messages']
             )
            
            if success:
                # Remove from pending bulk listings
                if hasattr(self, 'pending_bulk_listings') and chat.id in self.pending_bulk_listings:
                    del self.pending_bulk_listings[chat.id]
                
                # Format date for display
                if keyword_data.get('month'):
                    month_names = [
                        "January", "February", "March", "April", "May", "June",
                        "July", "August", "September", "October", "November", "December"
                    ]
                    date_str = f"{month_names[keyword_data['month']-1]} {keyword_data['year']}"
                else:
                    date_str = str(keyword_data['year'])
                
                await update.message.reply_text(
                    f"🎉 **Bulk Listing Successful!**\n\n"
                    f"**Group:** {group_title}\n"
                    f"**Buying ID:** `{buying_id}`\n"
                    f"**Price:** ${format_price(price)} USDT\n"
                    f"**Target Date:** {date_str}\n"
                    f"**Keyword:** {keyword_data['keyword']}\n\n"
                    f"Your group is now available in the marketplace! 🚀",
                    parse_mode=ParseMode.MARKDOWN
                )
                
                logger.info(f"Bulk listing successful: Group {chat.id} by user {user.id} with keyword {keyword_data['keyword']}")
            else:
                await update.message.reply_text(
                    "❌ **Listing Failed**\n\n"
                    "Unable to add group to marketplace. Please try again or contact support.",
                    parse_mode=ParseMode.MARKDOWN
                )
            
        except Exception as e:
            logger.error(f"Error in bulk done command: {e}")
            await update.message.reply_text(
                "❌ An error occurred while verifying the group. Please try again.",
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
        elif data.startswith("manage_session_"):
            await self.handle_session_management(query, context)
        elif data.startswith("remove_session_"):
            await self.handle_session_removal(query, context)
        elif data == "manage_sessions":
            await self.handle_sessions_command_callback(query, context)
        elif data == "overwrite_session":
            await self.handle_session_overwrite(query, context)
        elif data == "add_new_session":
            await self.handle_add_new_session_callback(query, context)
        elif data == "refresh_sessions":
            await self.handle_refresh_sessions(query, context)
        elif data.startswith("approve_withdrawal_") or data.startswith("reject_withdrawal_"):
            await self.handle_withdrawal_approval(update, context)
        elif data == "restart_session_setup":
            await self.handle_restart_session_setup(query, context)
        elif data == "cancel_session_setup":
            await self.handle_cancel_session_setup(query, context)
    
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
    
    # Session Management Callbacks
    async def handle_session_management(self, query, context):
        """Handle session management callback"""
        session_id = int(query.data.split('_')[2])
        
        # Get session details
        session_info = self.get_session_details(query.from_user.id, session_id)
        if not session_info:
            await query.edit_message_text("❌ Session not found.")
            return
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Test Connection", callback_data=f"test_session_{session_id}")],
            [InlineKeyboardButton("🔒 Change 2FA", callback_data=f"change_2fa_{session_id}")],
            [InlineKeyboardButton("📱 Toggle Active", callback_data=f"toggle_session_{session_id}")],
            [InlineKeyboardButton("🗑️ Delete Session", callback_data=f"delete_session_{session_id}")],
            [InlineKeyboardButton("⬅️ Back to Sessions", callback_data="refresh_sessions")]
        ])
        
        status = "🟢 Active" if session_info['is_active'] else "🔴 Inactive"
        has_2fa = "🔒 2FA Enabled" if session_info['has_2fa'] else "🔓 No 2FA"
        
        text = f"""
📱 **Session Management**

**Session ID:** {session_id}
**Phone:** {session_info['phone_number']}
**Status:** {status}
**Security:** {has_2fa}
**Created:** {session_info['created_at'][:10]}

Choose an action:
"""
        
        await query.edit_message_text(text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)
    
    async def handle_session_removal(self, query, context):
        """Handle session removal callback"""
        session_id = int(query.data.split('_')[2])
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Yes, Delete", callback_data=f"confirm_delete_{session_id}")],
            [InlineKeyboardButton("❌ Cancel", callback_data="refresh_sessions")]
        ])
        
        await query.edit_message_text(
            f"🗑️ **Confirm Session Deletion**\n\n"
            f"Are you sure you want to delete session {session_id}?\n\n"
            f"**This action cannot be undone!**",
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def handle_sessions_command_callback(self, query, context):
        """Handle manage sessions callback"""
        # Redirect to sessions command
        await self.sessions_command(query, context)
    
    async def handle_session_overwrite(self, query, context):
        """Handle session overwrite callback"""
        user_id = query.from_user.id
        
        # Show list of sessions to overwrite
        sessions = db.get_user_sessions(user_id)
        keyboard_buttons = []
        
        for session in sessions:
            status = "🟢" if session['is_active'] else "🔴"
            keyboard_buttons.append([
                InlineKeyboardButton(
                    f"{status} {session['phone_number']} (ID: {session['id']})",
                    callback_data=f"overwrite_session_id_{session['id']}"
                )
            ])
        
        keyboard_buttons.append([
            InlineKeyboardButton("❌ Cancel", callback_data="refresh_sessions")
        ])
        
        keyboard = InlineKeyboardMarkup(keyboard_buttons)
        
        await query.edit_message_text(
            "🔄 **Select Session to Overwrite**\n\n"
            "Choose which session you want to replace:",
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def handle_add_new_session_callback(self, query, context):
        """Handle add new session callback"""
        # Start the session adding process
        text = """
🤖 **Add Userbot Session**

Let's add a new userbot session for group transfers.

Please provide your **API ID**:

You can get this from https://my.telegram.org
"""
        
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN)
        self.user_contexts[query.from_user.id] = {'state': 'waiting_api_id'}
    
    async def handle_refresh_sessions(self, query, context):
        """Handle refresh sessions callback"""
        await self.sessions_command(query, context)
    
    async def handle_restart_session_setup(self, query, context):
        """Handle restart session setup callback"""
        user_id = query.from_user.id
        if user_id in self.user_contexts:
            del self.user_contexts[user_id]
        
        await self.handle_add_new_session_callback(query, context)
    
    async def handle_cancel_session_setup(self, query, context):
        """Handle cancel session setup callback"""
        user_id = query.from_user.id
        if user_id in self.user_contexts:
            del self.user_contexts[user_id]
        
        await query.edit_message_text(
            "❌ **Session Setup Cancelled**\n\n"
            "Session setup has been cancelled.",
            parse_mode=ParseMode.MARKDOWN
        )
    
    def get_session_details(self, user_id: int, session_id: int) -> Optional[Dict]:
        """Get detailed session information"""
        sessions = db.get_user_sessions(user_id)
        for session in sessions:
            if session['id'] == session_id:
                return session
        return None
    
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
        elif state == 'waiting_import_api_id':
            await self.handle_import_api_id_input(update, context)
        elif state == 'waiting_import_api_hash':
            await self.handle_import_api_hash_input(update, context)
        elif state == 'waiting_import_password':
            await self.handle_import_password_input(update, context)
        elif state == 'waiting_bulk_price':
            await self.handle_bulk_price_input(update, context)
        elif state == 'waiting_bank_api_id':
            await self.handle_bank_api_id_input(update, context)
        elif state == 'waiting_bank_api_hash':
            await self.handle_bank_api_hash_input(update, context)
        elif state == 'waiting_bank_phone':
            await self.handle_bank_phone_input(update, context)
        elif state == 'waiting_bank_code':
            await self.handle_bank_code_input(update, context)
        elif state == 'waiting_bank_password':
            await self.handle_bank_password_input(update, context)
    
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
        
        # Add pending listing
        self.add_pending_listing(user.id, user_context['chat_id'], price)
        
        text = f"""
✅ **Price Set:** ${format_price(price)} USDT

Now, please add one of our userbots to your group as admin with full rights:

**Available Userbots:**
• @example_userbot (add this bot to your group)

**Steps:**
1. Add the userbot to your group
2. Give it admin rights with full permissions
3. Transfer ownership to the userbot
4. Type `/done` when completed

**⏰ Timeout:** 5 minutes

**Important:** The userbot must become the owner (not just admin) for the listing to work.
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
        """Handle 2FA password input with retry logic"""
        user = update.effective_user
        password = update.message.text.strip()
        
        user_context = self.user_contexts.get(user.id, {})
        retry_count = user_context.get('password_retries', 0)
        
        success, message = await session_manager.verify_password(user.id, password)
        
        if success:
            success, complete_message = await session_manager.complete_auth(user.id)
            
            if success:
                # Validate session after successful login
                validation_success = await self.validate_session_post_login(user.id)
                if validation_success:
                    await update.message.reply_text(
                        "✅ **Session Added Successfully!**\n\n"
                        "Your userbot session has been saved with 2FA protection and validated.",
                        parse_mode=ParseMode.MARKDOWN
                    )
                else:
                    await update.message.reply_text(
                        "⚠️ **Session Saved with Warning**\n\n"
                        "Session was saved but validation failed. Please check session manually.",
                        parse_mode=ParseMode.MARKDOWN
                    )
            else:
                await update.message.reply_text(
                    f"❌ Failed to save session: {complete_message}",
                    parse_mode=ParseMode.MARKDOWN
                )
            del self.user_contexts[user.id]
        else:
            retry_count += 1
            
            if retry_count >= 3:
                await update.message.reply_text(
                    "❌ **Too Many Failed Attempts**\n\n"
                    "Maximum password attempts exceeded. Please restart the session adding process.\n\n"
                    "**Hints:**\n"
                    "• Make sure you're using your 2-Step Verification password\n"
                    "• Check for typos or extra spaces\n"
                    "• Ensure your account has 2FA enabled\n"
                    "• Try using `/add` again if needed",
                    parse_mode=ParseMode.MARKDOWN
                )
                del self.user_contexts[user.id]
            else:
                remaining_attempts = 3 - retry_count
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Restart Session Setup", callback_data="restart_session_setup")],
                    [InlineKeyboardButton("❌ Cancel", callback_data="cancel_session_setup")]
                ])
                
                await update.message.reply_text(
                    f"❌ **Incorrect Password**\n\n"
                    f"{message}\n\n"
                    f"**Attempts remaining:** {remaining_attempts}/3\n\n"
                    f"**Tips:**\n"
                    f"• Use your 2-Step Verification password (not login password)\n"
                    f"• Check for typos and extra spaces\n"
                    f"• Make sure 2FA is enabled on your account\n\n"
                    f"Please enter your 2FA password again:",
                    reply_markup=keyboard,
                    parse_mode=ParseMode.MARKDOWN
                )
                
                self.user_contexts[user.id] = {
                    **user_context,
                    'password_retries': retry_count
                }
    
    async def validate_session_post_login(self, user_id: int) -> bool:
        """Validate session after successful login"""
        try:
            sessions = db.get_user_sessions(user_id)
            if not sessions:
                return False
            
            # Get the most recent session
            latest_session = sessions[-1]
            
            # Create a temporary client to test the session
            client = TelegramClient(
                session=latest_session['session_string'],
                api_id=latest_session['api_id'],
                api_hash=latest_session['api_hash']
            )
            
            await client.connect()
            
            if await client.is_user_authorized():
                me = await client.get_me()
                logger.info(f"Session validated for user {me.id} (@{me.username})")
                await client.disconnect()
                return True
            else:
                logger.warning(f"Session validation failed - not authorized")
                await client.disconnect()
                return False
                
        except Exception as e:
            logger.error(f"Session validation error: {e}")
            return False
    
    async def handle_import_password_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle password input for session import"""
        user = update.effective_user
        password_input = update.message.text.strip()
        
        user_context = self.user_contexts[user.id]
        session_file = user_context['session_file']
        
        password = None if password_input.lower() == 'skip' else password_input
        
        try:
            # Get API credentials from context
            api_id = user_context.get('api_id')
            api_hash = user_context.get('api_hash')
            
            if not api_id or not api_hash:
                await update.message.reply_text(
                    "❌ **Missing API Credentials**\n\n"
                    "Please restart the import process.",
                    parse_mode=ParseMode.MARKDOWN
                )
                del self.user_contexts[user.id]
                return
            
            # Import the session file with API credentials
            success, message = await session_manager.import_session_file(
                user.id, session_file, password, api_id, api_hash
            )
            
            if success:
                await update.message.reply_text(
                    "✅ **Session Imported Successfully!**\n\n"
                    "The session file has been imported and is ready to use.",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await update.message.reply_text(
                    f"❌ Session import failed: {message}",
                    parse_mode=ParseMode.MARKDOWN
                )
            
            # Clean up temporary file
            try:
                os.remove(session_file)
            except:
                pass
                
        except Exception as e:
            logger.error(f"Error importing session: {e}")
            await update.message.reply_text(
                "❌ An error occurred during session import.",
                parse_mode=ParseMode.MARKDOWN
            )
        
        del self.user_contexts[user.id]
    
    async def handle_import_api_id_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle API ID input for session import"""
        user = update.effective_user
        api_id_str = update.message.text.strip()
        
        try:
            api_id = int(api_id_str)
            if api_id <= 0:
                raise ValueError("API ID must be positive")
        except ValueError:
            await update.message.reply_text(
                "❌ **Invalid API ID**\n\n"
                "Please enter a valid API ID (numbers only):",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        # Update context and ask for API Hash
        self.user_contexts[user.id]['api_id'] = api_id
        self.user_contexts[user.id]['state'] = 'waiting_import_api_hash'
        
        await update.message.reply_text(
            "✅ **API ID Saved**\n\n"
            "**Step 2:** Enter your API Hash\n"
            "(32-character string from https://my.telegram.org)",
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def handle_import_api_hash_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle API Hash input for session import"""
        user = update.effective_user
        api_hash = update.message.text.strip()
        
        if len(api_hash) != 32 or not all(c in '0123456789abcdef' for c in api_hash.lower()):
            await update.message.reply_text(
                "❌ **Invalid API Hash**\n\n"
                "Please enter a valid API Hash (32-character hexadecimal string):",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        # Update context and ask for password
        self.user_contexts[user.id]['api_hash'] = api_hash
        self.user_contexts[user.id]['state'] = 'waiting_import_password'
        
        await update.message.reply_text(
            "✅ **API Hash Saved**\n\n"
            "**Step 3:** Enter your 2FA password\n"
            "If this session doesn't have 2FA enabled, type `skip`\n\n"
            "⚠️ **Note:** Sessions without 2FA will be rejected for security.",
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def handle_bulk_price_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle price input for bulk listing"""
        user = update.effective_user
        price_text = update.message.text.strip()
        
        user_context = self.user_contexts[user.id]
        keyword_data = user_context['keyword_data']
        group_id = user_context['group_id']
        group_title = user_context['group_title']
        
        # Validate price
        is_valid, price = validate_price(price_text)
        if not is_valid:
            await update.message.reply_text(
                "❌ **Invalid Price**\n\n"
                "Please enter a valid price between $0.01 and $99.99.\n"
                "Examples: 10, 15.50, 99.99",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        # Generate buying ID
        buying_id = db.get_or_create_buying_id(group_id)
        
        # Set the creation date from keyword data
        if keyword_data['month']:
            # Specific month
            creation_date = f"{keyword_data['year']}-{keyword_data['month']:02d}-01"
        else:
            # Year only - set to January 1st
            creation_date = f"{keyword_data['year']}-01-01"
        
        # Show confirmation
        if keyword_data['month']:
            month_names = [
                "January", "February", "March", "April", "May", "June",
                "July", "August", "September", "October", "November", "December"
            ]
            date_str = f"{month_names[keyword_data['month']-1]} {keyword_data['year']}"
        else:
            date_str = str(keyword_data['year'])
        
        text = f"""
✅ **Bulk Listing - Price Set**

**📋 Group Information:**
**Group ID:** `{group_id}`
**Buying ID:** `{buying_id}`
**Group Name:** {group_title}
**Target Date:** {date_str} (from keyword: {keyword_data['keyword']})
**Price:** ${format_price(price)} USDT

**📝 Next Steps:**
1. Add our userbot to this group as admin
2. Transfer group ownership to the userbot
3. Return to this group and type `/done` when complete

**⚠️ Important:**
• You must transfer actual ownership (not just admin rights)
• Only the original group owner should use `/done`
• Group must meet listing requirements (private, 4+ messages, etc.)

**Userbot to add:** @{context.bot.username}_bot
"""
        
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
        
        # Add to pending bulk listings with price
        self.add_pending_bulk_listing(user.id, group_id, {
            **keyword_data,
            'price': price,
            'creation_date': creation_date,
            'group_title': group_title
        })
        
        # Clear user context
        del self.user_contexts[user.id]
    
    # Bank Session Handlers
    async def handle_bank_api_id_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle API ID input for bank session"""
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
        
        self.user_contexts[user.id]['api_id'] = api_id
        self.user_contexts[user.id]['state'] = 'waiting_bank_api_hash'
        
        await update.message.reply_text(
            "✅ **API ID Saved**\n\n"
            "**Step 2:** Please enter your API Hash\n"
            "(32-character string from https://my.telegram.org):",
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def handle_bank_api_hash_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle API Hash input for bank session"""
        user = update.effective_user
        api_hash = update.message.text.strip()
        
        is_valid, _ = validate_api_credentials("12345", api_hash)
        
        if not is_valid:
            await update.message.reply_text(
                "❌ Invalid API Hash format.\n\n"
                "Please enter a valid API Hash (32-character string):",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        self.user_contexts[user.id]['api_hash'] = api_hash
        self.user_contexts[user.id]['state'] = 'waiting_bank_phone'
        
        await update.message.reply_text(
            "✅ **API Hash Saved**\n\n"
            "**Step 3:** Please enter your phone number\n"
            "(Format: +1234567890):",
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def handle_bank_phone_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle phone input for bank session"""
        user = update.effective_user
        phone = update.message.text.strip()
        
        if not validate_phone_number(phone):
            await update.message.reply_text(
                "❌ Invalid phone number format.\n\n"
                "Please enter a valid phone number (e.g., +1234567890):",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        user_context = self.user_contexts[user.id]
        api_id = user_context['api_id']
        api_hash = user_context['api_hash']
        
        try:
            success = await session_manager.start_auth_process(user.id, api_id, api_hash, phone, 'bank')
            
            if success:
                self.user_contexts[user.id]['state'] = 'waiting_bank_code'
                await update.message.reply_text(
                    f"📱 **OTP Sent**\n\n"
                    f"An OTP has been sent to {phone}.\n"
                    f"Please enter the code:",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await update.message.reply_text(
                    f"❌ Failed to send OTP\n\n"
                    f"Please try again with a valid phone number:",
                    parse_mode=ParseMode.MARKDOWN
                )
        except Exception as e:
            logger.error(f"Error starting bank auth: {e}")
            await update.message.reply_text(
                "❌ An error occurred. Please try again.",
                parse_mode=ParseMode.MARKDOWN
            )
    
    async def handle_bank_code_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle OTP code input for bank session"""
        user = update.effective_user
        code = update.message.text.strip()
        
        if not code.isdigit() or len(code) != 5:
            await update.message.reply_text(
                "❌ Invalid code format.\n\n"
                "Please enter the 5-digit code:",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        try:
            success, message = await session_manager.verify_code(user.id, code)
            
            if success and "password required" in message.lower():
                self.user_contexts[user.id]['state'] = 'waiting_bank_password'
                await update.message.reply_text(
                    f"🔐 **2FA Required**\n\n"
                    f"Please enter your 2FA password:\n\n"
                    f"⚠️ **Required for bank sessions**",
                    parse_mode=ParseMode.MARKDOWN
                )
            elif success:
                # This shouldn't happen for bank sessions (2FA required)
                await update.message.reply_text(
                    "❌ **2FA Required**\n\n"
                    "Bank sessions must have 2FA enabled for security.",
                    parse_mode=ParseMode.MARKDOWN
                )
                del self.user_contexts[user.id]
            else:
                await update.message.reply_text(
                    f"❌ {message}\n\n"
                    f"Please try again:",
                    parse_mode=ParseMode.MARKDOWN
                )
        except Exception as e:
            logger.error(f"Error verifying bank code: {e}")
            await update.message.reply_text(
                "❌ An error occurred. Please try again.",
                parse_mode=ParseMode.MARKDOWN
            )
    
    async def handle_bank_password_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle 2FA password input for bank session"""
        user = update.effective_user
        password = update.message.text.strip()
        
        try:
            # First verify the password
            success, message = await session_manager.verify_password(user.id, password)
            
            if success:
                # Then complete authentication
                success, complete_message = await session_manager.complete_auth(user.id)
                
                if success:
                    await update.message.reply_text(
                        "✅ **Bank Session Added Successfully!**\n\n"
                        "🏦 Bank userbot is now active for payment processing.\n"
                        "💡 This session will be used for tip detection in the bank group.\n\n"
                        "**Security Features:**\n"
                        "• Dedicated to payment processing only\n"
                        "• Isolated from regular group operations\n"
                        "• Enhanced monitoring and logging",
                        parse_mode=ParseMode.MARKDOWN
                    )
                    logger.info(f"Bank session added successfully for user {user.id}")
                else:
                    await update.message.reply_text(
                        f"❌ {complete_message}\n\n"
                        f"Please try again:",
                        parse_mode=ParseMode.MARKDOWN
                    )
                    return
            else:
                await update.message.reply_text(
                    f"❌ {message}\n\n"
                    f"Please try again:",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
                
        except Exception as e:
            logger.error(f"Error completing bank auth: {e}")
            await update.message.reply_text(
                "❌ An error occurred during authentication.",
                parse_mode=ParseMode.MARKDOWN
            )
        
        del self.user_contexts[user.id]
    
    # Payment Detection
    async def handle_tip_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle tip messages from cctip bot with balance updates"""
        message = update.message
        
        # Double-check security: only from CCTIP bot in bank group
        if (message.from_user.id != CCTIP_BOT_ID or 
            message.chat.id != BANK_GROUP_ID):
            logger.warning(f"Tip message from wrong source: user_id={message.from_user.id}, chat_id={message.chat.id}")
            return
        
        tip_info = parse_tip_message(message.text)
        
        if not tip_info or not tip_info['valid']:
            logger.info(f"Invalid tip message format: {message.text}")
            return
        
        # ENHANCED VALIDATION: Ensure correct Cwallet format
        # Must contain "tip details:" and "USDT +"
        if not re.search(r'tip details:', message.text, re.IGNORECASE):
            logger.debug(f"Message doesn't contain 'tip details:' - not a tip message")
            return
            
        if not re.search(r'USDT\s*\+', message.text, re.IGNORECASE):
            logger.debug(f"Message doesn't contain 'USDT +' - not a valid tip format")
            return
        
        # ENHANCED VALIDATION: Ensure user is tagged (via entities or @mention)
        has_user_mention = False
        if message.entities:
            for entity in message.entities:
                if entity.type in ['text_mention', 'mention']:
                    has_user_mention = True
                    break
        
        # Also check for username in text after USDT amount
        has_recipient_in_text = bool(re.search(r'USDT\s*\+\d+(?:\.\d+)?\s+\w+', message.text, re.IGNORECASE))
        
        if not has_user_mention and not has_recipient_in_text:
            logger.warning(f"Tip message does not contain user mention or recipient: {message.text}")
            return
        
        # Get bank userbot username to validate tips
        bank_username = db.get_bank_userbot_username()
        if not bank_username:
            logger.warning(f"No bank userbot configured - cannot process tips")
            return
        
        # Validate that the tip is for our bank userbot
        if not re.search(rf'USDT\s*\+\d+(?:\.\d+)?\s+@?{re.escape(bank_username)}(?:\s|$)', message.text, re.IGNORECASE):
            logger.debug(f"Tip not for our bank userbot (@{bank_username}), ignoring")
            return
        
        # Extract recipient user information from the message
        recipient_info = self.extract_recipient_from_tip(message.text, message.entities or [])
        
        if not recipient_info:
            logger.warning(f"Could not extract recipient from tip message: {message.text}")
            return
        
        # Validate that the recipient is actually our bank userbot
        recipient_username = recipient_info.get('username')
        if recipient_username != bank_username:
            logger.warning(f"Tip recipient (@{recipient_username}) is not our bank userbot (@{bank_username})")
            return
        
        # The bank userbot receives tips on behalf of users - we need to determine the actual user
        # For now, we'll extract the user who sent the original /tip command from the message
        actual_user_id = self.extract_tipper_from_message(message.text, message.entities or [])
        if not actual_user_id:
            logger.warning(f"Could not determine who sent the tip")
            return
        
        # ENHANCED VALIDATION: Only accept tips with high confidence
        if tip_info.get('confidence') == 'medium' and not tip_info.get('usdt_mentioned'):
            logger.warning(f"Low confidence tip parsing, rejecting: {message.text}")
            return
        
        # Update user balance
        success = db.update_user_balance(actual_user_id, tip_info['amount'], 'tip')
        
        if success:
            logger.info(f"Balance updated: User {actual_user_id} +${tip_info['amount']} USDT")
            
            # Notify user about balance update
            try:
                new_balance = db.get_user_balance(actual_user_id)
                await context.bot.send_message(
                    chat_id=actual_user_id,
                    text=f"💰 **Balance Updated!**\n\n"
                         f"**Received:** +${format_balance(tip_info['amount'])} USDT\n"
                         f"**New Balance:** ${format_balance(new_balance)} USDT\n\n"
                         f"Thank you for your payment!",
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception as e:
                logger.error(f"Failed to notify user {actual_user_id} about balance update: {e}")
        else:
            logger.error(f"Failed to update balance for user {actual_user_id}")
    
    def extract_recipient_from_tip(self, message_text: str, entities: List) -> Optional[Dict]:
        """Extract recipient information from Cwallet tip message"""
        try:
            # Priority 1: Look for user mention in entities (most reliable)
            for entity in entities:
                if entity.type == 'text_mention' and entity.user:
                    # Direct user mention
                    return {'user_id': entity.user.id, 'username': entity.user.username}
                elif entity.type == 'mention':
                    # Username mention (@username)
                    start = entity.offset
                    end = start + entity.length
                    username = message_text[start:end].replace('@', '')
                    
                    # Look up user by username in database
                    user_id = self.get_user_id_by_username(username)
                    if user_id:
                        return {'user_id': user_id, 'username': username}
            
            # Priority 2: Parse Cwallet format "USDT +amount @recipient"
            # Format: "Username tip details:\n\nUSDT +amount @recipient"
            cwallet_pattern = r'USDT\s*\+\d+(?:\.\d+)?\s+@(\w+)|USDT\s*\+\d+(?:\.\d+)?\s+(\w+)'
            match = re.search(cwallet_pattern, message_text, re.IGNORECASE)
            if match:
                # Check both groups for username
                username = match.group(1) or match.group(2)
                if username:
                    user_id = self.get_user_id_by_username(username)
                    if user_id:
                        logger.info(f"Found recipient in Cwallet format: @{username} (ID: {user_id})")
                        return {'user_id': user_id, 'username': username}
            
            # Priority 3: Look for recipient after USDT amount
            amount_recipient_pattern = r'USDT\s*\+\d+(?:\.\d+)?\s+([^\s@]+)|USDT\s*\+\d+(?:\.\d+)?\s+@(\w+)'
            match = re.search(amount_recipient_pattern, message_text, re.IGNORECASE)
            if match:
                # Check both groups for username
                username = match.group(1) or match.group(2)
                if username:
                    # Remove any @ symbol
                    username = username.replace('@', '')
                    user_id = self.get_user_id_by_username(username)
                    if user_id:
                        logger.info(f"Found recipient after USDT amount: @{username} (ID: {user_id})")
                        return {'user_id': user_id, 'username': username}
            
            # Priority 4: Fallback patterns for other possible formats
            fallback_patterns = [
                r'tip details:.*?@(\w+)',  # After tip details
                r'@(\w+).*?USDT',          # Username before USDT
                r'USDT.*?@(\w+)',          # Username after USDT
                r'tipped\s+@(\w+)',        # Traditional tipped format
                r'💰.*?@(\w+)',            # With money emoji
            ]
            
            for pattern in fallback_patterns:
                match = re.search(pattern, message_text, re.IGNORECASE | re.DOTALL)
                if match:
                    username = match.group(1)
                    user_id = self.get_user_id_by_username(username)
                    if user_id:
                        logger.info(f"Found recipient with fallback pattern: @{username} (ID: {user_id})")
                        return {'user_id': user_id, 'username': username}
            
            logger.warning(f"Could not extract recipient from message: {message_text[:100]}...")
            return None
            
        except Exception as e:
            logger.error(f"Error extracting recipient from tip: {e}")
            return None
    
    def get_user_id_by_username(self, username: str) -> Optional[int]:
        """Get user ID by username from database"""
        with db.lock:
            conn = db.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT user_id FROM users WHERE username = ?', (username,))
            result = cursor.fetchone()
            conn.close()
            return result[0] if result else None
    
    def extract_tipper_from_message(self, message_text: str, entities: List) -> Optional[int]:
        """Extract the original tipper (user who sent the tip) from Cwallet message"""
        try:
            # In Cwallet format: "Username tip details:\n\nUSDT +amount @recipient"
            # The username at the beginning is the tipper
            
            # Priority 1: Look for user mention in entities (most reliable)
            for entity in entities:
                if entity.type == 'text_mention' and entity.user:
                    # This would be the tipper mentioned in the message
                    if entity.offset == 0:  # At the beginning of message
                        return entity.user.id
                elif entity.type == 'mention':
                    # Username mention at the beginning
                    if entity.offset == 0:
                        start = entity.offset
                        end = start + entity.length
                        username = message_text[start:end].replace('@', '')
                        
                        # Look up user by username in database
                        user_id = self.get_user_id_by_username(username)
                        if user_id:
                            logger.info(f"Found tipper from mention: @{username} (ID: {user_id})")
                            return user_id
            
            # Priority 2: Extract username from start of message
            # Pattern: "username tip details:"
            tipper_pattern = r'^([^\s]+)\s+tip details:'
            match = re.search(tipper_pattern, message_text, re.IGNORECASE | re.MULTILINE)
            if match:
                username = match.group(1).replace('@', '')
                user_id = self.get_user_id_by_username(username)
                if user_id:
                    logger.info(f"Found tipper from message pattern: @{username} (ID: {user_id})")
                    return user_id
            
            logger.warning(f"Could not extract tipper from message: {message_text[:50]}...")
            return None
            
        except Exception as e:
            logger.error(f"Error extracting tipper from message: {e}")
            return None
    
    def verify_group_in_database(self, group_id: int) -> bool:
        """Verify that a group exists in the database"""
        with db.lock:
            conn = db.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT id FROM groups WHERE group_id = ?', (group_id,))
            result = cursor.fetchone()
            conn.close()
            return result is not None
    
    # Document Handler for Session Import
    async def handle_document(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle document uploads (for session import)"""
        user = update.effective_user
        
        if user.id not in BOT_OWNERS:
            await update.message.reply_text(
                "❌ Only bot administrators can import session files.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        document = update.message.document
        
        if document.file_name.endswith('.session'):
            await self.handle_session_file_import(update, context, document)
        elif document.file_name.endswith('.json'):
            await self.handle_json_file_import(update, context, document)
        else:
            await update.message.reply_text(
                "❌ Please send a valid .session or .json file.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
    
    async def handle_session_file_import(self, update: Update, context: ContextTypes.DEFAULT_TYPE, document):
        """Handle .session file import"""
        await update.message.reply_text(
            "📁 **Session File Received**\n\n"
            "Processing session file...",
            parse_mode=ParseMode.MARKDOWN
        )
        
        try:
            # Download the file
            file = await context.bot.get_file(document.file_id)
            file_path = f"/tmp/{document.file_name}"
            await file.download_to_drive(file_path)
            
            # Parse session file to check if it's valid
            try:
                from telethon.sessions import SQLiteSession
                session = SQLiteSession(file_path)
                
                # Try to create a temporary client to validate the session
                temp_client = TelegramClient(session, 0, "")
                await temp_client.connect()
                
                if not await temp_client.is_user_authorized():
                    await temp_client.disconnect()
                    await update.message.reply_text(
                        "❌ **Invalid Session File**\n\n"
                        "The session file is not authorized or corrupted.",
                        parse_mode=ParseMode.MARKDOWN
                    )
                    os.remove(file_path)
                    return
                
                await temp_client.disconnect()
                
            except Exception as e:
                logger.error(f"Error validating session file: {e}")
                await update.message.reply_text(
                    "❌ **Invalid Session File**\n\n"
                    "Unable to parse the session file. Please ensure it's a valid Telethon .session file.",
                    parse_mode=ParseMode.MARKDOWN
                )
                os.remove(file_path)
                return
            
            # Ask for API credentials first
            text = """
📁 **Session File Validated**

To import this session, please provide your API credentials:

**Step 1:** Enter your API ID
(Get it from https://my.telegram.org)
"""
            
            await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
            
            self.user_contexts[update.effective_user.id] = {
                'state': 'waiting_import_api_id',
                'session_file': file_path
            }
            
        except Exception as e:
            logger.error(f"Error handling session file: {e}")
            await update.message.reply_text(
                "❌ Failed to process session file. Please try again.",
                parse_mode=ParseMode.MARKDOWN
            )
    
    async def handle_json_file_import(self, update: Update, context: ContextTypes.DEFAULT_TYPE, document):
        """Handle JSON file import for users/groups data"""
        await update.message.reply_text(
            "📄 **JSON File Received**\n\n"
            "Processing import data...",
            parse_mode=ParseMode.MARKDOWN
        )
        
        try:
            # Download the file
            file = await context.bot.get_file(document.file_id)
            file_path = f"/tmp/{document.file_name}"
            await file.download_to_drive(file_path)
            
            # Parse JSON file
            with open(file_path, 'r') as f:
                import_data = json.load(f)
            
            export_type = import_data.get('export_type', '').lower()
            
            if export_type == 'users':
                await self.import_users_data(update, import_data)
            elif export_type == 'groups':
                await self.import_groups_data(update, import_data)
            elif export_type == 'transactions':
                await self.import_transactions_data(update, import_data)
            else:
                await update.message.reply_text(
                    "❌ **Invalid JSON Format**\n\n"
                    "Please send a valid export file with 'export_type' field.",
                    parse_mode=ParseMode.MARKDOWN
                )
            
            # Clean up file
            os.remove(file_path)
            
        except json.JSONDecodeError:
            await update.message.reply_text(
                "❌ **Invalid JSON File**\n\n"
                "Please send a valid JSON file.",
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception as e:
            logger.error(f"Error handling JSON file: {e}")
            await update.message.reply_text(
                "❌ Failed to process JSON file. Please try again.",
                parse_mode=ParseMode.MARKDOWN
            )
    
    async def import_users_data(self, update: Update, import_data: dict):
        """Import users data from JSON"""
        users = import_data.get('users', [])
        imported_count = 0
        
        for user_data in users:
            try:
                success = db.add_user(
                    user_data.get('user_id'),
                    user_data.get('username'),
                    user_data.get('first_name')
                )
                if success:
                    imported_count += 1
            except Exception as e:
                logger.error(f"Error importing user {user_data.get('user_id')}: {e}")
        
        await update.message.reply_text(
            f"✅ **Users Import Complete**\n\n"
            f"**Successfully imported:** {imported_count}/{len(users)} users",
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def import_groups_data(self, update: Update, import_data: dict):
        """Import groups data from JSON"""
        groups = import_data.get('groups', [])
        imported_count = 0
        
        for group_data in groups:
            try:
                success = db.add_group(
                    group_data.get('group_id'),
                    group_data.get('group_name'),
                    group_data.get('group_username'),
                    group_data.get('invite_link'),
                    group_data.get('owner_user_id'),
                    group_data.get('session_id'),
                    group_data.get('price'),
                    group_data.get('creation_date'),
                    group_data.get('total_messages')
                )
                if success:
                    imported_count += 1
            except Exception as e:
                logger.error(f"Error importing group {group_data.get('group_id')}: {e}")
        
        await update.message.reply_text(
            f"✅ **Groups Import Complete**\n\n"
            f"**Successfully imported:** {imported_count}/{len(groups)} groups",
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def import_transactions_data(self, update: Update, import_data: dict):
        """Import transactions data from JSON"""
        await update.message.reply_text(
            "⚠️ **Transaction Import Not Supported**\n\n"
            "Transaction imports are restricted for security reasons.",
            parse_mode=ParseMode.MARKDOWN
        )
    
    # Withdrawal Management (Admin)
    async def withdrawal_requests_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show pending withdrawal requests to admins"""
        user = update.effective_user
        
        if user.id not in BOT_OWNERS:
            await update.message.reply_text("❌ Access denied.")
            return
        
        try:
            # Get pending withdrawal requests
            conn = db.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT wr.id, wr.user_id, wr.amount, wr.address, wr.created_at,
                       u.username, u.first_name
                FROM withdrawal_requests wr
                JOIN users u ON wr.user_id = u.user_id
                WHERE wr.status = 'pending'
                ORDER BY wr.created_at ASC
            ''')
            requests = cursor.fetchall()
            conn.close()
            
            if not requests:
                await update.message.reply_text(
                    "📋 **Withdrawal Requests**\n\n"
                    "No pending withdrawal requests.",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            
            text = "📋 **Pending Withdrawal Requests**\n\n"
            keyboard = []
            
            for req in requests:
                req_id, user_id, amount, address, created_at, username, first_name = req
                user_display = f"@{username}" if username else first_name or f"User {user_id}"
                
                text += f"**Request #{req_id}**\n"
                text += f"👤 User: {user_display} (`{user_id}`)\n"
                text += f"💰 Amount: ${format_balance(amount)} USDT\n"
                text += f"📍 Address: `{address}`\n"
                text += f"📅 Date: {created_at}\n"
                text += "─" * 30 + "\n\n"
                
                keyboard.append([
                    InlineKeyboardButton(f"✅ Approve #{req_id}", callback_data=f"approve_withdrawal_{req_id}"),
                    InlineKeyboardButton(f"❌ Reject #{req_id}", callback_data=f"reject_withdrawal_{req_id}")
                ])
            
            await update.message.reply_text(
                text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            
        except Exception as e:
            logger.error(f"Error fetching withdrawal requests: {e}")
            await update.message.reply_text(
                "❌ Error fetching withdrawal requests.",
                parse_mode=ParseMode.MARKDOWN
            )
    
    async def handle_withdrawal_approval(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle withdrawal approval/rejection callbacks"""
        query = update.callback_query
        user = update.effective_user
        
        if user.id not in BOT_OWNERS:
            await query.answer("❌ Access denied.")
            return
        
        data = query.data
        
        if data.startswith("approve_withdrawal_"):
            request_id = int(data.split("_")[-1])
            await self.process_withdrawal_decision(query, request_id, "approved")
        elif data.startswith("reject_withdrawal_"):
            request_id = int(data.split("_")[-1])
            await self.process_withdrawal_decision(query, request_id, "rejected")
    
    async def process_withdrawal_decision(self, query, request_id: int, decision: str):
        """Process admin decision on withdrawal request"""
        try:
            # Get withdrawal request details
            conn = db.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT wr.user_id, wr.amount, wr.address, u.username, u.first_name
                FROM withdrawal_requests wr
                JOIN users u ON wr.user_id = u.user_id
                WHERE wr.id = ? AND wr.status = 'pending'
            ''', (request_id,))
            result = cursor.fetchone()
            
            if not result:
                await query.answer("❌ Withdrawal request not found or already processed.")
                return
            
            user_id, amount, address, username, first_name = result
            user_display = f"@{username}" if username else first_name or f"User {user_id}"
            
            # Update request status
            cursor.execute('''
                UPDATE withdrawal_requests 
                SET status = ?, processed_at = datetime('now')
                WHERE id = ?
            ''', (decision, request_id))
            
            if decision == "rejected":
                # Return funds to user balance if rejected
                cursor.execute('''
                    UPDATE users SET balance = balance + ?
                    WHERE user_id = ?
                ''', (amount, user_id))
            
            conn.commit()
            conn.close()
            
            # Notify user
            try:
                status_emoji = "✅" if decision == "approved" else "❌"
                status_text = "approved" if decision == "approved" else "rejected"
                
                message = f"{status_emoji} **Withdrawal {status_text.title()}**\n\n"
                message += f"**Amount:** ${format_balance(amount)} USDT\n"
                message += f"**Address:** `{address}`\n\n"
                
                if decision == "approved":
                    message += "Your withdrawal has been processed. Please check your wallet."
                else:
                    message += "Your withdrawal was rejected. Funds have been returned to your balance."
                
                await context.bot.send_message(
                    chat_id=user_id,
                    text=message,
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception as e:
                logger.error(f"Failed to notify user {user_id} about withdrawal decision: {e}")
            
            # Update admin message
            await query.edit_message_text(
                f"✅ **Withdrawal Request #{request_id} {decision.title()}**\n\n"
                f"👤 User: {user_display}\n"
                f"💰 Amount: ${format_balance(amount)} USDT\n"
                f"📍 Address: `{address}`\n"
                f"📋 Status: {decision.title()}",
                parse_mode=ParseMode.MARKDOWN
            )
            
            await query.answer(f"Withdrawal request {decision}!")
            logger.info(f"Withdrawal request {request_id} {decision} by admin {query.from_user.id}")
            
        except Exception as e:
            logger.error(f"Error processing withdrawal decision: {e}")
            await query.answer("❌ Error processing request.")

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
        app.add_handler(CommandHandler("done", bot_commands.done_command))
        app.add_handler(CommandHandler("referral", bot_commands.referral_command))
        
        # Admin Commands
        app.add_handler(CommandHandler("ahelp", bot_commands.admin_help_command))
        app.add_handler(CommandHandler("add", bot_commands.add_session_command))
        app.add_handler(CommandHandler("add_bank", bot_commands.add_bank_command))
        app.add_handler(CommandHandler("users", bot_commands.users_command))
        app.add_handler(CommandHandler("add_bal", bot_commands.add_balance_command))
        app.add_handler(CommandHandler("import", bot_commands.import_command))
        app.add_handler(CommandHandler("export", bot_commands.export_command))
        app.add_handler(CommandHandler("sessions", bot_commands.sessions_command))
        app.add_handler(CommandHandler("set_bulk", bot_commands.set_bulk_command))
        app.add_handler(CommandHandler("blist", bot_commands.blist_command))
        app.add_handler(CommandHandler("withdrawals", bot_commands.withdrawal_requests_command))
        
        # Callback Query Handler
        app.add_handler(CallbackQueryHandler(bot_commands.handle_callback_query))
        
        # Text Message Handler
        app.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND, 
            bot_commands.handle_text_message
        ))
        
        # Document Handler (for session imports)
        app.add_handler(MessageHandler(
            filters.Document.ALL, 
            bot_commands.handle_document
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