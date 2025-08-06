import sqlite3
import json
import hashlib
from datetime import datetime
from typing import Optional, List, Dict, Any
import asyncio
import threading
from config import DATABASE_URL

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
                    transaction_type TEXT, -- 'purchase', 'tip', 'withdrawal', 'refund'
                    amount REAL,
                    group_ids TEXT, -- JSON array of group IDs for purchases
                    status TEXT DEFAULT 'pending', -- 'pending', 'completed', 'failed'
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
                    status TEXT DEFAULT 'pending', -- 'pending', 'completed', 'rejected'
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed_at TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (user_id)
                )
            ''')
            
            # Group codes mapping (permanent storage)
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
                
                # Update existing user info
                cursor.execute('''
                    UPDATE users SET username = ?, first_name = ?
                    WHERE user_id = ?
                ''', (username, first_name, user_id))
                
                conn.commit()
                conn.close()
                return True
            except Exception as e:
                print(f"Error adding user: {e}")
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
                
                # Update balance
                cursor.execute('''
                    UPDATE users SET balance = balance + ?
                    WHERE user_id = ?
                ''', (amount, user_id))
                
                # Record transaction
                cursor.execute('''
                    INSERT INTO transactions (user_id, transaction_type, amount, status)
                    VALUES (?, ?, ?, 'completed')
                ''', (user_id, transaction_type, amount))
                
                # Update total volume if positive amount
                if amount > 0:
                    cursor.execute('''
                        UPDATE users SET total_volume = total_volume + ?
                        WHERE user_id = ?
                    ''', (amount, user_id))
                
                conn.commit()
                conn.close()
                return True
            except Exception as e:
                print(f"Error updating balance: {e}")
                return False
    
    def add_session(self, user_id: int, api_id: int, api_hash: str, phone_number: str, 
                   session_string: str, password_hash: str = None, has_2fa: bool = False) -> bool:
        with self.lock:
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                
                # Check if phone number already exists
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
                print(f"Error adding session: {e}")
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
            
            # Check if group already has a buying ID
            cursor.execute('SELECT buying_id FROM group_codes WHERE group_id = ?', (group_id,))
            result = cursor.fetchone()
            
            if result:
                conn.close()
                return result[0]
            
            # Generate new buying ID
            import random
            import string
            while True:
                buying_id = 'G' + ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
                cursor.execute('SELECT group_id FROM group_codes WHERE buying_id = ?', (buying_id,))
                if not cursor.fetchone():
                    break
            
            # Store the mapping
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
                print(f"Error adding group: {e}")
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
                
                # Calculate total cost
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
                
                # Check user balance
                cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
                balance = cursor.fetchone()[0]
                
                if balance < total_cost:
                    conn.close()
                    return False
                
                # Deduct balance
                cursor.execute('UPDATE users SET balance = balance - ? WHERE user_id = ?', 
                             (total_cost, user_id))
                
                # Mark groups as unlisted
                for group in group_data:
                    cursor.execute('UPDATE groups SET is_listed = FALSE WHERE buying_id = ?', 
                                 (group['buying_id'],))
                
                # Record transaction
                cursor.execute('''
                    INSERT INTO transactions (user_id, transaction_type, amount, group_ids, status)
                    VALUES (?, 'purchase', ?, ?, 'completed')
                ''', (user_id, -total_cost, json.dumps([g['group_id'] for g in group_data])))
                
                conn.commit()
                conn.close()
                return True
            except Exception as e:
                print(f"Error purchasing groups: {e}")
                return False
    
    def add_withdrawal_request(self, user_id: int, amount: float, address: str) -> bool:
        with self.lock:
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                
                # Check balance
                cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
                balance = cursor.fetchone()[0]
                
                if balance < amount:
                    conn.close()
                    return False
                
                # Deduct balance
                cursor.execute('UPDATE users SET balance = balance - ? WHERE user_id = ?', 
                             (amount, user_id))
                
                # Add withdrawal request
                cursor.execute('''
                    INSERT INTO withdrawal_requests (user_id, amount, address)
                    VALUES (?, ?, ?)
                ''', (user_id, amount, address))
                
                conn.commit()
                conn.close()
                return True
            except Exception as e:
                print(f"Error adding withdrawal request: {e}")
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

# Global database instance
db = Database()