import asyncio
import json
import os
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler
from telegram.constants import ParseMode

from config import BOT_OWNERS, BANK_GROUP_ID, CCTIP_BOT_ID, LISTING_TIMEOUT
from database import db
from session_handler import session_manager
from utils import (
    validate_price, validate_phone_number, validate_api_credentials,
    validate_buying_ids, validate_withdrawal_amount, validate_polygon_address,
    format_price, format_balance, format_user_link, format_group_name,
    format_buying_id, parse_tip_message, create_market_keyboard,
    create_month_keyboard, create_groups_keyboard, create_group_list_keyboard,
    create_confirmation_keyboard, create_users_keyboard, escape_markdown,
    truncate_text, get_available_years, is_group_valid_for_listing,
    generate_help_text, generate_admin_help_text
)

# Conversation states
(ADD_API_ID, ADD_API_HASH, ADD_PHONE, ADD_CODE, ADD_PASSWORD,
 IMPORT_SESSION_PASSWORD, LIST_PRICE, LIST_USERBOT,
 WITHDRAW_AMOUNT, WITHDRAW_ADDRESS, ADD_BAL_USER, ADD_BAL_AMOUNT) = range(12)

class BotCommands:
    def __init__(self):
        self.user_contexts = {}
        self.pending_purchases = {}
        
    # User Commands
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command"""
        user = update.effective_user
        chat = update.effective_chat
        
        # Add user to database
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
        
        # Parse buying IDs
        buying_ids_str = " ".join(context.args)
        buying_ids = validate_buying_ids(buying_ids_str)
        
        if not buying_ids:
            await update.message.reply_text(
                "❌ Invalid buying ID format.\n\n"
                "Buying IDs should be in format: `G123ABC`",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        # Check if groups exist and calculate total cost
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
        
        # Check user balance
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
        
        # Process purchase
        success = db.purchase_groups(user.id, buying_ids)
        
        if not success:
            await update.message.reply_text(
                "❌ Purchase failed. Please try again or contact support.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        # Send purchase confirmation with invite links
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
        """Handle /claim command (must be used in group)"""
        user = update.effective_user
        chat = update.effective_chat
        
        if chat.type not in ['group', 'supergroup']:
            await update.message.reply_text(
                "❌ This command can only be used in groups.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        # Check if group is in database and was purchased by user
        group = db.get_group_by_buying_id("")  # Need to modify to get by group_id
        # This is a simplified version - would need proper group lookup
        
        # For now, send a placeholder response
        await update.message.reply_text(
            "🔄 Processing ownership transfer...\n\n"
            "Please wait while we verify your purchase and transfer ownership.",
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def list_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /list command (must be used in group)"""
        user = update.effective_user
        chat = update.effective_chat
        
        if chat.type not in ['group', 'supergroup']:
            await update.message.reply_text(
                "❌ This command can only be used in groups you want to sell.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        # Start listing conversation
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
        
        # Check if user owns this listed group
        # Implementation would check database for ownership
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
        
        # Update price in database
        # Implementation would update the group price
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
        """Handle /add command for adding userbot sessions"""
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
        
        keyboard = create_users_keyboard(users, page, 10)
        
        await update.message.reply_text(
            text, 
            reply_markup=keyboard, 
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
        
        # Update balance
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
        elif data.startswith('price_'):
            await self.handle_price_selection(query, context)
        elif data.startswith('group_'):
            await self.handle_group_selection(query, context)
        elif data.startswith('confirm_'):
            await self.handle_confirmation(query, context)
        elif data.startswith('cancel_'):
            await self.handle_cancellation(query, context)
        elif data == 'market_back':
            await self.market_command(update, context)
        # Add more callback handlers as needed
    
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
        user_context['state'] = 'waiting_userbot'
        
        # Get available userbot sessions
        sessions = db.get_user_sessions(user.id)  # This would need to be admin sessions
        
        if not sessions:
            await update.message.reply_text(
                "❌ No userbot sessions available. Please contact an administrator.",
                parse_mode=ParseMode.MARKDOWN
            )
            del self.user_contexts[user.id]
            return
        
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
        
        # Create withdrawal request
        success = db.add_withdrawal_request(user.id, amount, address)
        
        if success:
            keyboard = create_confirmation_keyboard('withdraw', f"{amount}_{address}")
            
            text = f"""
💸 **Confirm Withdrawal**

**Amount:** ${format_price(amount)} USDT
**Address:** `{address}`
**Fee:** $0.00 USDT
**You'll Receive:** ${format_price(amount)} USDT

⚠️ **Important:** This action cannot be undone.

Confirm withdrawal?
"""
            
            await update.message.reply_text(
                text, 
                reply_markup=keyboard, 
                parse_mode=ParseMode.MARKDOWN
            )
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
        
        # Start authentication process
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
                # Complete authentication
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
            # Complete authentication
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
        
        # Check if message is from cctip bot in bank group
        if (message.from_user.id != CCTIP_BOT_ID or 
            message.chat.id != BANK_GROUP_ID):
            return
        
        # Parse tip information
        tip_info = parse_tip_message(message.text)
        
        if not tip_info or not tip_info['valid']:
            return
        
        # Extract recipient information from message
        # This would need more sophisticated parsing
        # For now, placeholder implementation
        
        # Update user balance
        # recipient_user_id = extract_recipient_from_message(message.text)
        # if recipient_user_id:
        #     db.update_user_balance(recipient_user_id, tip_info['amount'], 'tip')
    
    # Document Handler for Session Import
    async def handle_document(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle document uploads (for session import)"""
        user = update.effective_user
        
        if user.id not in BOT_OWNERS:
            return
        
        document = update.message.document
        
        if not document.file_name.endswith('.session'):
            await update.message.reply_text(
                "❌ Please send a valid .session file.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        # Handle session file import
        file = await context.bot.get_file(document.file_id)
        file_path = f"/tmp/{document.file_name}"
        await file.download_to_drive(file_path)
        
        # Ask for 2FA password if needed
        text = """
📁 **Session File Received**

If this session has 2-step verification enabled, please enter the password.
If not, type `skip`:
"""
        
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
        
        self.user_contexts[user.id] = {
            'state': 'waiting_import_password',
            'session_file': file_path
        }
    
    # Bulk Listing Commands
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
        
        text = f"""
📦 **Bulk Listing - {keyword.upper()}**

**Target Date:** {date_str}
**Group:** {chat.title}

**Instructions:**
1. Add our userbot to this group as admin
2. Transfer group ownership to the userbot
3. Type `/done` when ownership transfer is complete

**⚠️ Important:**
• You must transfer actual ownership (not just admin rights)
• Only the original group owner should use `/done`
• Group must meet listing requirements (private, 4+ messages, etc.)

**Userbot to add:** @example_userbot
"""
        
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
        
        # Add to pending bulk listings
        self.add_pending_bulk_listing(user.id, chat.id, keyword_data)
    
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

# Global instance
bot_commands = BotCommands()