#!/usr/bin/env python3
"""
Telegram Group Market Bot
A bot for buying and selling Telegram groups with secure ownership transfer.
"""

import asyncio
import logging
import sys
import signal
from datetime import datetime

from telegram import Update
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    filters, ContextTypes
)
from telegram.error import NetworkError, TimedOut, BadRequest

# Import our modules
from config import BOT_TOKEN, BOT_OWNERS, BANK_GROUP_ID, CCTIP_BOT_ID
from database import db
from commands_features import bot_commands
from session_handler import session_manager

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

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
        app.add_handler(CommandHandler("set_bulk", bot_commands.set_bulk_command))
        app.add_handler(CommandHandler("blist", bot_commands.blist_command))
        
        # Admin Commands
        app.add_handler(CommandHandler("ahelp", bot_commands.admin_help_command))
        app.add_handler(CommandHandler("add", bot_commands.add_session_command))
        app.add_handler(CommandHandler("add_bank", bot_commands.add_session_command))  # Same as add for now
        app.add_handler(CommandHandler("users", bot_commands.users_command))
        app.add_handler(CommandHandler("add_bal", bot_commands.add_balance_command))
        
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
        
        # Log the error
        logger.error(f"Exception while handling an update: {error}")
        
        # Handle different types of errors
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
        
        # Try to send error message to user if possible
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
                        await asyncio.sleep(2 ** attempt)  # Exponential backoff
                except Exception as e:
                    logger.error(f"Unexpected error sending startup message: {e}")
                    break
    
    async def shutdown_message(self):
        """Send shutdown message to bot owners"""
        for owner_id in BOT_OWNERS:
            try:
                await self.application.bot.send_message(
                    chat_id=owner_id,
                    text="🤖 **Bot Shutting Down**\n\n"
                         f"**Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                         f"**Status:** ⏹️ Stopping",
                    parse_mode='Markdown'
                )
                logger.info(f"Shutdown message sent to owner {owner_id}")
            except Exception as e:
                logger.error(f"Failed to send shutdown message: {e}")
    
    def signal_handler(self, signum, frame):
        """Handle system signals for graceful shutdown"""
        logger.info(f"Received signal {signum}. Shutting down gracefully...")
        self.is_running = False
    
    async def run(self):
        """Run the bot with improved error handling and recovery"""
        try:
            # Set up signal handlers
            signal.signal(signal.SIGINT, self.signal_handler)
            signal.signal(signal.SIGTERM, self.signal_handler)
            
            # Create application with improved settings
            self.application = (
                Application.builder()
                .token(BOT_TOKEN)
                .read_timeout(30)
                .write_timeout(30)
                .connect_timeout(30)
                .pool_timeout(30)
                .build()
            )
            
            # Setup handlers
            self.setup_handlers()
            
            # Initialize application
            await self.application.initialize()
            
            # Start the bot
            await self.application.start()
            self.is_running = True
            
            # Send startup message
            await self.startup_message()
            
            logger.info("🤖 Telegram Group Market Bot started successfully!")
            
            try:
                bot_info = await self.application.bot.get_me()
                logger.info(f"Bot username: @{bot_info.username}")
            except Exception as e:
                logger.warning(f"Could not get bot info: {e}")
            
            # Start polling with improved settings
            await self.application.updater.start_polling(
                allowed_updates=Update.ALL_TYPES,
                drop_pending_updates=True,
                read_timeout=30,
                write_timeout=30,
                connect_timeout=30,
                pool_timeout=30
            )
            
            # Keep the bot running
            while self.is_running:
                try:
                    await asyncio.sleep(1)
                except asyncio.CancelledError:
                    break
            
        except Exception as e:
            logger.error(f"Critical error in bot execution: {e}")
            raise
        finally:
            # Cleanup
            logger.info("Bot shutting down...")
            self.is_running = False
            
            try:
                await self.shutdown_message()
            except Exception as e:
                logger.error(f"Error sending shutdown message: {e}")
            
            if self.application:
                try:
                    if self.application.updater.running:
                        await self.application.updater.stop()
                    await self.application.stop()
                    await self.application.shutdown()
                except Exception as e:
                    logger.error(f"Error during cleanup: {e}")
            
            logger.info("Bot shutdown complete")

def main():
    """Main function with improved error handling"""
    try:
        # Check if database is accessible
        logger.info("Checking database connection...")
        total_users = db.get_total_users_count()
        logger.info(f"Database connected. Total users: {total_users}")
        
        # Check sessions directory
        import os
        if not os.path.exists('sessions'):
            os.makedirs('sessions')
            logger.info("Created sessions directory")
        
        # Validate bot token
        if not BOT_TOKEN or BOT_TOKEN == "your_bot_token_here":
            logger.error("Invalid bot token. Please configure BOT_TOKEN in config.py")
            sys.exit(1)
        
        # Initialize and run bot
        bot = TelegramMarketBot()
        
        # Run the bot
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
║                                                              ║
║  A secure marketplace for buying and selling Telegram       ║
║  groups with automated ownership transfer.                  ║
║                                                              ║
║  Features:                                                   ║
║  • Secure group trading with escrow                         ║
║  • Automated ownership transfer via userbots                ║
║  • Balance management with USDT payments                    ║
║  • Admin panel for user and session management              ║
║                                                              ║
║  Starting bot...                                             ║
╚══════════════════════════════════════════════════════════════╝
    """)
    
    main()
