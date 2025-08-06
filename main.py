#!/usr/bin/env python3
"""
Telegram Group Market Bot
A bot for buying and selling Telegram groups with secure ownership transfer.
"""

import asyncio
import logging
import sys
from datetime import datetime

from telegram import Update
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    filters, ContextTypes
)

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
        """Handle errors"""
        logger.error(f"Exception while handling an update: {context.error}")
        
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
        """Send startup message to bot owners"""
        try:
            for owner_id in BOT_OWNERS:
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
        except Exception as e:
            logger.error(f"Failed to send startup message: {e}")
    
    async def shutdown_message(self):
        """Send shutdown message to bot owners"""
        try:
            for owner_id in BOT_OWNERS:
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
    
    async def run(self):
        """Run the bot"""
        try:
            # Create application
            self.application = Application.builder().token(BOT_TOKEN).build()
            
            # Setup handlers
            self.setup_handlers()
            
            # Initialize application
            await self.application.initialize()
            
            # Start the bot
            await self.application.start()
            
            # Send startup message
            await self.startup_message()
            
            logger.info("🤖 Telegram Group Market Bot started successfully!")
            logger.info(f"Bot username: @{self.application.bot.username}")
            
            # Start polling
            await self.application.updater.start_polling(
                allowed_updates=Update.ALL_TYPES,
                drop_pending_updates=True
            )
            
            # Keep the bot running
            await self.application.updater.idle()
            
        except Exception as e:
            logger.error(f"Critical error in bot execution: {e}")
            raise
        finally:
            # Cleanup
            logger.info("Bot shutting down...")
            await self.shutdown_message()
            
            if self.application:
                await self.application.stop()
                await self.application.shutdown()
            
            logger.info("Bot shutdown complete")

def main():
    """Main function"""
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
