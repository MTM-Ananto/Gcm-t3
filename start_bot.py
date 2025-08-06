#!/usr/bin/env python3
"""
Simple startup script for the Telegram Group Market Bot
"""

import os
import sys
import subprocess

def check_requirements():
    """Check if all requirements are met"""
    print("🔍 Checking requirements...")
    
    # Check Python version
    if sys.version_info < (3, 8):
        print("❌ Python 3.8 or higher is required")
        return False
    
    print(f"✅ Python {sys.version_info.major}.{sys.version_info.minor} detected")
    
    # Check if config file exists
    if not os.path.exists('config.py'):
        print("❌ config.py not found")
        return False
    
    print("✅ config.py found")
    
    # Try to import config
    try:
        from config import BOT_TOKEN, BOT_OWNERS
        if not BOT_TOKEN or BOT_TOKEN == "your_bot_token_here":
            print("❌ Bot token not configured. Please edit config.py")
            return False
        
        if not BOT_OWNERS:
            print("❌ Bot owners not configured. Please edit config.py")
            return False
        
        print("✅ Configuration validated")
    except ImportError as e:
        print(f"❌ Error importing config: {e}")
        return False
    
    # Check if database module can be imported
    try:
        from database import db
        print("✅ Database module loaded")
    except ImportError as e:
        print(f"❌ Error importing database module: {e}")
        print("💡 Try: pip install -r requirements.txt")
        return False
    
    return True

def install_requirements():
    """Install requirements if needed"""
    print("📦 Installing requirements...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("✅ Requirements installed successfully")
        return True
    except subprocess.CalledProcessError:
        print("❌ Failed to install requirements")
        print("💡 Try running manually: pip install -r requirements.txt")
        return False

def main():
    """Main startup function"""
    print("""
╔══════════════════════════════════════════════════════════════╗
║                  Telegram Group Market Bot                  ║
║                        Startup Script                       ║
╚══════════════════════════════════════════════════════════════╝
    """)
    
    # Check if requirements file exists
    if not os.path.exists('requirements.txt'):
        print("❌ requirements.txt not found")
        sys.exit(1)
    
    # Check requirements
    if not check_requirements():
        print("\n🔧 Attempting to install missing requirements...")
        if not install_requirements():
            print("\n❌ Setup failed. Please install requirements manually.")
            sys.exit(1)
        
        # Check again after installation
        if not check_requirements():
            print("\n❌ Setup validation failed.")
            sys.exit(1)
    
    print("\n🚀 Starting bot...")
    
    # Import and run the main bot
    try:
        from main import main as bot_main
        bot_main()
    except KeyboardInterrupt:
        print("\n👋 Bot stopped by user")
    except Exception as e:
        print(f"\n❌ Error starting bot: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()