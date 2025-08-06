import asyncio
import hashlib
import os
import json
from datetime import datetime, timedelta
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
from config import SESSIONS_DIR
from database import db

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
            # Create temporary client for authentication
            session_file = os.path.join(SESSIONS_DIR, f"temp_{user_id}_{phone_number}")
            client = TelegramClient(session_file, api_id, api_hash)
            
            await client.connect()
            
            # Send code request
            sent_code = await client.send_code_request(phone_number)
            
            # Store pending auth data
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
            print(f"Error starting auth process: {e}")
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
            
            # Check if 2FA is enabled
            me = await client.get_me()
            auth_data['me'] = me
            auth_data['step'] = 'completed'
            
            return True, "Code verified successfully"
            
        except SessionPasswordNeededError:
            # 2FA is enabled
            auth_data['step'] = 'password'
            return True, "2FA password required"
            
        except PhoneCodeInvalidError:
            return False, "Invalid code"
        except Exception as e:
            print(f"Error verifying code: {e}")
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
            print(f"Error verifying password: {e}")
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
            # Get session string
            session_string = client.session.save()
            
            # Hash 2FA password if provided
            password_hash = None
            has_2fa = False
            if 'password' in auth_data:
                password_hash = self.hash_password(auth_data['password'])
                has_2fa = True
            
            # Save to database
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
                # Move session file to permanent location
                permanent_file = os.path.join(SESSIONS_DIR, f"{user_id}_{auth_data['phone_number']}")
                os.rename(auth_data['session_file'] + ".session", permanent_file + ".session")
                
                # Clean up
                await client.disconnect()
                del self.pending_auth[user_id]
                
                return True, "Session saved successfully"
            else:
                return False, "Failed to save session (phone number may already exist)"
                
        except Exception as e:
            print(f"Error completing auth: {e}")
            return False, str(e)
    
    async def import_session_file(self, user_id: int, session_file_path: str, password: str = None):
        """Import session from .session file"""
        try:
            # Try to create client from session file
            client = TelegramClient(session_file_path.replace('.session', ''), 
                                  api_id='dummy', api_hash='dummy')
            
            await client.connect()
            
            # Check if password is needed
            if not await client.is_user_authorized():
                if password:
                    try:
                        await client.sign_in(password=password)
                    except PasswordHashInvalidError:
                        return False, "Invalid 2FA password"
                else:
                    return False, "2FA password required but not provided"
            
            # Get user info
            me = await client.get_me()
            
            # Get session string
            session_string = client.session.save()
            
            # Hash password if provided
            password_hash = None
            has_2fa = False
            if password:
                password_hash = self.hash_password(password)
                has_2fa = True
            
            # Save to database (use phone from session)
            phone_number = f"+{me.phone}" if me.phone else "imported"
            success = db.add_session(
                user_id=user_id,
                api_id=0,  # Unknown for imported sessions
                api_hash="imported",
                phone_number=phone_number,
                session_string=session_string,
                password_hash=password_hash,
                has_2fa=has_2fa
            )
            
            if success:
                # Copy session file to permanent location
                permanent_file = os.path.join(SESSIONS_DIR, f"{user_id}_{phone_number}")
                import shutil
                shutil.copy2(session_file_path, permanent_file + ".session")
                
                await client.disconnect()
                return True, "Session imported successfully"
            else:
                await client.disconnect()
                return False, "Failed to save session (phone number may already exist)"
                
        except Exception as e:
            print(f"Error importing session: {e}")
            return False, str(e)
    
    async def get_client(self, session_id: int):
        """Get active client for session"""
        if session_id in self.active_sessions:
            return self.active_sessions[session_id]
        
        # Get session from database
        sessions = db.get_user_sessions(0)  # Will modify to get specific session
        session_data = None
        for s in sessions:
            if s['id'] == session_id:
                session_data = s
                break
        
        if not session_data:
            return None
        
        try:
            # Create client from session string
            client = TelegramClient(
                session=session_data['session_string'],
                api_id=session_data['api_id'],
                api_hash=session_data['api_hash']
            )
            
            await client.connect()
            
            if not await client.is_user_authorized():
                return None
            
            self.active_sessions[session_id] = client
            return client
            
        except Exception as e:
            print(f"Error getting client: {e}")
            return None
    
    async def check_group_ownership(self, client: TelegramClient, group_id: int):
        """Check if the session user is the owner of the group"""
        try:
            # Get group entity
            entity = await client.get_entity(group_id)
            
            # Get current user
            me = await client.get_me()
            
            # Check if supergroup/channel
            if not hasattr(entity, 'megagroup') and not hasattr(entity, 'broadcast'):
                return False, "Not a supergroup or channel"
            
            # Check if private
            if hasattr(entity, 'username') and entity.username:
                return False, "Group must be private"
            
            # Get admin list
            participants = await client(GetParticipantsRequest(
                entity, ChannelParticipantsAdmins(), offset=0, limit=100, hash=0
            ))
            
            # Check if user is the creator/owner
            for participant in participants.participants:
                if participant.user_id == me.id:
                    return hasattr(participant, 'admin_rights') and getattr(participant, 'rank', '') == 'creator', "User is owner"
            
            return False, "User is not the owner"
            
        except Exception as e:
            print(f"Error checking ownership: {e}")
            return False, str(e)
    
    async def get_group_info(self, client: TelegramClient, group_id: int):
        """Get group information"""
        try:
            entity = await client.get_entity(group_id)
            
            # Get full info
            full_info = await client.get_entity(entity)
            
            # Get creation date
            creation_date = entity.date.strftime('%Y-%m-%d') if entity.date else None
            
            # Get message count (approximate)
            try:
                messages = await client.get_messages(entity, limit=1)
                total_messages = messages.total or 0
            except:
                total_messages = 0
            
            # Generate invite link
            try:
                invite = await client(ExportChatInviteRequest(entity))
                invite_link = invite.link
            except:
                invite_link = None
            
            return {
                'group_id': entity.id,
                'title': entity.title,
                'username': getattr(entity, 'username', None),
                'creation_date': creation_date,
                'total_messages': total_messages,
                'invite_link': invite_link,
                'is_megagroup': getattr(entity, 'megagroup', False),
                'is_private': not getattr(entity, 'username', None)
            }
            
        except Exception as e:
            print(f"Error getting group info: {e}")
            return None
    
    async def transfer_ownership(self, client: TelegramClient, group_id: int, new_owner_id: int, password: str = None):
        """Transfer group ownership to new user"""
        try:
            # Get group entity
            entity = await client.get_entity(group_id)
            
            # Get new owner entity
            new_owner = await client.get_entity(new_owner_id)
            
            # First, make the new user an admin with all rights
            admin_rights = ChatAdminRights(
                change_info=True,
                post_messages=True,
                edit_messages=True,
                delete_messages=True,
                ban_users=True,
                invite_users=True,
                pin_messages=True,
                add_admins=True,
                manage_call=True,
                other=True
            )
            
            # Add new owner as admin
            await client(EditAdminRequest(
                channel=entity,
                user_id=new_owner,
                admin_rights=admin_rights,
                rank="Owner"
            ))
            
            # Note: Telegram doesn't allow direct ownership transfer via API
            # This makes the user a full admin with owner privileges
            # Manual transfer would need to be done by the original owner
            
            return True, "User promoted to admin with full rights"
            
        except Exception as e:
            print(f"Error transferring ownership: {e}")
            return False, str(e)
    
    async def check_user_in_group(self, client: TelegramClient, group_id: int, user_id: int):
        """Check if user is a member of the group"""
        try:
            entity = await client.get_entity(group_id)
            
            # Try to get the user in the group
            try:
                participant = await client.get_participants(entity, search=str(user_id), limit=1)
                return len(participant) > 0
            except:
                # Alternative method
                try:
                    user_entity = await client.get_entity(user_id)
                    participants = await client.get_participants(entity, limit=1000)
                    return any(p.id == user_id for p in participants)
                except:
                    return False
                    
        except Exception as e:
            print(f"Error checking user in group: {e}")
            return False
    
    def cleanup_pending_auth(self, user_id: int):
        """Clean up pending authentication data"""
        if user_id in self.pending_auth:
            auth_data = self.pending_auth[user_id]
            if 'client' in auth_data:
                try:
                    asyncio.create_task(auth_data['client'].disconnect())
                except:
                    pass
            
            # Remove temporary session file
            if 'session_file' in auth_data:
                try:
                    os.remove(auth_data['session_file'] + '.session')
                except:
                    pass
            
            del self.pending_auth[user_id]

# Global session manager instance
session_manager = SessionManager()