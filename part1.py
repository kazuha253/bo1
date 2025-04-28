import os
import logging
import asyncio
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
import telegram
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters
from dotenv import load_dotenv
from telegram.request import HTTPXRequest
import pandas as pd
import re
import string
import shutil
import csv

# Fungsi untuk memastikan direktori data ada
def ensure_data_directory():
    if not os.path.exists('data'):
        os.makedirs('data')

# Fungsi untuk mendapatkan username atau ID
def get_username_or_id(user):
    if user.username:
        return user.username, "---"
    else:
        return "---", str(user.id)

# Fungsi untuk mencatat data ke file CSV
def log_file_data(sender_user, forward_user, file_name):
    ensure_data_directory()

    # Dapatkan username atau ID pengirim
    sender_username, sender_id = get_username_or_id(sender_user)

    # Dapatkan username atau ID penerus
    if forward_user:
        forward_username, forward_id = get_username_or_id(forward_user)
    else:
        forward_username, forward_id = "---", "---"

    # Tentukan nama file CSV berdasarkan pengirim
    csv_file_name = f"data/{sender_username if sender_username != '---' else sender_id}.csv"

    # Periksa apakah file CSV sudah ada
    file_exists = os.path.exists(csv_file_name)

    # Tulis data ke file CSV
    with open(csv_file_name, mode='a', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file)
        if not file_exists:
            # Tulis header jika file baru
            writer.writerow(["username_pengirim", "ID_pengirim", "username_penerus", "ID_penerus", "nama_file"])
        # Tulis data
        writer.writerow([sender_username, sender_id, forward_username, forward_id, file_name])

# Konfigurasi logging
logging.basicConfig(
    format='%(levelname)s: %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Menghilangkan log httpx INFO
httpx_logger = logging.getLogger("httpx")
httpx_logger.setLevel(logging.WARNING)

# Load environment variables dari .env
load_dotenv()

TELEGRAM_BOT_API_TOKEN = os.getenv('TELEGRAM_BOT_API_TOKEN')
AUTHORIZED_PASSWORD = os.getenv('AUTHORIZED_PASSWORD')

# Variabel untuk melacak pengguna yang aktif
active_users = set()

# Inisialisasi Semaphore untuk membatasi jumlah tugas yang berjalan bersamaan
semaphore = asyncio.Semaphore(50)

# Fungsi untuk mendapatkan identitas pengguna
def get_user_identity(update: Update) -> str:
    return update.message.from_user.username

# Fungsi untuk menghapus emoji dari string
def remove_emoji(text):
    return re.sub(r'[^\w\s]', '', text)

# Fungsi untuk membersihkan nomor telepon
def clean_phone_number(number):
    number = re.sub(r'\D', '', number)  # Hapus semua karakter non-numerik
    if len(number) >= 8:
        if not number.startswith('+'):
            number = '+' + number
        return number
    return None

def clean_filename(filename):
    return re.sub(r'[\\/:*?"<>|]', '_', filename)

def clean_contact_name(contact_name):
    return re.sub(r'[\\/:*?"<>|]', '_', contact_name)

# Fungsi untuk menghapus semua file di dalam folder cache
async def remove_cache_files(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_identity = get_user_identity(update)
    logger.info(f"User {user_identity} issued /remove command.")
    
    # Bagian ini perlu dikomentari
    # if user_identity != "Karin383":
    #     await send_message_with_retry(context, update.message.chat_id, "Anda tidak memiliki izin untuk mengakses perintah ini.")
    #     logger.info(f"User {user_identity} attempted to access /remove without permission.")
    #     return

    # if user_identity not in active_users:
    #     await send_message_with_retry(context, update.message.chat_id, "Masukkan password:")
    #     context.user_data['awaiting_password'] = True
    #     logger.info("Bot response: Masukkan password:")
    #     return

    cache_folder = 'cache'
    if os.path.exists(cache_folder):
        shutil.rmtree(cache_folder)
        os.makedirs(cache_folder)
        await send_message_with_retry(context, update.message.chat_id, "Semua file di dalam folder cache telah dihapus.")
        logger.info("Bot response: Semua file di dalam folder cache telah dihapus.")
    else:
        await send_message_with_retry(context, update.message.chat_id, "Folder cache tidak ditemukan.")
        logger.info("Bot response: Folder cache tidak ditemukan.")