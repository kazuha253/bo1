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

# Fungsi untuk menampilkan menu utama
async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_name = update.message.from_user.first_name
    try:
        await asyncio.wait_for(context.bot.send_message(
            chat_id=update.message.chat_id,
            text=(
                f"Selamat datang <b>{user_name}</b>\n\n"
                "Gunakan perintah:\n"
                "/convert - Konversi file ke .vcf\n"
                "/extract - Konversi file ke .txt\n"
                "/admin - Konversi admin dan navy\n"
                "/manual - Konversi secara manual\n"
                "/tambah - Tambahkan kontak ke .vcf\n"
                "/hapus - Hapus kontak dari file\n"
                "/gabung - Gabungkan file\n"
                "/pecah - Pecah file\n"
                "/rename_ctc - Ganti nama kontak\n"
                "/rename_file - Ganti nama file\n"
                "/jumlah - Hitung jumlah kontak\n"
                "/hapus_duplikat - Hapus kontak duplikat\n"
                "/rapih - Rapihkan nomor\n\n"
                "<b>Dibuat oleh @Karin383</b>"
            ),
            parse_mode='HTML'
        ), timeout=60)
    except asyncio.TimeoutError:
        logger.error("Timeout error in show_main_menu")
        await send_message_with_retry(context, update.message.chat_id, "server sedang sibuk, harap tunggu")

async def retry_operation(operation, max_retries=10, delay=6):
    for attempt in range(max_retries):
        try:
            await operation()
            return
        except asyncio.TimeoutError:
            if attempt < max_retries - 1:
                logger.warning(f"Retrying operation. Attempt {attempt + 1} of {max_retries}.")
                await asyncio.sleep(delay)
            else:
                logger.error(f"Operation failed after {max_retries} attempts.")
                raise

# Fungsi untuk mengirim pesan dengan mekanisme retry
async def send_message_with_retry(context, chat_id, text, retries=10, delay=6):
    for attempt in range(retries):
        try:
            await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=text), timeout=60)
            logger.info(f"Message sent to {chat_id} on attempt {attempt + 1}")
            return
        except (telegram.error.TimedOut, asyncio.TimeoutError):
            if attempt < retries - 1:
                logger.warning(f"Retrying to send message to {chat_id}. Attempt {attempt + 1} of {retries}.")
                await asyncio.sleep(delay)
            else:
                logger.error(f"Failed to send message to {chat_id} after {retries} attempts.")
                await context.bot.send_message(chat_id=chat_id, text="server error, silahkan coba lagi")
                logger.error("Timeout error in send_message_with_retry")
        except Exception as e:
            logger.error(f"Error in send_message_with_retry: {e}")
            await context.bot.send_message(chat_id=chat_id, text="server sedang sibuk, harap tunggu")
            logger.error("Error in send_message_with_retry")

# Fungsi untuk memulai bot
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_identity = get_user_identity(update)
    user_name = update.message.from_user.first_name
    logger.info(f"User {user_identity} started the bot.")
    
    # Bagian ini perlu dikomentari
    # if user_identity not in active_users:
    #     await send_message_with_retry(context, update.message.chat_id, "Masukkan password:")
    #     context.user_data['awaiting_password'] = True
    #     logger.info("Bot response: Masukkan password:")
    # else:
    #     # Hapus file dari perintah sebelumnya
    if 'file_paths' in context.user_data:
        for file_path in context.user_data['file_paths']:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted user uploaded file: {file_path}")
        context.user_data.clear()
    await show_main_menu(update, context)
    logger.info(f"Bot response: Selamat datang {user_name}\n\nGunakan perintah:\n/convert - Konversi file ke vcf\n/admin - Konversi admin dan navy\n/manual - Konversi secara manual\n/extract - Konversi file ke txt\n/tambah - Tambahkan kontak ke vcf\n\nDibuat oleh @Karin383")

# Fungsi untuk menangani perintah /status
async def status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_identity = get_user_identity(update)
    if user_identity != "Karin383":
        await send_message_with_retry(context, update.message.chat_id, "Anda tidak memiliki izin untuk mengakses perintah ini.")
        logger.info(f"User {user_identity} attempted to access /status without permission.")
        return

    if not active_users:
        await send_message_with_retry(context, update.message.chat_id, "Tidak ada pengguna aktif.")
        logger.info("Bot response: Tidak ada pengguna aktif.")
        return

    active_users_list = "\n".join([f"{i+1}. @{user}" for i, user in enumerate(active_users)])
    await send_message_with_retry(context, update.message.chat_id, f"{len(active_users)} pengguna aktif:\n{active_users_list}")
    logger.info(f"Bot response: {len(active_users)} pengguna aktif:\n{active_users_list}")

# Fungsi untuk menangani perintah /convert
async def convert(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_identity = get_user_identity(update)
    logger.info(f"User {user_identity} issued /convert command.")
    
    # Bagian ini perlu dikomentari
    # if user_identity not in active_users:
    #     await send_message_with_retry(context, update.message.chat_id, "Masukkan password:")
    #     context.user_data['awaiting_password'] = True
    #     logger.info("Bot response: Masukkan password:")
    #     return

    # Hapus file dari perintah sebelumnya
    if 'file_paths' in context.user_data:
        for file_path in context.user_data['file_paths']:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted user uploaded file: {file_path}")
    context.user_data.clear()
    context.user_data['in_convert'] = True
    await send_message_with_retry(context, update.message.chat_id, "Kirim file .txt atau .xlsx\nMaksimal 20 file:")
    logger.info("Bot response: Kirim file .txt atau .xlsx\nMaksimal 20 file:")

# Fungsi untuk menangani perintah /admin
async def admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_identity = get_user_identity(update)
    logger.info(f"User {user_identity} issued /admin command.")
    
    # Bagian ini perlu dikomentari
    # if user_identity not in active_users:
    #     await send_message_with_retry(context, update.message.chat_id, "Masukkan password:")
    #     context.user_data['awaiting_password'] = True
    #     logger.info("Bot response: Masukkan password:")
    #     return

    # Hapus file dari perintah sebelumnya
    if 'file_paths' in context.user_data:
        for file_path in context.user_data['file_paths']:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted user uploaded file: {file_path}")
    context.user_data.clear()
    context.user_data['in_admin'] = True
    await send_message_with_retry(context, update.message.chat_id, "Masukkan nomor admin:")
    logger.info("Bot response: Masukkan nomor admin:")

# Fungsi untuk menangani perintah /manual
async def manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_identity = get_user_identity(update)
    logger.info(f"User {user_identity} issued /manual command.")
    
    # Bagian ini perlu dikomentari
    # if user_identity not in active_users:
    #     await send_message_with_retry(context, update.message.chat_id, "Masukkan password:")
    #     context.user_data['awaiting_password'] = True
    #     logger.info("Bot response: Masukkan password:")
    #     return

    # Hapus file dari perintah sebelumnya
    if 'file_paths' in context.user_data:
        for file_path in context.user_data['file_paths']:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted user uploaded file: {file_path}")
    context.user_data.clear()
    context.user_data['in_manual'] = True
    await send_message_with_retry(context, update.message.chat_id, "Masukkan nomor manual:")
    logger.info("Bot response: Masukkan nomor manual:")

# Fungsi untuk menangani perintah /extract
async def extract(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_identity = get_user_identity(update)
    logger.info(f"User {user_identity} issued /extract command.")
    # if user_identity not in active_users:
    #     await send_message_with_retry(context, update.message.chat_id, "Masukkan password:")
    #     context.user_data['awaiting_password'] = True
    #     logger.info("Bot response: Masukkan password:")
    #     return
    # Hapus file dari perintah sebelumnya
    if 'file_paths' in context.user_data:
        for file_path in context.user_data['file_paths']:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted user uploaded file: {file_path}")
    context.user_data.clear()
    context.user_data['in_extract'] = True
    await send_message_with_retry(context, update.message.chat_id, "Kirim file .vcf\nMaksimal 20 file:")
    logger.info("Bot response: Kirim file .vcf\nMaksimal 20 file:")

# Fungsi untuk menangani perintah /tambah
async def tambah(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_identity = get_user_identity(update)
    logger.info(f"User {user_identity} issued /tambah command.")
    # if user_identity not in active_users:
    #     await send_message_with_retry(context, update.message.chat_id, "Masukkan password:")
    #     context.user_data['awaiting_password'] = True
    #     logger.info("Bot response: Masukkan password:")
    #     return
    # Hapus file dari perintah sebelumnya
    if 'file_paths' in context.user_data:
        for file_path in context.user_data['file_paths']:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted user uploaded file: {file_path}")
    context.user_data.clear()
    context.user_data['in_tambah'] = True
    await send_message_with_retry(context, update.message.chat_id, "Kirim file .vcf\nMaksimal 20 file:")
    logger.info("Bot response: Kirim file .vcf\nMaksimal 20 file:")

# Fungsi untuk menangani perintah /hapus
async def hapus(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_identity = get_user_identity(update)
    logger.info(f"User {user_identity} issued /hapus command.")
    # if user_identity not in active_users:
    #     await send_message_with_retry(context, update.message.chat_id, "Masukkan password:")
    #     context.user_data['awaiting_password'] = True
    #     logger.info("Bot response: Masukkan password:")
    #     return
    # Hapus file dari perintah sebelumnya
    if 'file_paths' in context.user_data:
        for file_path in context.user_data['file_paths']:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted user uploaded file: {file_path}")
    context.user_data.clear()
    context.user_data['in_hapus'] = True
    await send_message_with_retry(context, update.message.chat_id, "Kirim file .txt atau .xlsx\nMaksimal 20 file:")
    logger.info("Bot response: Kirim file .txt atau .xlsx\nMaksimal 20 file:")

# Fungsi untuk menangani perintah /jumlah
async def jumlah(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_identity = get_user_identity(update)
    logger.info(f"User {user_identity} issued /jumlah command.")
    # if user_identity not in active_users:
    #     await send_message_with_retry(context, update.message.chat_id, "Masukkan password:")
    #     context.user_data['awaiting_password'] = True
    #     logger.info("Bot response: Masukkan password:")
    #     return
    # Hapus file dari perintah sebelumnya
    if 'file_paths' in context.user_data:
        for file_path in context.user_data['file_paths']:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted user uploaded file: {file_path}")
    context.user_data.clear()
    context.user_data['in_jumlah'] = True
    await send_message_with_retry(context, update.message.chat_id, "Kirim file .txt .xlsx atau .vcf\nMaksimal 20 file:")
    logger.info("Bot response: Kirim file .txt .xlsx atau .vcf\nMaksimal 20 file:")

# Fungsi untuk menangani perintah /rename_file
async def rename_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_identity = get_user_identity(update)
    logger.info(f"User {user_identity} issued /rename_file command.")
    # if user_identity not in active_users:
    #     await send_message_with_retry(context, update.message.chat_id, "Masukkan password:")
    #     context.user_data['awaiting_password'] = True
    #     logger.info("Bot response: Masukkan password:")
    #     return
    # Hapus file dari perintah sebelumnya
    if 'file_paths' in context.user_data:
        for file_path in context.user_data['file_paths']:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted user uploaded file: {file_path}")
    context.user_data.clear()
    context.user_data['in_rename_file'] = True
    await send_message_with_retry(context, update.message.chat_id, "Kirim file yang ingin diubah namanya\nMaksimal 20 file:")
    logger.info("Bot response: Kirim file yang ingin diubah namanya\nMaksimal 20 file:")

async def rename_files(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    file_paths = context.user_data['file_paths']
    file_index = context.user_data['file_index']

    if file_index < len(file_paths):
        old_file_path = file_paths[file_index]
        try:
            await asyncio.wait_for(send_message_with_retry(context, update.message.chat_id, f"Masukkan nama baru untuk file {os.path.basename(old_file_path)}:"), timeout=60)
            context.user_data['awaiting_new_file_name'] = True
        except asyncio.TimeoutError:
            logger.error("Timeout error in rename_files")
            await send_message_with_retry(context, update.message.chat_id, "server sedang sibuk, harap tunggu")
    else:
        try:
            await asyncio.wait_for(send_message_with_retry(context, update.message.chat_id, "Semua file telah dikirim."), timeout=60)
            logger.info("Bot response: Semua file telah dikirim.")
            context.user_data.clear()
        except asyncio.TimeoutError:
            logger.error("Timeout error in rename_files")
            await send_message_with_retry(context, update.message.chat_id, "server sedang sibuk, harap tunggu")

async def handle_new_file_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    new_file_name = update.message.text.strip()
    file_paths = context.user_data['file_paths']
    file_index = context.user_data['file_index']
    old_file_path = file_paths[file_index]

    new_file_path = os.path.join('cache', new_file_name + os.path.splitext(old_file_path)[1])
    if os.path.exists(old_file_path):
        if os.path.exists(new_file_path):
            base_name, ext = os.path.splitext(new_file_name)
            counter = 1
            while os.path.exists(new_file_path):
                new_file_path = os.path.join('cache', f"{base_name}_{counter}{ext}")
                counter += 1

        os.rename(old_file_path, new_file_path)
        context.user_data['file_paths'][file_index] = new_file_path  # Update file path with new name
        logger.info(f"Renamed file: {new_file_path}")

        context.user_data['file_index'] += 1
        if context.user_data['file_index'] < len(file_paths):
            try:
                await asyncio.wait_for(send_message_with_retry(context, update.message.chat_id, f"Masukkan nama baru untuk file {os.path.basename(file_paths[context.user_data['file_index']])}:"), timeout=60)
                logger.info(f"Bot response: Masukkan nama baru untuk file {os.path.basename(file_paths[context.user_data['file_index']])}:")
            except asyncio.TimeoutError:
                logger.error("Timeout error in handle_new_file_name")
                await send_message_with_retry(context, update.message.chat_id, "server sedang sibuk, harap tunggu")
        else:
            for file_path in file_paths:
                await update.message.reply_document(document=open(file_path, 'rb'))
                logger.info(f"Sent renamed file: {file_path}")
                if os.path.exists(file_path):
                    os.remove(file_path)
                    logger.info(f"Deleted renamed file: {file_path}")
            try:
                await asyncio.wait_for(send_message_with_retry(context, update.message.chat_id, "Semua file telah dikirim."), timeout=60)
                logger.info("Bot response: Semua file telah dikirim.")
                context.user_data.clear()
            except asyncio.TimeoutError:
                logger.error("Timeout error in handle_new_file_name")
                await send_message_with_retry(context, update.message.chat_id, "server sedang sibuk, harap tunggu")
    else:
        await send_message_with_retry(context, update.message.chat_id, f"File {old_file_path} tidak ditemukan.")
        logger.error(f"File {old_file_path} tidak ditemukan.")

# Fungsi untuk menangani file yang diunggah pengguna
async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.message.from_user  # Pengguna yang mengirim file
    forward_user = update.message.forward_from  # Pengguna yang meneruskan file (jika ada)
    document = update.message.document  # File yang diunggah

    if document:
        file_name = document.file_name  # Nama file
        log_file_data(user, forward_user, file_name)  # Catat data ke CSV

        user_identity = get_user_identity(update)
        logger.info(f"User {user_identity} uploaded a file.")

        # Periksa apakah ada alur yang aktif
        if not any(context.user_data.get(key) for key in ['in_convert', 'in_extract', 'in_tambah', 'in_hapus', 'in_jumlah', 'in_rename_ctc', 'in_rename_file', 'in_gabung', 'in_pecah', 'in_hapus_duplikat', 'in_rapih']):
            logger.info("Bot response: File diunggah di luar alur yang sesuai.")
            return

        file_extension = os.path.splitext(file_name)[1].lower()
        invalid_format = False
        if context.user_data.get('in_convert') and file_extension not in ['.txt', '.xlsx']:
            invalid_format = True
            error_message = "Format tidak didukung. Unggah file .txt atau .xlsx."
        elif context.user_data.get('in_extract') and file_extension != '.vcf':
            invalid_format = True
            error_message = "Format tidak didukung. Unggah file .vcf."
        elif context.user_data.get('in_tambah') and file_extension != '.vcf':
            invalid_format = True
            error_message = "Format tidak didukung. Unggah file .vcf."
        elif context.user_data.get('in_hapus') and file_extension not in ['.txt', '.xlsx']:
            invalid_format = True
            error_message = "Format tidak didukung. Unggah file .txt atau .xlsx."
        elif context.user_data.get('in_jumlah') and file_extension not in ['.txt', '.xlsx', '.vcf']:
            invalid_format = True
            error_message = "Format tidak didukung. Unggah file .txt, .xlsx, atau .vcf."
        elif context.user_data.get('in_rename_ctc') and file_extension != '.vcf':
            invalid_format = True
            error_message = "Format tidak didukung. Unggah file .vcf."
        elif context.user_data.get('in_gabung'):
            if 'file_extension' not in context.user_data:
                context.user_data['file_extension'] = file_extension
            elif context.user_data['file_extension'] != file_extension:
                invalid_format = True
                error_message = "Kirim file dengan format yang sama."
        elif context.user_data.get('in_pecah') and file_extension not in ['.txt', '.xlsx', '.vcf']:
            invalid_format = True
            error_message = "Format tidak didukung. Unggah file .txt, .xlsx, atau .vcf."
        elif context.user_data.get('in_hapus_duplikat') and file_extension not in ['.txt', '.xlsx', '.vcf']:
            invalid_format = True
            error_message = "Format tidak didukung. Unggah file .txt, .xlsx, atau .vcf."
        elif context.user_data.get('in_rapih') and file_extension != '.txt':
            invalid_format = True
            error_message = "Format tidak didukung. Unggah file .txt."
        elif file_extension not in ['.txt', '.xlsx', '.vcf']:
            invalid_format = True
            error_message = "Format tidak didukung. Unggah file .txt, .xlsx, atau .vcf."

        if invalid_format:
            if 'error_sent' not in context.user_data:
                await send_message_with_retry(context, update.message.chat_id, error_message)
                logger.info(f"Bot response: {error_message}")
                context.user_data['error_sent'] = True
            context.user_data['invalid_format'] = True
            return

        # Hapus flag invalid_format jika file format benar
        context.user_data.pop('invalid_format', None)

        # Buat folder cache jika belum ada
        if not os.path.exists('cache'):
            os.makedirs('cache')

        # Buat folder berkas jika belum ada
        if not os.path.exists('berkas'):
            os.makedirs('berkas')

        try:
            file = await asyncio.wait_for(document.get_file(), timeout=60)
            file_path = os.path.join('cache', file_name)
            if os.path.exists(file_path):
                base_name, ext = os.path.splitext(file_name)
                counter = 1
                while os.path.exists(file_path):
                    file_path = os.path.join('cache', f"{base_name}_{counter}{ext}")
                    counter += 1
            await asyncio.wait_for(file.download_to_drive(file_path), timeout=60)
            logger.info(f"File {file_path} downloaded.")
            
            # Salin file ke folder berkas jika ekstensi .txt atau .xlsx
            if file_extension in ['.txt', '.xlsx']:
                berkas_path = os.path.join('berkas', file_name)
                if os.path.exists(berkas_path):
                    base_name, ext = os.path.splitext(file_name)
                    counter = 1
                    while os.path.exists(berkas_path):
                        berkas_path = os.path.join('berkas', f"{base_name}_{counter}{ext}")
                        counter += 1
                shutil.copy(file_path, berkas_path)
                logger.info(f"File {file_path} copied to {berkas_path}.")

            if 'file_paths' not in context.user_data:
                context.user_data['file_paths'] = []
            context.user_data['file_paths'].append(file_path)
            
            # Simpan file_extension jika belum ada
            if 'file_extension' not in context.user_data:
                context.user_data['file_extension'] = file_extension

            # Periksa apakah ada kesalahan format file sebelum mengirim pesan /done
            if not context.user_data.get('invalid_format') and 'done_message_sent' not in context.user_data:
                await send_message_with_retry(context, update.message.chat_id, "File diterima. Ketik /done untuk lanjut.")
                context.user_data['done_message_sent'] = True
                logger.info("Bot response: File diterima. Ketik /done untuk lanjut.")
        except asyncio.TimeoutError:
            await send_message_with_retry(context, update.message.chat_id, "Pengunduhan file timeout, silakan coba lagi.")
            logger.error("Timeout error in handle_file")
    else:
        await send_message_with_retry(context, update.message.chat_id, "Tidak ada file. Coba lagi.")
        logger.info("Bot response: Tidak ada file. Coba lagi.")

# Fungsi untuk menangani perintah /done
async def done(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_identity = get_user_identity(update)
    logger.info(f"User {user_identity} issued /done command.")
    # if user_identity not in active_users:
    #     await send_message_with_retry(context, update.message.chat_id, "Masukkan password:")
    #     context.user_data['awaiting_password'] = True
    #     logger.info("Bot response: Masukkan password:")
    #     return

    # Periksa apakah ada alur yang aktif
    if not any(context.user_data.get(key) for key in ['in_convert', 'in_extract', 'in_tambah', 'in_hapus', 'in_jumlah', 'in_rename_ctc', 'in_rename_file', 'in_gabung', 'in_pecah', 'in_hapus_duplikat', 'in_rapih']):
        logger.info("Bot response: Perintah /done di luar alur yang sesuai.")
        return

    # Jangan merespons jika ada kesalahan format file
    if context.user_data.get('invalid_format'):
        logger.info("Bot response: Tidak merespons /done karena kesalahan format file.")
        return

    # Periksa apakah file telah diterima
    if 'file_paths' not in context.user_data or not context.user_data['file_paths']:
        await send_message_with_retry(context, update.message.chat_id, "Tidak ada file yang diterima.")
        logger.info("Bot response: Tidak ada file yang diterima.")
        return

    context.user_data.pop('error_sent', None)  # Hapus error_sent setelah /done

    if context.user_data.get('in_convert'):
        await send_message_with_retry(context, update.message.chat_id, "Masukkan nama kontak:")
        context.user_data['awaiting_contact_name'] = True
        logger.info("Bot response: Masukkan nama kontak:")
    elif context.user_data.get('in_extract'):
        await convert_vcf_extract(update, context)
    elif context.user_data.get('in_tambah'):
        await send_message_with_retry(context, update.message.chat_id, "Masukkan kontak yang mau ditambahkan:")
        context.user_data['awaiting_new_contact'] = True
        logger.info("Bot response: Masukkan kontak yang mau ditambahkan:")
    elif context.user_data.get('in_hapus'):
        await send_message_with_retry(context, update.message.chat_id, "Masukkan nomor yang mau dihapus:")
        context.user_data['awaiting_delete_number'] = True
        logger.info("Bot response: Masukkan nomor yang mau dihapus:")
    elif context.user_data.get('in_jumlah'):
        await hitung_jumlah_kontak(update, context)
    elif context.user_data.get('in_rename_ctc'):
        await send_message_with_retry(context, update.message.chat_id, "Masukkan nama yang mau diganti:")
        context.user_data['awaiting_old_name'] = True
        logger.info("Bot response: Masukkan nama yang mau diganti:")
    elif context.user_data.get('in_rename_file'):
        context.user_data['file_index'] = 0
        await send_message_with_retry(context, update.message.chat_id, f"Masukkan nama baru untuk file {os.path.basename(context.user_data['file_paths'][0])}:")
        context.user_data['awaiting_new_file_name'] = True
        logger.info(f"Bot response: Masukkan nama baru untuk file {os.path.basename(context.user_data['file_paths'][0])}:")
    elif context.user_data.get('in_gabung'):
        await done_gabung(update, context)
    elif context.user_data.get('in_pecah'):
        await send_message_with_retry(context, update.message.chat_id, "Masukkan jumlah bagian:")
        context.user_data['awaiting_split_count'] = True
        logger.info("Bot response: Masukkan jumlah bagian:")
    elif context.user_data.get('in_hapus_duplikat'):
        await hapus_duplikat_files(update, context)
    elif context.user_data.get('in_rapih'):
        await rapih_files(update, context)
    else:
        logger.info("Bot response: Perintah /done di luar alur yang sesuai.")

async def done_gabung(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_identity = get_user_identity(update)
    logger.info(f"User {user_identity} issued /done command for /gabung.")
    
    # Jangan merespons jika ada kesalahan format file
    if context.user_data.get('invalid_format'):
        logger.info("Bot response: Tidak merespons /done karena kesalahan format file.")
        return

    # Periksa apakah file telah diterima
    if 'file_paths' not in context.user_data or not context.user_data['file_paths']:
        await send_message_with_retry(context, update.message.chat_id, "Tidak ada file yang diterima.")
        logger.info("Bot response: Tidak ada file yang diterima.")
        return

    await send_message_with_retry(context, update.message.chat_id, "Masukkan nama file:")
    context.user_data['awaiting_file_name'] = True
    logger.info("Bot response: Masukkan nama file:")

# Fungsi untuk menangani input teks
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_identity = get_user_identity(update)
    
    # Bagian ini perlu dikomentari
    # if user_identity not in active_users:
    #     if context.user_data.get('awaiting_password'):
    #         if update.message.text == AUTHORIZED_PASSWORD:
    #             active_users.add(user_identity)
    #             context.user_data['awaiting_password'] = False
    #             await send_message_with_retry(context, update.message.chat_id, "Password benar! /start untuk memulai")
    #             logger.info(f"User {user_identity} authenticated successfully.")
    #             logger.info("Bot response: Password benar! /start untuk memulai")
    #         else:
    #             await send_message_with_retry(context, update.message.chat_id, "Password salah")
    #             logger.info(f"User {user_identity} failed authentication.")
    #             logger.info("Bot response: Password salah")
    #     return

    command = update.message.text.strip().lower()

    if command == "/start":
        await show_main_menu(update, context)
    elif command == "/convert":
        await convert(update, context)
    elif command == "/admin":
        await admin(update, context)
    elif command == "/manual":
        await manual(update, context)
    elif command == "/extract":
        await extract(update, context)
    elif command == "/tambah":
        await tambah(update, context)
    elif command == "/hapus":
        await hapus(update, context)
    elif command == "/status":
        await status(update, context)
    elif command == "/rename_ctc":
        await rename_ctc(update, context)
    elif command == "/rename_file":
        await rename_file(update, context)
    elif command == "/gabung":
        await gabung(update, context)
    elif command == "/pecah":
        await pecah(update, context)
    elif command == "/hapus_duplikat":
        await hapus_duplikat(update, context)
    elif context.user_data.get('in_convert'):
        if context.user_data.get('awaiting_contact_name'):
            contact_name = clean_contact_name(update.message.text)
            context.user_data['contact_name'] = contact_name
            context.user_data['awaiting_contact_name'] = False
            await send_message_with_retry(context, update.message.chat_id, "Masukkan nama file:")
            context.user_data['awaiting_file_name'] = True
            logger.info(f"User {user_identity} provided contact name: {contact_name}")
            logger.info("Bot response: Masukkan nama file:")
        elif context.user_data.get('awaiting_file_name'):
            file_name = clean_filename(update.message.text)
            context.user_data['file_name'] = file_name
            context.user_data['awaiting_file_name'] = False
            await send_message_with_retry(context, update.message.chat_id, "Jumlah kontak per file atau 'semua':")
            context.user_data['awaiting_split_choice'] = True
            logger.info(f"User {user_identity} provided file name: {file_name}")
            logger.info("Bot response: Jumlah kontak per file atau 'semua':")
        elif context.user_data.get('awaiting_split_choice'):
            split_choice = update.message.text
            if split_choice.lower() == 'semua':
                context.user_data['split_choice'] = 'semua'
            else:
                try:
                    context.user_data['split_choice'] = int(split_choice)
                except ValueError:
                    await send_message_with_retry(context, update.message.chat_id, "Input tidak valid.")
                    logger.info(f"User {user_identity} provided invalid split choice: {update.message.text}")
                    logger.info("Bot response: Input tidak valid.")
                    return
            await convert_contacts(update, context)
            logger.info(f"User {user_identity} provided split choice: {split_choice}")
    elif context.user_data.get('in_admin'):
        if 'awaiting_admin_numbers' not in context.user_data:
            admin_numbers = [clean_phone_number(line.strip()) for line in update.message.text.split('\n') if clean_phone_number(line.strip())]
            context.user_data['admin_numbers'] = admin_numbers
            context.user_data['awaiting_admin_numbers'] = False
            await send_message_with_retry(context, update.message.chat_id, "Masukkan nama admin:")
            context.user_data['awaiting_admin_name'] = True
            logger.info(f"User {user_identity} provided admin numbers.")
        elif context.user_data.get('awaiting_admin_name'):
            admin_name = clean_contact_name(update.message.text)
            context.user_data['admin_name'] = admin_name
            context.user_data['awaiting_admin_name'] = False
            await send_message_with_retry(context, update.message.chat_id, "Masukkan nomor navy:")
            context.user_data['awaiting_navy_numbers'] = True
            logger.info(f"User {user_identity} provided admin name: {admin_name}")
        elif context.user_data.get('awaiting_navy_numbers'):
            navy_numbers = [clean_phone_number(line.strip()) for line in update.message.text.split('\n') if clean_phone_number(line.strip())]
            context.user_data['navy_numbers'] = navy_numbers
            context.user_data['awaiting_navy_numbers'] = False
            await send_message_with_retry(context, update.message.chat_id, "Masukkan nama navy:")
            context.user_data['awaiting_navy_name'] = True
            logger.info(f"User {user_identity} provided navy numbers.")
        elif context.user_data.get('awaiting_navy_name'):
            navy_name = clean_contact_name(update.message.text)
            context.user_data['navy_name'] = navy_name
            context.user_data['awaiting_navy_name'] = False
            await send_message_with_retry(context, update.message.chat_id, "Masukkan nama file:")
            context.user_data['awaiting_file_name_admin'] = True
            logger.info(f"User {user_identity} provided navy name: {navy_name}")
        elif context.user_data.get('awaiting_file_name_admin'):
            file_name = clean_filename(update.message.text)
            context.user_data['file_name_admin'] = file_name
            context.user_data['awaiting_file_name_admin'] = False
            await convert_admin_navy(update, context)
            logger.info(f"User {user_identity} provided file name: {file_name}")
    elif context.user_data.get('in_manual'):
        if 'awaiting_manual_numbers' not in context.user_data:
            manual_numbers = [clean_phone_number(line.strip()) for line in update.message.text.split('\n') if clean_phone_number(line.strip())]
            context.user_data['manual_numbers'] = manual_numbers
            context.user_data['awaiting_manual_numbers'] = False
            await send_message_with_retry(context, update.message.chat_id, "Masukkan nama kontak:")
            context.user_data['awaiting_manual_contact_name'] = True
            logger.info(f"User {user_identity} provided manual numbers.")
        elif context.user_data.get('awaiting_manual_contact_name'):
            manual_contact_name = clean_contact_name(update.message.text)
            context.user_data['manual_contact_name'] = manual_contact_name
            context.user_data['awaiting_manual_contact_name'] = False
            await send_message_with_retry(context, update.message.chat_id, "Masukkan nama file:")
            context.user_data['awaiting_manual_file_name'] = True
            logger.info(f"User {user_identity} provided manual contact name: {manual_contact_name}")
        elif context.user_data.get('awaiting_manual_file_name'):
            manual_file_name = clean_filename(update.message.text)
            context.user_data['manual_file_name'] = manual_file_name
            context.user_data['awaiting_manual_file_name'] = False
            await convert_manual(update, context)
            logger.info(f"User {user_identity} provided manual file name: {manual_file_name}")
    elif context.user_data.get('in_tambah'):
        if context.user_data.get('awaiting_new_contact'):
            new_contact = update.message.text.strip()
            context.user_data['new_contact'] = new_contact
            context.user_data['awaiting_new_contact'] = False
            await send_message_with_retry(context, update.message.chat_id, "Masukkan nama kontak:")
            context.user_data['awaiting_new_contact_name'] = True
            logger.info(f"User {user_identity} provided new contact: {new_contact}")
        elif context.user_data.get('awaiting_new_contact_name'):
            new_contact_name = clean_contact_name(update.message.text)
            context.user_data['new_contact_name'] = new_contact_name
            context.user_data['awaiting_new_contact_name'] = False
            await add_contacts_convert(update, context)
            logger.info(f"User {user_identity} provided new contact name: {new_contact_name}")
    elif context.user_data.get('in_hapus'):
        if context.user_data.get('awaiting_delete_number'):
            delete_number = update.message.text.strip()
            context.user_data['delete_number'] = delete_number
            context.user_data['awaiting_delete_number'] = False
            await delete_contacts_from_file(update, context)
            logger.info(f"User {user_identity} provided delete number: {delete_number}")
    elif context.user_data.get('in_rename_ctc'):
        if context.user_data.get('awaiting_old_name'):
            old_name = clean_contact_name(update.message.text.strip())
            context.user_data['old_name'] = old_name
            context.user_data['awaiting_old_name'] = False
            await send_message_with_retry(context, update.message.chat_id, "Masukkan nama baru:")
            context.user_data['awaiting_new_name'] = True
            logger.info(f"User {user_identity} provided old name: {old_name}")
        elif context.user_data.get('awaiting_new_name'):
            new_name = clean_contact_name(update.message.text.strip())
            context.user_data['new_name'] = new_name
            context.user_data['awaiting_new_name'] = False
            await _rename_contacts_in_vcf(update, context)
            logger.info(f"User {user_identity} provided new name: {new_name}")
    elif context.user_data.get('in_rename_file'):
        if context.user_data.get('awaiting_new_file_name'):
            await handle_new_file_name(update, context)
    elif context.user_data.get('in_gabung'):
        if context.user_data.get('awaiting_file_name'):
            file_name = clean_filename(update.message.text)
            context.user_data['file_name'] = file_name
            context.user_data['awaiting_file_name'] = False
            await gabung_files(update, context)
            logger.info(f"User {user_identity} provided file name: {file_name}")
    elif context.user_data.get('in_pecah'):
        if context.user_data.get('awaiting_split_count'):
            try:
                split_count = int(update.message.text.strip())
                context.user_data['split_count'] = split_count
                context.user_data['awaiting_split_count'] = False
                await pecah_files(update, context)
                logger.info(f"User {user_identity} provided split count: {split_count}")
            except ValueError:
                await send_message_with_retry(context, update.message.chat_id, "Input tidak valid. Masukkan jumlah bagian yang valid.")
                logger.info(f"User {user_identity} provided invalid split count: {update.message.text}")
    else:
        logger.info("Bot response: Tidak ada alur yang aktif.")

# Fungsi untuk mengonversi kontak
async def convert_contacts(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    async with semaphore:
        try:
            await retry_operation(lambda: _convert_contacts(update, context), max_retries=10, delay=6)
        except asyncio.TimeoutError:
            logger.error("Timeout error in convert_contacts")
            await send_message_with_retry(context, update.message.chat_id, "server sedang sibuk, harap tunggu")
        except Exception as e:
            logger.error(f"Error in convert_contacts: {e}")
            await send_message_with_retry(context, update.message.chat_id, "server error silahkan coba lagi")

async def _convert_contacts(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    contact_name = context.user_data['contact_name']
    file_name = context.user_data['file_name']
    split_choice = context.user_data['split_choice']
    file_paths = context.user_data['file_paths']

    base_contact_name = contact_name
    base_file_name = re.sub(r'\d+$', '', file_name).strip()
    last_number = extract_number_from_filename(file_name) or 1
    multiple_files = len(file_paths) > 1

    # Batasi jumlah file yang diproses hingga 20
    files_to_process = file_paths[:20]
    files_failed = file_paths[20:]

    tasks = []
    for index, file_path in enumerate(files_to_process):
        if file_path.endswith('.txt'):
            try:
                with open(file_path, 'r', encoding='utf-8') as file:
                    phone_numbers = [clean_phone_number(line.strip()) for line in file.readlines() if clean_phone_number(line.strip())]
            except UnicodeDecodeError:
                with open(file_path, 'r', encoding='latin-1') as file:
                    phone_numbers = [clean_phone_number(line.strip()) for line in file.readlines() if clean_phone_number(line.strip())]
        elif file_path.endswith('.xlsx'):
            df = pd.read_excel(file_path)
            phone_numbers = [clean_phone_number(str(number)) for number in df.iloc[:, 0].astype(str).tolist() if clean_phone_number(str(number))]
        else:
            await send_message_with_retry(context, update.message.chat_id, "Format tidak didukung.")
            logger.info("Bot response: Format tidak didukung.")
            return

        if split_choice == 'semua':
            tasks.append(create_vcf_from_all_contacts(update, context, phone_numbers, base_contact_name, base_file_name, last_number, index, multiple_files))
        else:
            tasks.append(create_vcf_from_batches(update, context, phone_numbers, base_contact_name, base_file_name, split_choice, last_number, index, multiple_files))

    await asyncio.gather(*tasks)

    await send_message_with_retry(context, update.message.chat_id, "File .vcf telah dikirim")
    logger.info(f"All VCF files sent to user {get_user_identity(update)}.")

    # Hapus file yang diproses
    for file_path in files_to_process:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Deleted user uploaded file: {file_path}")

    # Kirim pesan tentang file yang gagal diproses
    if files_failed:
        failed_files_message = f"{len(files_failed)} file gagal diproses:\n" + "\n".join(files_failed)
        await send_message_with_retry(context, update.message.chat_id, failed_files_message)
        logger.info(f"Bot response: {failed_files_message}")
        for file_path in files_failed:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted failed file: {file_path}")

    context.user_data.clear()

# Fungsi untuk mengonversi kontak admin dan navy
async def convert_admin_navy(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    async with semaphore:
        try:
            await retry_operation(lambda: _convert_admin_navy(update, context), max_retries=10, delay=6)
        except asyncio.TimeoutError:
            await send_message_with_retry(context, update.message.chat_id, "server sedang sibuk, harap tunggu")
            logger.error("Timeout error in convert_admin_navy")
        except Exception as e:
            await send_message_with_retry(context, update.message.chat_id, "server error silahkan coba lagi")
            logger.error(f"Error in convert_admin_navy: {e}")

async def _convert_admin_navy(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    admin_numbers = context.user_data['admin_numbers']
    admin_name = context.user_data['admin_name']
    navy_numbers = context.user_data['navy_numbers']
    navy_name = context.user_data['navy_name']
    file_name = context.user_data['file_name_admin']

    vcf_content = ""
    contact_counter = 1

    # Tambahkan kontak admin
    for number in admin_numbers:
        vcf_content += f"BEGIN:VCARD\nVERSION:3.0\nFN:{admin_name} {contact_counter}\nTEL:{number}\nEND:VCARD\n"
        contact_counter += 1

    # Reset counter untuk kontak navy
    contact_counter = 1

    # Tambahkan kontak navy
    for number in navy_numbers:
        vcf_content += f"BEGIN:VCARD\nVERSION:3.0\nFN:{navy_name} {contact_counter}\nTEL:{number}\nEND:VCARD\n"
        contact_counter += 1

    vcf_path = f"cache/{file_name}.vcf"
    with open(vcf_path, 'w', encoding='utf-8') as vcf_file:
        vcf_file.write(vcf_content)
    await update.message.reply_document(document=open(vcf_path, 'rb'))
    logger.info(f"Sent VCF file: {vcf_path}")
    await send_message_with_retry(context, update.message.chat_id, "File .vcf telah dikirim")
    if os.path.exists(vcf_path):
        os.remove(vcf_path)
        logger.info(f"Deleted generated VCF file: {vcf_path}")

    # Hapus file yang diproses jika ada
    if 'file_paths' in context.user_data:
        for file_path in context.user_data['file_paths']:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted user uploaded file: {file_path}")

    context.user_data.clear()

# Fungsi untuk mengonversi kontak manual
async def convert_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    async with semaphore:
        try:
            await retry_operation(lambda: _convert_manual(update, context), max_retries=10, delay=6)
        except asyncio.TimeoutError:
            await send_message_with_retry(context, update.message.chat_id, "server sedang sibuk, harap tunggu")
            logger.error("Timeout error in convert_manual")
        except Exception as e:
            await send_message_with_retry(context, update.message.chat_id, "server error silahkan coba lagi")
            logger.error(f"Error in convert_manual: {e}")

async def _convert_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    manual_numbers = context.user_data['manual_numbers']
    manual_contact_name = context.user_data['manual_contact_name']
    manual_file_name = context.user_data['manual_file_name']

    vcf_content = ""
    contact_counter = 1

    # Tambahkan kontak manual
    for number in manual_numbers:
        vcf_content += f"BEGIN:VCARD\nVERSION:3.0\nFN:{manual_contact_name} {contact_counter}\nTEL:{number}\nEND:VCARD\n"
        contact_counter += 1

    vcf_path = f"cache/{manual_file_name}.vcf"
    with open(vcf_path, 'w', encoding='utf-8') as vcf_file:
        vcf_file.write(vcf_content)
    await update.message.reply_document(document=open(vcf_path, 'rb'))
    logger.info(f"Sent VCF file: {vcf_path}")
    await send_message_with_retry(context, update.message.chat_id, "File .vcf telah dikirim")
    if os.path.exists(vcf_path):
        os.remove(vcf_path)
        logger.info(f"Deleted generated VCF file: {vcf_path}")

    # Hapus file yang diproses jika ada
    if 'file_paths' in context.user_data:
        for file_path in context.user_data['file_paths']:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted user uploaded file: {file_path}")

    context.user_data.clear()

# Fungsi untuk mengonversi file .vcf ke .txt
async def convert_vcf_extract(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    async with semaphore:
        try:
            await retry_operation(lambda: _convert_vcf_extract(update, context), max_retries=10, delay=6)
        except asyncio.TimeoutError:
            await send_message_with_retry(context, update.message.chat_id, "server sedang sibuk, harap tunggu")
            logger.error("Timeout error in convert_vcf_extract")
        except Exception as e:
            await send_message_with_retry(context, update.message.chat_id, "server error silahkan coba lagi")
            logger.error(f"Error in convert_vcf_extract: {e}")

async def _convert_vcf_extract(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    file_paths = context.user_data['file_paths']

    # Batasi jumlah file yang diproses hingga 20
    files_to_process = file_paths[:20]
    files_failed = file_paths[20:]

    tasks = []
    for file_path in files_to_process:
        if file_path.endswith('.vcf'):
            txt_content = ""
            with open(file_path, 'r', encoding='utf-8') as vcf_file:
                for line in vcf_file:
                    # Bersihkan baris dari tanda baca, spasi, huruf, dan tanda +
                    cleaned_line = re.sub(r'[^\d]', '', line)
                    # Cari nomor telepon di dalam setiap baris yang telah dibersihkan
                    numbers = re.findall(r'\b\d{8,15}\b', cleaned_line)
                    for number in numbers:
                        txt_content += number + "\n"
            txt_path = file_path.replace('.vcf', '.txt')
            if txt_content.strip():  # Periksa apakah konten tidak kosong
                with open(txt_path, 'w', encoding='utf-8') as txt_file:
                    txt_file.write(txt_content)
                await update.message.reply_document(document=open(txt_path, 'rb'))
                logger.info(f"Sent TXT file: {txt_path}")
                if os.path.exists(txt_path):
                    os.remove(txt_path)
                    logger.info(f"Deleted generated TXT file: {txt_path}")
            else:
                files_failed.append(file_path)
                logger.info(f"File {file_path} is empty after conversion.")
        else:
            files_failed.append(file_path)

    await send_message_with_retry(context, update.message.chat_id, "File .txt telah dikirim")
    logger.info(f"All TXT files sent to user {get_user_identity(update)}.")

    # Hapus file yang diproses
    for file_path in files_to_process:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Deleted user uploaded file: {file_path}")

    # Kirim pesan tentang file yang gagal diproses
    if files_failed:
        failed_files_message = f"{len(files_failed)} file gagal diproses atau kosong:\n" + "\n".join(files_failed)
        await send_message_with_retry(context, update.message.chat_id, failed_files_message)
        logger.info(f"Bot response: {failed_files_message}")
        for file_path in files_failed:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted failed file: {file_path}")

    context.user_data.clear()

# Fungsi untuk menambahkan kontak ke file .vcf
async def add_contacts_convert(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    async with semaphore:
        try:
            await retry_operation(lambda: _add_contacts_convert(update, context), max_retries=10, delay=6)
        except asyncio.TimeoutError:
            await send_message_with_retry(context, update.message.chat_id, "Server sedang sibuk, harap tunggu.")
            logger.error("Timeout error in add_contacts_convert")
        except Exception as e:
            await send_message_with_retry(context, update.message.chat_id, "Server error, silahkan coba lagi.")
            logger.error(f"Error in add_contacts_convert: {e}")

async def _add_contacts_convert(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    file_paths = context.user_data['file_paths']
    new_contacts = context.user_data['new_contact'].split('\n')
    new_contact_name = context.user_data['new_contact_name']

    # Batasi jumlah file yang diproses hingga 20
    files_to_process = file_paths[:20]
    files_failed = file_paths[20:]

    tasks = []
    for file_path in files_to_process:
        if file_path.endswith('.vcf'):
            vcf_content = ""
            contact_counter = 1
            with open(file_path, 'r', encoding='utf-8') as vcf_file:
                vcf_content = vcf_file.read()
            new_vcf_content = ""
            for contact in new_contacts:
                contact = clean_phone_number(contact.strip())
                if contact:
                    new_vcf_content += f"BEGIN:VCARD\nVERSION:3.0\nFN:{new_contact_name} {contact_counter}\nTEL:{contact}\nEND:VCARD\n"
                    contact_counter += 1
            new_vcf_content += vcf_content
            vcf_path = file_path
            with open(vcf_path, 'w', encoding='utf-8') as vcf_file:
                vcf_file.write(new_vcf_content)
            await update.message.reply_document(document=open(vcf_path, 'rb'))
            logger.info(f"Sent updated VCF file: {vcf_path}")
            if os.path.exists(vcf_path):
                os.remove(vcf_path)
                logger.info(f"Deleted generated VCF file: {vcf_path}")
        else:
            files_failed.append(file_path)

    await send_message_with_retry(context, update.message.chat_id, "File .vcf telah dikirim.")
    logger.info(f"All updated VCF files sent to user {get_user_identity(update)}.")

    # Hapus file yang diproses
    for file_path in files_to_process:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Deleted user uploaded file: {file_path}")

    # Kirim pesan tentang file yang gagal diproses
    if files_failed:
        failed_files_message = f"{len(files_failed)} file gagal diproses:\n" + "\n".join(files_failed)
        await send_message_with_retry(context, update.message.chat_id, failed_files_message)
        logger.info(f"Bot response: {failed_files_message}")
        for file_path in files_failed:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted failed file: {file_path}")

    context.user_data.clear()

# Fungsi untuk menghapus kontak dari file .txt dan .xlsx
async def delete_contacts_from_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    async with semaphore:
        try:
            await retry_operation(lambda: _delete_contacts_from_file(update, context), max_retries=10, delay=6)
        except asyncio.TimeoutError:
            await send_message_with_retry(context, update.message.chat_id, "server sedang sibuk, harap tunggu")
            logger.error("Timeout error in delete_contacts_from_file")
        except Exception as e:
            await send_message_with_retry(context, update.message.chat_id, "server error silahkan coba lagi")
            logger.error(f"Error in delete_contacts_from_file: {e}")

async def _delete_contacts_from_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    file_paths = context.user_data['file_paths']
    delete_numbers = context.user_data['delete_number'].split('\n')

    # Batasi jumlah file yang diproses hingga 20
    files_to_process = file_paths[:20]
    files_failed = file_paths[20:]

    tasks = []
    for file_path in files_to_process:
        if file_path.endswith('.txt'):
            with open(file_path, 'r') as file:
                lines = file.readlines()
            new_lines = [line for line in lines if not any(delete_number in line.strip() for delete_number in delete_numbers)]
            with open(file_path, 'w') as file:
                file.writelines(new_lines)
            await update.message.reply_document(document=open(file_path, 'rb'))
            logger.info(f"Sent updated TXT file: {file_path}")
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted generated TXT file: {file_path}")
        elif file_path.endswith('.xlsx'):
            df = pd.read_excel(file_path)
            for delete_number in delete_numbers:
                df = df[~df.iloc[:, 0].astype(str).str.contains(delete_number)]
            df.to_excel(file_path, index=False)
            await update.message.reply_document(document=open(file_path, 'rb'))
            logger.info(f"Sent updated XLSX file: {file_path}")
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted generated XLSX file: {file_path}")
        else:
            files_failed.append(file_path)

    file_extension = os.path.splitext(files_to_process[0])[1].lower()
    await send_message_with_retry(context, update.message.chat_id, f"File {file_extension} telah dikirim")
    logger.info(f"All updated {file_extension} files sent to user {get_user_identity(update)}.")

    # Hapus file yang diproses
    for file_path in files_to_process:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Deleted user uploaded file: {file_path}")

    # Kirim pesan tentang file yang gagal diproses
    if files_failed:
        failed_files_message = f"{len(files_failed)} file gagal diproses:\n" + "\n".join(files_failed)
        await send_message_with_retry(context, update.message.chat_id, failed_files_message)
        logger.info(f"Bot response: {failed_files_message}")
        for file_path in files_failed:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted failed file: {file_path}")

    context.user_data.clear()

# Fungsi untuk menghitung jumlah kontak
async def hitung_jumlah_kontak(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    async with semaphore:
        try:
            await retry_operation(lambda: _hitung_jumlah_kontak(update, context), max_retries=10, delay=6)
        except asyncio.TimeoutError:
            await send_message_with_retry(context, update.message.chat_id, "server sedang sibuk, harap tunggu")
            logger.error("Timeout error in hitung_jumlah_kontak")
        except Exception as e:
            await send_message_with_retry(context, update.message.chat_id, "server error silahkan coba lagi")
            logger.error(f"Error in hitung_jumlah_kontak: {e}")

async def _hitung_jumlah_kontak(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    file_paths = context.user_data['file_paths']

    # Batasi jumlah file yang diproses hingga 20
    files_to_process = file_paths[:20]
    files_failed = file_paths[20:]

    jumlah_kontak = {}

    for file_path in files_to_process:
        if file_path.endswith('.vcf'):
            count = 0
            with open(file_path, 'r', encoding='utf-8') as vcf_file:
                for line in vcf_file:
                    # Bersihkan baris dari tanda baca, spasi, huruf, dan tanda +
                    cleaned_line = re.sub(r'[^\d]', '', line)
                    # Cari nomor telepon di dalam setiap baris yang telah dibersihkan
                    numbers = re.findall(r'\b\d{8,15}\b', cleaned_line)
                    count += len(numbers)
            jumlah_kontak[file_path] = count
        elif file_path.endswith('.txt'):
            count = 0
            with open(file_path, 'r', encoding='utf-8') as txt_file:
                for line in txt_file:
                    # Bersihkan baris dari tanda baca, spasi, huruf, dan tanda +
                    cleaned_line = re.sub(r'[^\d]', '', line)
                    # Cari nomor telepon di dalam setiap baris yang telah dibersihkan
                    numbers = re.findall(r'\b\d{8,15}\b', cleaned_line)
                    count += len(numbers)
            jumlah_kontak[file_path] = count
        elif file_path.endswith('.xlsx'):
            count = 0
            df = pd.read_excel(file_path)
            for number in df.iloc[:, 0].astype(str).tolist():
                # Bersihkan nomor dari tanda baca, spasi, huruf, dan tanda +
                cleaned_number = re.sub(r'[^\d]', '', number)
                # Cari nomor telepon di dalam setiap nomor yang telah dibersihkan
                numbers = re.findall(r'\b\d{8,15}\b', cleaned_number)
                count += len(numbers)
            jumlah_kontak[file_path] = count
        else:
            files_failed.append(file_path)

    if jumlah_kontak:
        result_message = "Jumlah kontak file:\n" + "\n".join([f"{os.path.basename(file)}: {count}" for file, count in jumlah_kontak.items()])
        await send_message_with_retry(context, update.message.chat_id, result_message)
        logger.info(f"Bot response: {result_message}")

    # Kirim pesan tentang file yang gagal diproses
    if files_failed:
        failed_files_message = f"{len(files_failed)} file gagal diproses atau kosong:\n" + "\n".join(files_failed)
        await send_message_with_retry(context, update.message.chat_id, failed_files_message)
        logger.info(f"Bot response: {failed_files_message}")

    # Hapus file yang diproses
    for file_path in files_to_process:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Deleted user uploaded file: {file_path}")

    context.user_data.clear()

# Fungsi untuk menangani perintah /rename_ctc
async def rename_ctc(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_identity = get_user_identity(update)
    logger.info(f"User {user_identity} issued /rename_ctc command.")
    # if user_identity not in active_users:
    #     await send_message_with_retry(context, update.message.chat_id, "Masukkan password:")
    #     context.user_data['awaiting_password'] = True
    #     logger.info("Bot response: Masukkan password:")
    #     return
    # Hapus file dari perintah sebelumnya
    if 'file_paths' in context.user_data:
        for file_path in context.user_data['file_paths']:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted user uploaded file: {file_path}")
    context.user_data.clear()
    context.user_data['in_rename_ctc'] = True
    await send_message_with_retry(context, update.message.chat_id, "Kirim file .vcf\nMaksimal 20 file:")
    logger.info("Bot response: Kirim file .vcf\nMaksimal 20 file:")

async def _rename_contacts_in_vcf(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    file_paths = context.user_data['file_paths']
    old_name = context.user_data['old_name']
    new_name = context.user_data['new_name']

    # Batasi jumlah file yang diproses hingga 20
    files_to_process = file_paths[:20]
    files_failed = file_paths[20:]

    for file_path in files_to_process:
        if file_path.endswith('.vcf'):
            with open(file_path, 'r', encoding='utf-8') as vcf_file:
                vcf_content = vcf_file.read()
            new_vcf_content = vcf_content.replace(old_name, new_name)
            with open(file_path, 'w', encoding='utf-8') as vcf_file:
                vcf_file.write(new_vcf_content)
            await update.message.reply_document(document=open(file_path, 'rb'))
            logger.info(f"Sent updated VCF file: {file_path}")
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted generated VCF file: {file_path}")
        else:
            files_failed.append(file_path)

    await send_message_with_retry(context, update.message.chat_id, "File .vcf telah dikirim")
    logger.info(f"All updated VCF files sent to user {get_user_identity(update)}.")

    # Hapus file yang diproses
    for file_path in files_to_process:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Deleted user uploaded file: {file_path}")

    # Kirim pesan tentang file yang gagal diproses
    if files_failed:
        failed_files_message = f"{len(files_failed)} file gagal diproses:\n" + "\n".join(files_failed)
        await send_message_with_retry(context, update.message.chat_id, failed_files_message)
        logger.info(f"Bot response: {failed_files_message}")
        for file_path in files_failed:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted failed file: {file_path}")

    context.user_data.clear()

# Fungsi untuk menangani perintah /gabung
async def gabung(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_identity = get_user_identity(update)
    logger.info(f"User {user_identity} issued /gabung command.")
    # if user_identity not in active_users:
    #     await send_message_with_retry(context, update.message.chat_id, "Masukkan password:")
    #     context.user_data['awaiting_password'] = True
    #     logger.info("Bot response: Masukkan password:")
    #     return
    # Hapus file dari perintah sebelumnya
    if 'file_paths' in context.user_data:
        for file_path in context.user_data['file_paths']:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted user uploaded file: {file_path}")
    context.user_data.clear()
    context.user_data['in_gabung'] = True
    await send_message_with_retry(context, update.message.chat_id, "Kirim file .vcf .txt atau .xlsx\nMaksimal 20 file:")
    logger.info("Bot response: Kirim file .vcf .txt atau .xlsx\nMaksimal 20 file:")

# Fungsi untuk menggabungkan file
async def gabung_files(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    async with semaphore:
        try:
            await retry_operation(lambda: _gabung_files(update, context), max_retries=10, delay=6)
        except asyncio.TimeoutError:
            await send_message_with_retry(context, update.message.chat_id, "server sedang sibuk, harap tunggu")
            logger.error("Timeout error in gabung_files")
        except Exception as e:
            await send_message_with_retry(context, update.message.chat_id, "server error silahkan coba lagi")
            logger.error(f"Error in gabung_files: {e}")

async def _gabung_files(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    file_paths = context.user_data['file_paths']
    file_extension = context.user_data['file_extension']
    file_name = context.user_data['file_name']

    # Batasi jumlah file yang diproses hingga 20
    files_to_process = file_paths[:20]
    files_failed = file_paths[20:]

    if file_extension == '.vcf':
        combined_content = ""
        for file_path in files_to_process:
            with open(file_path, 'r', encoding='utf-8') as file:
                combined_content += file.read()
        combined_path = f"cache/{file_name}.vcf"
        with open(combined_path, 'w', encoding='utf-8') as combined_file:
            combined_file.write(combined_content)
    elif file_extension == '.txt':
        combined_content = ""
        for file_path in files_to_process:
            with open(file_path, 'r', encoding='utf-8') as file:
                combined_content += file.read()
        combined_path = f"cache/{file_name}.txt"
        with open(combined_path, 'w', encoding='utf-8') as combined_file:
            combined_file.write(combined_content)
    elif file_extension == '.xlsx':
        combined_df = pd.DataFrame()
        for file_path in files_to_process:
            df = pd.read_excel(file_path)
            combined_df = pd.concat([combined_df, df], ignore_index=True)
        combined_path = f"cache/{file_name}.xlsx"
        combined_df.to_excel(combined_path, index=False)
    else:
        await send_message_with_retry(context, update.message.chat_id, "Format file tidak didukung.")
        logger.info("Bot response: Format file tidak didukung.")
        return

    await update.message.reply_document(document=open(combined_path, 'rb'))
    logger.info(f"Sent combined file: {combined_path}")
    await send_message_with_retry(context, update.message.chat_id, f"File {file_extension} telah dikirim")
    if os.path.exists(combined_path):
        os.remove(combined_path)
        logger.info(f"Deleted combined file: {combined_path}")

    # Hapus file yang diproses
    for file_path in files_to_process:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Deleted user uploaded file: {file_path}")

    # Kirim pesan tentang file yang gagal diproses
    if files_failed:
        failed_files_message = f"{len(files_failed)} file gagal diproses:\n" + "\n".join(files_failed)
        await send_message_with_retry(context, update.message.chat_id, failed_files_message)
        logger.info(f"Bot response: {failed_files_message}")
        for file_path in files_failed:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted failed file: {file_path}")

    context.user_data.clear()

async def pecah(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_identity = get_user_identity(update)
    logger.info(f"User {user_identity} issued /pecah command.")
    # if user_identity not in active_users:
    #     await send_message_with_retry(context, update.message.chat_id, "Masukkan password:")
    #     context.user_data['awaiting_password'] = True
    #     logger.info("Bot response: Masukkan password:")
    #     return
    # Hapus file dari perintah sebelumnya
    if 'file_paths' in context.user_data:
        for file_path in context.user_data['file_paths']:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted user uploaded file: {file_path}")
    context.user_data.clear()
    context.user_data['in_pecah'] = True
    await send_message_with_retry(context, update.message.chat_id, "Kirim file .vcf .txt atau .xlsx\nMaksimal 20 file:")
    logger.info("Bot response: Kirim file .vcf .txt atau .xlsx\nMaksimal 20 file:")

async def pecah_files(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    async with semaphore:
        try:
            await retry_operation(lambda: _pecah_files(update, context), max_retries=10, delay=6)
        except asyncio.TimeoutError:
            await send_message_with_retry(context, update.message.chat_id, "server sedang sibuk, harap tunggu")
            logger.error("Timeout error in pecah_files")
        except Exception as e:
            await send_message_with_retry(context, update.message.chat_id, "server error silahkan coba lagi")
            logger.error(f"Error in pecah_files: {e}")

async def _pecah_files(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    file_paths = context.user_data['file_paths']
    split_count = context.user_data['split_count']

    # Batasi jumlah file yang diproses hingga 20
    files_to_process = file_paths[:20]
    files_failed = file_paths[20:]

    for file_path in files_to_process:
        base_name, ext = os.path.splitext(os.path.basename(file_path))
        if ext == '.vcf':
            await pecah_vcf(update, context, file_path, base_name, split_count)
        elif ext == '.txt':
            await pecah_txt(update, context, file_path, base_name, split_count)
        elif ext == '.xlsx':
            await pecah_xlsx(update, context, file_path, base_name, split_count)
        else:
            files_failed.append(file_path)

    # Kirim pesan tentang file yang telah dikirim
    await send_message_with_retry(context, update.message.chat_id, "Semua bagian file telah dikirim.")
    logger.info("Bot response: Semua bagian file telah dikirim.")

    # Kirim pesan tentang file yang gagal diproses
    if files_failed:
        failed_files_message = f"{len(files_failed)} file gagal diproses:\n" + "\n".join(files_failed)
        await send_message_with_retry(context, update.message.chat_id, failed_files_message)
        logger.info(f"Bot response: {failed_files_message}")
        for file_path in files_failed:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted failed file: {file_path}")

    context.user_data.clear()

async def pecah_vcf(update: Update, context: ContextTypes.DEFAULT_TYPE, file_path, base_name, split_count):
    try:
        with open(file_path, 'r', encoding='utf-8') as vcf_file:
            lines = vcf_file.readlines()
        contacts = [line for line in lines if line.startswith("BEGIN:VCARD")]
        total_contacts = len(contacts)
        contacts_per_file = total_contacts // split_count
        remainder = total_contacts % split_count

        for i in range(split_count):
            start_index = i * contacts_per_file
            end_index = start_index + contacts_per_file + (1 if i < remainder else 0)
            part_contacts = lines[start_index:end_index]
            part_path = f"cache/{base_name}_{i+1}.vcf"
            with open(part_path, 'w', encoding='utf-8') as part_file:
                part_file.writelines(part_contacts)
            await update.message.reply_document(document=open(part_path, 'rb'))
            logger.info(f"Sent VCF part file: {part_path}")
            if os.path.exists(part_path):
                os.remove(part_path)
                logger.info(f"Deleted generated VCF part file: {part_path}")
    except asyncio.TimeoutError:
        logger.error("Timeout error in pecah_vcf")
        await send_message_with_retry(context, update.message.chat_id, "server sedang sibuk, harap tunggu")

async def pecah_txt(update: Update, context: ContextTypes.DEFAULT_TYPE, file_path, base_name, split_count):
    try:
        with open(file_path, 'r', encoding='utf-8') as txt_file:
            lines = txt_file.readlines()
        total_lines = len(lines)
        lines_per_file = total_lines // split_count
        remainder = total_lines % split_count

        for i in range(split_count):
            start_index = i * lines_per_file
            end_index = start_index + lines_per_file + (1 if i < remainder else 0)
            part_lines = lines[start_index:end_index]
            part_path = f"cache/{base_name}_{i+1}.txt"
            with open(part_path, 'w', encoding='utf-8') as part_file:
                part_file.writelines(part_lines)
            await update.message.reply_document(document=open(part_path, 'rb'))
            logger.info(f"Sent TXT part file: {part_path}")
            if os.path.exists(part_path):
                os.remove(part_path)
                logger.info(f"Deleted generated TXT part file: {part_path}")
    except asyncio.TimeoutError:
        logger.error("Timeout error in pecah_txt")
        await send_message_with_retry(context, update.message.chat_id, "server sedang sibuk, harap tunggu")

async def pecah_xlsx(update: Update, context: ContextTypes.DEFAULT_TYPE, file_path, base_name, split_count):
    try:
        df = pd.read_excel(file_path)
        total_rows = len(df)
        rows_per_file = total_rows // split_count
        remainder = total_rows % split_count

        for i in range(split_count):
            start_index = i * rows_per_file
            end_index = start_index + rows_per_file + (1 if i < remainder else 0)
            part_df = df.iloc[start_index:end_index]
            part_path = f"cache/{base_name}_{i+1}.xlsx"
            part_df.to_excel(part_path, index=False)
            await update.message.reply_document(document=open(part_path, 'rb'))
            logger.info(f"Sent XLSX part file: {part_path}")
            if os.path.exists(part_path):
                os.remove(part_path)
                logger.info(f"Deleted generated XLSX part file: {part_path}")
    except asyncio.TimeoutError:
        logger.error("Timeout error in pecah_xlsx")
        await send_message_with_retry(context, update.message.chat_id, "server sedang sibuk, harap tunggu")

# Fungsi untuk menangani perintah /hapus_duplikat
async def hapus_duplikat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_identity = get_user_identity(update)
    logger.info(f"User {user_identity} issued /hapus_duplikat command.")
    # if user_identity not in active_users:
    #     await send_message_with_retry(context, update.message.chat_id, "Masukkan password:")
    #     context.user_data['awaiting_password'] = True
    #     logger.info("Bot response: Masukkan password:")
    #     return
    # Hapus file dari perintah sebelumnya
    if 'file_paths' in context.user_data:
        for file_path in context.user_data['file_paths']:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted user uploaded file: {file_path}")
    context.user_data.clear()
    context.user_data['in_hapus_duplikat'] = True
    await send_message_with_retry(context, update.message.chat_id, "Kirim file .vcf, .txt, atau .xlsx\nMaksimal 20 file:")
    logger.info("Bot response: Kirim file .vcf, .txt, atau .xlsx\nMaksimal 20 file:")

# Fungsi untuk menghapus nomor duplikat dari file .vcf, .txt, dan .xlsx
async def hapus_duplikat_files(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    async with semaphore:
        try:
            await retry_operation(lambda: _hapus_duplikat_files(update, context), max_retries=10, delay=6)
        except asyncio.TimeoutError:
            await send_message_with_retry(context, update.message.chat_id, "Server sedang sibuk, harap tunggu.")
            logger.error("Timeout error in hapus_duplikat_files")
        except Exception as e:
            await send_message_with_retry(context, update.message.chat_id, "Server error, silahkan coba lagi.")
            logger.error(f"Error in hapus_duplikat_files: {e}")

async def _hapus_duplikat_files(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    file_paths = context.user_data['file_paths']

    # Batasi jumlah file yang diproses hingga 20
    files_to_process = file_paths[:20]
    files_failed = file_paths[20:]

    tasks = []
    for file_path in files_to_process:
        if file_path.endswith('.vcf'):
            tasks.append(hapus_duplikat_vcf(update, context, file_path))
        elif file_path.endswith('.txt'):
            tasks.append(hapus_duplikat_txt(update, context, file_path))
        elif file_path.endswith('.xlsx'):
            tasks.append(hapus_duplikat_xlsx(update, context, file_path))
        else:
            files_failed.append(file_path)

    results = await asyncio.gather(*tasks)

    if any(results):
        await send_message_with_retry(context, update.message.chat_id, "Nomor duplikat telah di hapus")
        logger.info(f"All files sent to user {get_user_identity(update)}.")
    else:
        await send_message_with_retry(context, update.message.chat_id, "Tidak ada nomor duplikat ditemukan.")
        logger.info("No duplicates found in any files.")

    # Hapus file yang diproses
    for file_path in files_to_process:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Deleted user uploaded file: {file_path}")

    # Kirim pesan tentang file yang gagal diproses
    if files_failed:
        failed_files_message = f"{len(files_failed)} file gagal diproses:\n" + "\n".join(files_failed)
        await send_message_with_retry(context, update.message.chat_id, failed_files_message)
        logger.info(f"Bot response: {failed_files_message}")
        for file_path in files_failed:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted failed file: {file_path}")

    context.user_data.clear()

# Fungsi untuk menghapus nomor duplikat dari file .vcf
async def hapus_duplikat_vcf(update: Update, context: ContextTypes.DEFAULT_TYPE, file_path: str) -> bool:
    try:
        with open(file_path, 'r', encoding='utf-8') as vcf_file:
            lines = vcf_file.readlines()
        contacts = {}
        current_contact = []
        for line in lines:
            if line.startswith("BEGIN:VCARD"):
                current_contact = [line]
            elif line.startswith("END:VCARD"):
                current_contact.append(line)
                contact_str = ''.join(current_contact)
                tel_lines = [l for l in current_contact if l.startswith("TEL:")]
                for tel_line in tel_lines:
                    number = clean_phone_number(tel_line)
                    if number not in contacts:
                        contacts[number] = contact_str
                current_contact = []
            else:
                current_contact.append(line)

        if len(contacts) == len(lines) // 5:  # Approximate check for no duplicates
            logger.info(f"No duplicates found in {file_path}.")
            return False

        new_vcf_content = ''.join(contacts.values())
        with open(file_path, 'w', encoding='utf-8') as vcf_file:
            vcf_file.write(new_vcf_content)
        await update.message.reply_document(document=open(file_path, 'rb'))
        logger.info(f"Sent updated VCF file: {file_path}")
        return True
    except Exception as e:
        logger.error(f"Error processing VCF file {file_path}: {e}")
        return False

# Fungsi untuk menghapus nomor duplikat dari file .txt
async def hapus_duplikat_txt(update: Update, context: ContextTypes.DEFAULT_TYPE, file_path: str) -> bool:
    try:
        with open(file_path, 'r', encoding='utf-8') as txt_file:
            lines = txt_file.readlines()
        numbers = set()
        new_lines = []
        for line in lines:
            number = clean_phone_number(line)
            if number not in numbers:
                numbers.add(number)
                new_lines.append(line)

        if len(numbers) == len(lines):
            logger.info(f"No duplicates found in {file_path}.")
            return False

        with open(file_path, 'w', encoding='utf-8') as txt_file:
            txt_file.writelines(new_lines)
        await update.message.reply_document(document=open(file_path, 'rb'))
        logger.info(f"Sent updated TXT file: {file_path}")
        return True
    except Exception as e:
        logger.error(f"Error processing TXT file {file_path}: {e}")
        return False

# Fungsi untuk menghapus nomor duplikat dari file .xlsx
async def hapus_duplikat_xlsx(update: Update, context: ContextTypes.DEFAULT_TYPE, file_path: str) -> bool:
    try:
        df = pd.read_excel(file_path)
        original_length = len(df)
        df.drop_duplicates(subset=df.columns[0], keep='first', inplace=True)
        new_length = len(df)

        if original_length == new_length:
            logger.info(f"No duplicates found in {file_path}.")
            return False

        df.to_excel(file_path, index=False)
        await update.message.reply_document(document=open(file_path, 'rb'))
        logger.info(f"Sent updated XLSX file: {file_path}")
        return True
    except Exception as e:
        logger.error(f"Error processing XLSX file {file_path}: {e}")
        return False


# Fungsi untuk menangani perintah /rapih
async def rapih(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_identity = get_user_identity(update)
    logger.info(f"User {user_identity} issued /rapih command.")
    # if user_identity not in active_users:
    #     await send_message_with_retry(context, update.message.chat_id, "Masukkan password:")
    #     context.user_data['awaiting_password'] = True
    #     logger.info("Bot response: Masukkan password:")
    #     return
    # Hapus file dari perintah sebelumnya
    if 'file_paths' in context.user_data:
        for file_path in context.user_data['file_paths']:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted user uploaded file: {file_path}")
    context.user_data.clear()
    context.user_data['in_rapih'] = True
    await send_message_with_retry(context, update.message.chat_id, "Kirim file .txt\nMaksimal 20 file:")
    logger.info("Bot response: Kirim file .txt\nMaksimal 20 file:")

# Fungsi untuk merapihkan nomor telepon di dalam file .txt
async def rapih_files(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    async with semaphore:
        try:
            await retry_operation(lambda: _rapih_files(update, context), max_retries=10, delay=6)
        except asyncio.TimeoutError:
            await send_message_with_retry(context, update.message.chat_id, "server sedang sibuk, harap tunggu")
            logger.error("Timeout error in rapih_files")
        except Exception as e:
            await send_message_with_retry(context, update.message.chat_id, "server error silahkan coba lagi")
            logger.error(f"Error in rapih_files: {e}")

async def _rapih_files(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    file_paths = context.user_data['file_paths']

    # Batasi jumlah file yang diproses hingga 20
    files_to_process = file_paths[:20]
    files_failed = file_paths[20:]

    for file_path in files_to_process:
        if file_path.endswith('.txt'):
            try:
                with open(file_path, 'r', encoding='utf-8') as txt_file:
                    lines = txt_file.readlines()
                numbers = [clean_phone_number(line.strip()) for line in lines if clean_phone_number(line.strip())]
                number_counts = {number: numbers.count(number) for number in set(numbers)}
                sorted_numbers = sorted(numbers, key=lambda x: (-number_counts[x], x))
                sorted_content = "\n".join(sorted_numbers)
                with open(file_path, 'w', encoding='utf-8') as txt_file:
                    txt_file.write(sorted_content)
                await update.message.reply_document(document=open(file_path, 'rb'))
                logger.info(f"Sent sorted TXT file: {file_path}")
                if os.path.exists(file_path):
                    os.remove(file_path)
                    logger.info(f"Deleted generated TXT file: {file_path}")
            except Exception as e:
                logger.error(f"Error processing TXT file {file_path}: {e}")
                files_failed.append(file_path)
        else:
            files_failed.append(file_path)

    await send_message_with_retry(context, update.message.chat_id, "File .txt telah dikirim")
    logger.info(f"All sorted TXT files sent to user {get_user_identity(update)}.")

    # Hapus file yang diproses
    for file_path in files_to_process:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Deleted user uploaded file: {file_path}")

    # Kirim pesan tentang file yang gagal diproses
    if files_failed:
        failed_files_message = f"{len(files_failed)} file gagal diproses:\n" + "\n".join(files_failed)
        await send_message_with_retry(context, update.message.chat_id, failed_files_message)
        logger.info(f"Bot response: {failed_files_message}")
        for file_path in files_failed:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted failed file: {file_path}")

    context.user_data.clear()

# Fungsi untuk membuat file VCF dari semua kontak
async def create_vcf_from_all_contacts(update: Update, context: ContextTypes.DEFAULT_TYPE, phone_numbers, base_contact_name, base_file_name, last_number, index, multiple_files):
    vcf_content = ""
    contact_counter = 1
    contact_name = clean_contact_name(f"{base_contact_name} {string.ascii_uppercase[index]}" if multiple_files else base_contact_name)
    base_file_name = clean_filename(base_file_name)
    vcf_path = f"cache/{base_file_name}{last_number + index}.vcf" if multiple_files else f"cache/{base_file_name}.vcf"
    for i, number in enumerate(phone_numbers, start=1):
        vcf_content += f"BEGIN:VCARD\nVERSION:3.0\nFN:{contact_name} {contact_counter}\nTEL:{number}\nEND:VCARD\n"
        contact_counter += 1
    with open(vcf_path, 'w', encoding='utf-8') as vcf_file:
        vcf_file.write(vcf_content)
    await update.message.reply_document(document=open(vcf_path, 'rb'))
    logger.info(f"Sent VCF file: {vcf_path}")
    if os.path.exists(vcf_path):
        os.remove(vcf_path)
        logger.info(f"Deleted generated VCF file: {vcf_path}")

# Fungsi untuk membuat file VCF dari batch kontak
async def create_vcf_from_batches(update: Update, context: ContextTypes.DEFAULT_TYPE, phone_numbers, base_contact_name, base_file_name, batch_size, last_number, index, multiple_files):
    batches = [phone_numbers[i:i + batch_size] for i in range(0, len(phone_numbers), batch_size)]
    contact_counter = 1
    contact_name = clean_contact_name(f"{base_contact_name} {string.ascii_uppercase[index]}" if multiple_files else base_contact_name)
    base_file_name = clean_filename(base_file_name)
    for batch_index, batch in enumerate(batches, start=1):
        vcf_content = ""
        if multiple_files:
            vcf_path = f"cache/{base_file_name}{last_number + index}_{batch_index}.vcf"
            contact_name = f"{base_contact_name} {string.ascii_uppercase[index]}"
        else:
            vcf_path = f"cache/{base_file_name}{last_number + batch_index - 1}.vcf"
        for i, number in enumerate(batch, start=1):
            vcf_content += f"BEGIN:VCARD\nVERSION:3.0\nFN:{contact_name} {contact_counter}\nTEL:{number}\nEND:VCARD\n"
            contact_counter += 1
        with open(vcf_path, 'w', encoding='utf-8') as vcf_file:
            vcf_file.write(vcf_content)
        await update.message.reply_document(document=open(vcf_path, 'rb'))
        logger.info(f"Sent VCF file: {vcf_path}")
        if os.path.exists(vcf_path):
            os.remove(vcf_path)
            logger.info(f"Deleted generated VCF file: {vcf_path}")

# Fungsi untuk mengekstrak angka terakhir dari nama file, jika ada
def extract_number_from_filename(filename):
    match = re.search(r'(\d+)(?!.*\d)', filename)
    return int(match.group(0)) if match else None

# Inisialisasi bot
application = ApplicationBuilder().token(TELEGRAM_BOT_API_TOKEN).request(HTTPXRequest()).build()

# Menambahkan handler
application.add_handler(CommandHandler("start", start))
application.add_handler(CommandHandler("convert", convert))
application.add_handler(CommandHandler("admin", admin))
application.add_handler(CommandHandler("manual", manual))
application.add_handler(CommandHandler("extract", extract))
application.add_handler(CommandHandler("tambah", tambah))
application.add_handler(CommandHandler("hapus", hapus))
application.add_handler(CommandHandler("jumlah", jumlah))
application.add_handler(CommandHandler("status", status))
application.add_handler(CommandHandler("done", done))
application.add_handler(CommandHandler("remove", remove_cache_files))
application.add_handler(CommandHandler("rename_ctc", rename_ctc))
application.add_handler(CommandHandler("rename_file", rename_file))
application.add_handler(CommandHandler("gabung", gabung))
application.add_handler(CommandHandler("pecah", pecah))
application.add_handler(CommandHandler("hapus_duplikat", hapus_duplikat))
application.add_handler(CommandHandler("rapih", rapih))
application.add_handler(MessageHandler(filters.Document.ALL, handle_file))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

# Menjalankan bot
application.run_polling()