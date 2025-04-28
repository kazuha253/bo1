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