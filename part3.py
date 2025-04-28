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