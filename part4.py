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

