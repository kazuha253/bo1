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

