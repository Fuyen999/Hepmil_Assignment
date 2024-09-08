from typing import Final
import os
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

load_dotenv()
TOKEN: Final = os.getenv('BOT_TOKEN')
BOT_USERNAME: Final = os.getenv('BOT_USERNAME')

## Commands
# Message when user press start button (when starting the bot)
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text('Hello!')

# Message when user uses /help
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text('No help for u sad')

# Message when user uses /custom
async def generate_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    report_name = os.getenv("REPORT_NAME")
    report = open(os.getenv("REPORT_NAME"), "rb")
    await update.message.reply_document(report, caption=report_name)


## Message handler
def handle_response(text: str) -> str:
    return "This bot only accepts /generate command"

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message_type: str = update.message.chat.type # group or private chat
    text: str = update.message.text

    print(f'User ({update.message.chat.id}) in {message_type}: "{text}')

    if message_type == "group":
        if BOT_USERNAME in text:
            cleaned_text: str = text.replace(BOT_USERNAME, "").strip()
            response: str = handle_response(cleaned_text)
        else: 
            return
    else:
        response: str = handle_response(text)

    print(f"Bot: {response}")

    await update.message.reply_text(response)


## Error handler
async def error(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print(f"Update {update} caused error {context.error}")


## Run bot
if __name__ == '__main__':
    print("Starting bot ...")
    app = Application.builder().token(TOKEN).build()
    
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("generate", generate_command))

    app.add_handler(MessageHandler(filters.TEXT, handle_message))

    app.add_error_handler(error)

    print("Polling ...")
    app.run_polling(poll_interval=2)