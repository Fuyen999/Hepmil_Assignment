from typing import Final
import os
from dotenv import dotenv_values
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from generator import regeneration_check, get_newest_update, connect_database_and_cache_images, fetch_data_and_plot_graph, generate_html_report, generate_pdf_report

## Fetch config/secrets from environment variable
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
config = dotenv_values(dotenv_path)
TOKEN: Final = config['BOT_TOKEN']
BOT_USERNAME: Final = config['BOT_USERNAME']
REGENERATE_AFTER_SECONDS = 1

## Commands
# Message when user press start button (when starting the bot)
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text('Hello!')

# Message when user uses /help
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text('No help for u sad')

# Message when user uses /generate
async def generate_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Check if pdf reports needs to be regenerated
    pdf_report_path = regeneration_check(REGENERATE_AFTER_SECONDS)
    if pdf_report_path is None:
        # Fetch newest data
        await update.message.reply_text('Fetching newest data ...')
        timestamp = await get_newest_update()

        # Cache top 20 images
        await update.message.reply_text('Fetching images ...')
        engine, top_memes_data = await connect_database_and_cache_images()

        # Plot votes against time graph
        await update.message.reply_text('Plotting graph ...')
        fetch_data_and_plot_graph(engine)

        # Generate HTML and PDF reports
        await update.message.reply_text('Generating report ...')
        html_report_path = generate_html_report(top_memes_data, timestamp)
        pdf_report_path = generate_pdf_report(html_report_path)
        
    # Send PDF report to user
    await update.message.reply_text('Sending ...')
    report = open(pdf_report_path, "rb")
    await update.message.reply_document(report, caption="Top 20 memes report")
    engine.dispose()


## Message handler
# Default reply if user enters any text message
def handle_response(text: str) -> str:
    return "This bot only accepts /generate command"

# Enables different behaviour in groups / private chat
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


## Run bot by simple polling
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
