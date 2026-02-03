import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage

from app.telegram_bot.config import load_bot_config, load_portfolio_db_config, load_moex_api_config
from app.telegram_bot.database import PortfolioDatabase
from app.telegram_bot.price_service import PriceService
from app.telegram_bot.handlers import create_handlers_router

logger = logging.getLogger(__name__)


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    
    logger.info("Starting Telegram bot...")
    
    bot_config = load_bot_config()
    portfolio_db_config = load_portfolio_db_config()
    moex_api_config = load_moex_api_config()
    
    bot = Bot(token=bot_config.token)
    dp = Dispatcher(storage=MemoryStorage())
    
    portfolio_db = PortfolioDatabase(portfolio_db_config)
    await portfolio_db.connect()
    await portfolio_db.create_tables(drop_existing=False)
    
    price_service = PriceService(api_url=moex_api_config.url)
    
    router = create_handlers_router(portfolio_db, price_service)
    dp.include_router(router)
    
    logger.info("Bot initialized. Starting polling...")
    
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    finally:
        await price_service.close()
        await portfolio_db.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
