import asyncio
import logging
from datetime import datetime, timezone
from telethon import TelegramClient
from telethon.errors import ChannelPrivateError, FloodWaitError
from app.news_feeder.config import load_config
from app.news_feeder.database import Database
from app.news_feeder.parser import extract_tags

# TODO: migrate to Prefect for automated script execution

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class NewsFeeder:
    def __init__(self, config, db: Database, client: TelegramClient):
        self.config = config
        self.db = db
        self.client = client

    async def poll_channel(self, channel_name: str):
        try:
            logger.info(f"Starting poll for channel: {channel_name}")

            try:
                channel = await self.client.get_entity(channel_name)
            except ValueError as e:
                logger.error(f"Failed to find channel {channel_name}: {e}")
                return
            except ChannelPrivateError:
                logger.error(f"Channel {channel_name} is private or unavailable")
                return

            messages = await self.client.get_messages(channel, limit=5)
            logger.info(
                f"Retrieved {len(messages)} latest messages from channel {channel_name}"
            )

            saved_count = 0
            skipped_count = 0

            for msg in messages:
                if not msg.text:
                    continue

                tags = extract_tags(msg.text)

                msg_date = msg.date
                if msg_date.tzinfo is None:
                    msg_date = msg_date.replace(tzinfo=timezone.utc)

                saved = await self.db.save_news(
                    msg_id=msg.id,
                    channel=channel_name,
                    date=msg_date,
                    tags=tags,
                    text=msg.text,
                )

                if saved:
                    saved_count += 1
                else:
                    skipped_count += 1

            current_time = datetime.now(timezone.utc)
            await self.db.update_poll_state(
                channel=channel_name, last_poll_time=current_time
            )

            logger.info(
                f"Poll for channel {channel_name} completed: "
                f"saved {saved_count}, skipped {skipped_count} messages"
            )

        except FloodWaitError as e:
            logger.warning(
                f"Rate limit exceeded for channel {channel_name}, waiting {e.seconds} seconds"
            )
            await asyncio.sleep(e.seconds)
        except Exception as e:
            logger.error(f"Error polling channel {channel_name}: {e}", exc_info=True)

    async def poll_all_channels(self):
        logger.info(f"Starting poll for {len(self.config.channels)} channels")

        for channel_name in self.config.channels:
            await self.poll_channel(channel_name)
            await asyncio.sleep(1)

    async def run(self):
        logger.info("Starting news polling system")

        try:
            await self.poll_all_channels()
        except Exception as e:
            logger.error(f"Critical error: {e}", exc_info=True)
            raise
        finally:
            await self.db.close()
            await self.client.disconnect()


async def main():
    try:
        config = load_config()
        logger.info("Configuration loaded")

        db = Database(config.database)
        await db.connect()
        await db.create_tables(drop_existing=True)

        client = TelegramClient(
            config.telegram.session_file,
            config.telegram.api_id,
            config.telegram.api_hash,
        )
        await client.start()
        logger.info("Telegram client connected")

        feeder = NewsFeeder(config, db, client)
        await feeder.run()

    except Exception as e:
        logger.error(f"Error during startup: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    asyncio.run(main())
