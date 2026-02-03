import logging
from datetime import datetime, date
from typing import Dict

from aiogram import Router, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, Contact, CallbackQuery
from aiogram_calendar import SimpleCalendar, SimpleCalendarCallback, get_user_locale

from .database import PortfolioDatabase
from .price_service import PriceService
from .keyboards import (
    get_main_menu,
    get_portfolio_menu,
    get_phone_keyboard,
    get_cancel_keyboard,
)

logger = logging.getLogger(__name__)


class RegistrationState(StatesGroup):
    waiting_phone = State()


class AddPositionState(StatesGroup):
    waiting_ticker = State()
    waiting_price = State()
    waiting_quantity = State()
    waiting_date = State()


class SellPositionState(StatesGroup):
    waiting_ticker = State()
    waiting_price = State()
    waiting_quantity = State()
    waiting_date = State()


def create_handlers_router(
    db: PortfolioDatabase, price_service: PriceService
) -> Router:
    router = Router()

    @router.message(Command("start"))
    async def cmd_start(message: Message, state: FSMContext):
        await state.clear()
        
        user = await db.get_user_by_telegram_id(message.from_user.id)
        
        if user:
            await message.answer(
                f"Welcome back, {message.from_user.first_name}!\n\n"
                "Use the menu to navigate.",
                reply_markup=get_main_menu(),
            )
        else:
            await message.answer(
                "Welcome! To get started, you need to register.\n\n"
                "Please share your phone number.",
                reply_markup=get_phone_keyboard(),
            )
            await state.set_state(RegistrationState.waiting_phone)

    @router.message(StateFilter(RegistrationState.waiting_phone), F.contact)
    async def process_phone_contact(message: Message, state: FSMContext):
        contact: Contact = message.contact
        
        if contact.user_id != message.from_user.id:
            await message.answer(
                "Please share your own phone number.",
                reply_markup=get_phone_keyboard(),
            )
            return
        
        try:
            existing_user = await db.get_user_by_telegram_id(message.from_user.id)
            if existing_user:
                await message.answer(
                    "You are already registered!",
                    reply_markup=get_main_menu(),
                )
                await state.clear()
                return
            
            phone_number = contact.phone_number
            if not phone_number.startswith("+"):
                phone_number = "+" + phone_number
            
            user = await db.create_user(
                telegram_id=message.from_user.id,
                phone_number=phone_number,
            )
            
            await message.answer(
                f"Registration completed successfully!\n"
                f"Your phone number: {phone_number}\n\n"
                "Now you can manage your portfolio.",
                reply_markup=get_main_menu(),
            )
            await state.clear()
            
        except ValueError as e:
            await message.answer(
                f"Registration error: {e}",
                reply_markup=get_phone_keyboard(),
            )
        except Exception as e:
            logger.error(f"Error during registration: {e}", exc_info=True)
            await message.answer(
                "An error occurred during registration. Please try again later.",
                reply_markup=get_phone_keyboard(),
            )

    @router.message(F.text == "Portfolio")
    async def show_portfolio(message: Message, state: FSMContext):
        await state.clear()
        
        user = await db.get_user_by_telegram_id(message.from_user.id)
        if not user:
            await message.answer(
                "You are not registered. Use /start to register.",
            )
            return
        
        portfolio = await db.get_user_portfolio(user.id)
        
        if not portfolio:
            await message.answer(
                "Your portfolio is empty.\n\n"
                "Use the 'Add Securities' button to add positions.",
                reply_markup=get_portfolio_menu(),
            )
            return
        
        tickers = list(portfolio.keys())
        prices = await price_service.get_current_prices(tickers)
        
        portfolio_text = "📊 Your Portfolio:\n\n"
        
        for ticker, data in portfolio.items():
            current_price = prices.get(ticker)
            
            portfolio_text += f"🔹 *{ticker}*\n"
            portfolio_text += f"   Quantity: {data['total_quantity']} shares\n"
            portfolio_text += f"   Average purchase price: {data['average_price']:.2f} ₽\n"
            portfolio_text += f"   Total cost: {data['total_cost']:.2f} ₽\n"
            
            if current_price is not None:
                current_value = current_price * data['total_quantity']
                profit = current_value - data['total_cost']
                profit_percent = (profit / data['total_cost'] * 100) if data['total_cost'] > 0 else 0
                
                portfolio_text += f"   Current price: {current_price:.2f} ₽\n"
                portfolio_text += f"   Current value: {current_value:.2f} ₽\n"
                
                portfolio_text += f"   Profit/Loss: {profit:+.2f} ₽ ({profit_percent:+.2f}%)\n"
            else:
                portfolio_text += f"   Current price: unavailable\n"
            
            if len(data['purchases']) > 1:
                portfolio_text += f"\n   Purchases:\n"
                for purchase in data['purchases']:
                    purchase_profit = ""
                    if current_price is not None:
                        purchase_value = current_price * purchase['quantity']
                        purchase_cost = purchase['price'] * purchase['quantity']
                        purchase_profit_val = purchase_value - purchase_cost
                        purchase_profit_percent = (purchase_profit_val / purchase_cost * 100) if purchase_cost > 0 else 0
                        purchase_profit = f" ({purchase_profit_val:+.2f} ₽, {purchase_profit_percent:+.2f}%)"
                    
                    portfolio_text += (
                        f"      • {purchase['quantity']} shares at {purchase['price']:.2f} ₽ "
                        f"({purchase['date']}){purchase_profit}\n"
                    )
            
            portfolio_text += "\n"
        
        await message.answer(
            portfolio_text,
            reply_markup=get_portfolio_menu(),
            parse_mode='markdown'
        )

    @router.message(F.text == "Add Securities")
    async def start_add_position(message: Message, state: FSMContext):
        user = await db.get_user_by_telegram_id(message.from_user.id)
        if not user:
            await message.answer(
                "You are not registered. Use /start to register.",
            )
            return
        
        await message.answer(
            "Enter the security ticker (e.g., SBER, GAZP):",
            reply_markup=get_cancel_keyboard(),
        )
        await state.set_state(AddPositionState.waiting_ticker)

    @router.message(F.text == "Cancel")
    async def cancel_action(message: Message, state: FSMContext):
        await state.clear()
        await message.answer(
            "Action cancelled.",
            reply_markup=get_main_menu(),
        )

    @router.message(F.text == "Main Menu")
    async def back_to_main_menu(message: Message, state: FSMContext):
        await state.clear()
        await message.answer(
            "Main Menu:",
            reply_markup=get_main_menu(),
        )

    @router.message(StateFilter(AddPositionState.waiting_ticker), F.text)
    async def process_ticker(message: Message, state: FSMContext):
        ticker = message.text.strip().upper()
        
        if not ticker or len(ticker) > 20:
            await message.answer(
                "Invalid ticker. Enter a security ticker (e.g., SBER):",
                reply_markup=get_cancel_keyboard(),
            )
            return
        
        await state.update_data(ticker=ticker)
        await message.answer(
            f"Ticker: {ticker}\n\n"
            "Enter the purchase price per share (in rubles, e.g., 300.50):",
            reply_markup=get_cancel_keyboard(),
        )
        await state.set_state(AddPositionState.waiting_price)

    @router.message(StateFilter(AddPositionState.waiting_price), F.text)
    async def process_price(message: Message, state: FSMContext):
        try:
            price = float(message.text.strip().replace(",", "."))
            if price <= 0:
                raise ValueError("Price must be positive")
        except ValueError:
            await message.answer(
                "Invalid price. Enter the purchase price in rubles (e.g., 300.50):",
                reply_markup=get_cancel_keyboard(),
            )
            return
        
        await state.update_data(price=price)
        await message.answer(
            f"Price: {price:.2f} ₽\n\n"
            "Enter the number of shares (whole number, e.g., 10):",
            reply_markup=get_cancel_keyboard(),
        )
        await state.set_state(AddPositionState.waiting_quantity)

    @router.message(StateFilter(AddPositionState.waiting_quantity), F.text)
    async def process_quantity(message: Message, state: FSMContext):
        try:
            quantity = int(message.text.strip())
            if quantity <= 0:
                raise ValueError("Quantity must be positive")
        except ValueError:
            await message.answer(
                "Invalid quantity. Enter a whole number (e.g., 10):",
                reply_markup=get_cancel_keyboard(),
            )
            return
        
        await state.update_data(quantity=quantity)
        
        calendar = SimpleCalendar(
            locale=await get_user_locale(message.from_user),
            show_alerts=True
        )
        calendar.set_dates_range(datetime(2020, 1, 1), datetime.now())
        
        await message.answer(
            f"Quantity: {quantity} shares\n\n"
            "Select the purchase date:",
            reply_markup=await calendar.start_calendar(),
        )
        await state.set_state(AddPositionState.waiting_date)

    @router.callback_query(SimpleCalendarCallback.filter(), StateFilter(AddPositionState.waiting_date))
    async def process_calendar(callback_query: CallbackQuery, callback_data: SimpleCalendarCallback, state: FSMContext):
        user = await db.get_user_by_telegram_id(callback_query.from_user.id)
        if not user:
            await callback_query.message.answer(
                "Error: user not found.",
                reply_markup=get_main_menu(),
            )
            await state.clear()
            await callback_query.answer()
            return
        
        calendar = SimpleCalendar(
            locale=await get_user_locale(callback_query.from_user),
            show_alerts=True
        )
        calendar.set_dates_range(datetime(2020, 1, 1), datetime.now())
        
        selected, purchase_date = await calendar.process_selection(callback_query, callback_data)
        
        if selected:
            purchase_date = purchase_date.date()
            
            data = await state.get_data()
            ticker = data.get("ticker")
            price = data.get("price")
            quantity = data.get("quantity")
            
            try:
                purchase = await db.add_purchase(
                    user_id=user.id,
                    ticker=ticker,
                    purchase_price=price,
                    quantity=quantity,
                    purchase_date=purchase_date,
                )
                
                await callback_query.message.answer(
                    f"✅ Position added successfully!\n\n"
                    f"Ticker: {ticker}\n"
                    f"Quantity: {quantity} shares\n"
                    f"Purchase price: {price:.2f} ₽\n"
                    f"Purchase date: {purchase_date}\n"
                    f"Total cost: {price * quantity:.2f} ₽",
                    reply_markup=get_portfolio_menu(),
                )
                await state.clear()
                
            except Exception as e:
                logger.error(f"Error adding purchase: {e}", exc_info=True)
                await callback_query.message.answer(
                    "An error occurred while adding the position. Please try again later.",
                    reply_markup=get_portfolio_menu(),
                )
                await state.clear()
        
        await callback_query.answer()

    @router.message(StateFilter(AddPositionState.waiting_date), F.text)
    async def process_date_text(message: Message, state: FSMContext):
        text = message.text.strip().lower()
        
        if text == "cancel":
            await state.clear()
            await message.answer(
                "Action cancelled.",
                reply_markup=get_main_menu(),
            )
            return
        
        await message.answer(
            "Please use the calendar to select a date or press 'Cancel' to cancel the action.",
            reply_markup=get_cancel_keyboard(),
        )

    @router.message(F.text == "Sell Securities")
    async def start_sell_position(message: Message, state: FSMContext):
        user = await db.get_user_by_telegram_id(message.from_user.id)
        if not user:
            await message.answer(
                "You are not registered. Use /start to register.",
            )
            return
        
        await message.answer(
            "Enter the security ticker to sell (e.g., SBER, GAZP):",
            reply_markup=get_cancel_keyboard(),
        )
        await state.set_state(SellPositionState.waiting_ticker)

    @router.message(StateFilter(SellPositionState.waiting_ticker), F.text)
    async def process_sell_ticker(message: Message, state: FSMContext):
        user = await db.get_user_by_telegram_id(message.from_user.id)
        if not user:
            await message.answer(
                "You are not registered. Use /start to register.",
            )
            await state.clear()
            return
        
        ticker = message.text.strip().upper()
        
        if not ticker or len(ticker) > 20:
            await message.answer(
                "Invalid ticker. Enter a security ticker (e.g., SBER):",
                reply_markup=get_cancel_keyboard(),
            )
            return
        
        available_quantity = await db.get_available_quantity(user.id, ticker)
        
        if available_quantity <= 0:
            await message.answer(
                f"You don't have any shares of {ticker} to sell.\n\n"
                "Please enter a different ticker or cancel.",
                reply_markup=get_cancel_keyboard(),
            )
            return
        
        await state.update_data(ticker=ticker)
        await message.answer(
            f"Ticker: {ticker}\n"
            f"Available shares: {available_quantity}\n\n"
            "Enter the sale price per share (in rubles, e.g., 300.50):",
            reply_markup=get_cancel_keyboard(),
        )
        await state.set_state(SellPositionState.waiting_price)

    @router.message(StateFilter(SellPositionState.waiting_price), F.text)
    async def process_sell_price(message: Message, state: FSMContext):
        try:
            price = float(message.text.strip().replace(",", "."))
            if price <= 0:
                raise ValueError("Price must be positive")
        except ValueError:
            await message.answer(
                "Invalid price. Enter the sale price in rubles (e.g., 300.50):",
                reply_markup=get_cancel_keyboard(),
            )
            return
        
        await state.update_data(price=price)
        await message.answer(
            f"Price: {price:.2f} ₽\n\n"
            "Enter the number of shares to sell (whole number, e.g., 10):",
            reply_markup=get_cancel_keyboard(),
        )
        await state.set_state(SellPositionState.waiting_quantity)

    @router.message(StateFilter(SellPositionState.waiting_quantity), F.text)
    async def process_sell_quantity(message: Message, state: FSMContext):
        user = await db.get_user_by_telegram_id(message.from_user.id)
        if not user:
            await message.answer(
                "You are not registered. Use /start to register.",
            )
            await state.clear()
            return
        
        try:
            quantity = int(message.text.strip())
            if quantity <= 0:
                raise ValueError("Quantity must be positive")
        except ValueError:
            await message.answer(
                "Invalid quantity. Enter a whole number (e.g., 10):",
                reply_markup=get_cancel_keyboard(),
            )
            return
        
        data = await state.get_data()
        ticker = data.get("ticker")
        available_quantity = await db.get_available_quantity(user.id, ticker)
        
        if quantity > available_quantity:
            await message.answer(
                f"❌ Insufficient shares!\n\n"
                f"You are trying to sell {quantity} shares of {ticker},\n"
                f"but you only have {available_quantity} shares available.\n\n"
                f"Please enter a valid quantity (max {available_quantity}):",
                reply_markup=get_cancel_keyboard(),
            )
            return
        
        await state.update_data(quantity=quantity)
        
        calendar = SimpleCalendar(
            locale=await get_user_locale(message.from_user),
            show_alerts=True
        )
        calendar.set_dates_range(datetime(2020, 1, 1), datetime.now())
        
        await message.answer(
            f"Quantity: {quantity} shares\n\n"
            "Select the sale date:",
            reply_markup=await calendar.start_calendar(),
        )
        await state.set_state(SellPositionState.waiting_date)

    @router.callback_query(SimpleCalendarCallback.filter(), StateFilter(SellPositionState.waiting_date))
    async def process_sell_calendar(callback_query: CallbackQuery, callback_data: SimpleCalendarCallback, state: FSMContext):
        user = await db.get_user_by_telegram_id(callback_query.from_user.id)
        if not user:
            await callback_query.message.answer(
                "Error: user not found.",
                reply_markup=get_main_menu(),
            )
            await state.clear()
            await callback_query.answer()
            return
        
        calendar = SimpleCalendar(
            locale=await get_user_locale(callback_query.from_user),
            show_alerts=True
        )
        calendar.set_dates_range(datetime(2020, 1, 1), datetime.now())
        
        selected, sale_date = await calendar.process_selection(callback_query, callback_data)
        
        if selected:
            sale_date = sale_date.date()
            
            data = await state.get_data()
            ticker = data.get("ticker")
            price = data.get("price")
            quantity = data.get("quantity")
            
            try:
                available_quantity = await db.get_available_quantity(user.id, ticker)
                
                if quantity > available_quantity:
                    await callback_query.message.answer(
                        f"❌ Insufficient shares!\n\n"
                        f"You are trying to sell {quantity} shares of {ticker},\n"
                        f"but you only have {available_quantity} shares available.\n\n"
                        f"Please enter a valid quantity.",
                        reply_markup=get_cancel_keyboard(),
                    )
                    await callback_query.answer()
                    return
                
                sale = await db.add_sale(
                    user_id=user.id,
                    ticker=ticker,
                    sale_price=price,
                    quantity=quantity,
                    sale_date=sale_date,
                )
                
                await callback_query.message.answer(
                    f"✅ Sale recorded successfully!\n\n"
                    f"Ticker: {ticker}\n"
                    f"Quantity: {quantity} shares\n"
                    f"Sale price: {price:.2f} ₽\n"
                    f"Sale date: {sale_date}\n"
                    f"Total proceeds: {price * quantity:.2f} ₽",
                    reply_markup=get_portfolio_menu(),
                )
                await state.clear()
                
            except Exception as e:
                logger.error(f"Error adding sale: {e}", exc_info=True)
                await callback_query.message.answer(
                    "An error occurred while recording the sale. Please try again later.",
                    reply_markup=get_portfolio_menu(),
                )
                await state.clear()
        
        await callback_query.answer()

    @router.message(StateFilter(SellPositionState.waiting_date), F.text)
    async def process_sell_date_text(message: Message, state: FSMContext):
        text = message.text.strip().lower()
        
        if text == "cancel":
            await state.clear()
            await message.answer(
                "Action cancelled.",
                reply_markup=get_main_menu(),
            )
            return
        
        await message.answer(
            "Please use the calendar to select a date or press 'Cancel' to cancel the action.",
            reply_markup=get_cancel_keyboard(),
        )

    return router
