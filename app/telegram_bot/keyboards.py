from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton


def get_main_menu() -> ReplyKeyboardMarkup:
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Portfolio")],
        ],
        resize_keyboard=True,
    )
    return keyboard


def get_portfolio_menu() -> ReplyKeyboardMarkup:
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Add Securities"), KeyboardButton(text="Sell Securities")],
            [KeyboardButton(text="Main Menu")],
        ],
        resize_keyboard=True,
    )
    return keyboard


def get_phone_keyboard() -> ReplyKeyboardMarkup:
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Share Phone Number", request_contact=True)],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    return keyboard


def get_cancel_keyboard() -> ReplyKeyboardMarkup:
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Cancel")],
        ],
        resize_keyboard=True,
    )
    return keyboard
