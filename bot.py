#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Shonex Black Market — коммерческий Telegram bot
Вебхук + FastAPI + Telegram Stars + SQLite
"""

import asyncio
import logging
import os
import aiosqlite
from decimal import Decimal, InvalidOperation
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    LabeledPrice, PreCheckoutQuery, Update
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder

# ============================================
# КОНФИГ
# ============================================

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "8710105438"))
ADMIN_USERNAME = "@shonex_01"
DB_PATH = os.getenv("DB_PATH", "shonex.db")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "https://i-n-d-y-leader.onrender.com")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "shonex_webhook_secret_2026")

GAME_NICK = "Shay_Vance"
DONATE_URL = "https://blackrussia.online/donate.php"
RATE_RUB_PER_1KK = Decimal("35")

# ============================================
# СПИСОК СЕРВЕРОВ (91 сервер)
# ============================================

SERVERS = [
    "RED", "GREEN", "BLUE", "YELLOW", "ORANGE", "PURPLE", "LIME", "PINK",
    "CHERRY", "BLACK", "INDIGO", "WHITE", "MAGENTA", "CRIMSON", "GOLD",
    "AZURE", "PLATINUM", "AQUA", "GRAY", "ICE", "CHILLI", "CHOCO",
    "MOSCOW", "SPB", "UFA", "SOCHI", "KAZAN", "SAMARA", "ROSTOV",
    "ANAPA", "EKB", "KRASNODAR", "ARZAMAS", "NOVOSIB", "GROZNY",
    "SARATOV", "OMSK", "IRKUTSK", "VOLGOGRAD", "VORONEZH", "BELGOROD",
    "MAKHACHKALA", "VLADIKAVKAZ", "VLADIVOSTOK", "KALININGRAD",
    "CHELYABINSK", "KRASNOYARSK", "CHEBOKSARY", "KHABAROVSK", "PERM",
    "TULA", "RYAZAN", "MURMANSK", "PENZA", "KURSK", "ARKHANGELSK",
    "ORENBURG", "KIROV", "KEMEROVO", "TYUMEN", "TOLYATTI", "IVANOVO",
    "STAVROPOL", "SMOLENSK", "PSKOV", "BRYANSK", "OREL", "YAROSLAVL",
    "BARNAUL", "LIPETSK", "ULYANOVSK", "YAKUTSK", "TAMBOV", "BRATSK",
    "ASTRAKHAN", "CHITA", "KOSTROMA", "VLADIMIR", "KALUGA", "NOVGOROD",
    "TAGANROG", "VOLOGDA", "TVER", "TOMSK", "IZHEVSK", "SURGUT",
    "PODOLSK", "MAGADAN", "CHEREPOVETS", "NORILSK", "ASTANA",
]

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задан в переменных окружения")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================
# БОТ И ДИСПЕТЧЕР
# ============================================

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()


# ============================================
# БАЗА ДАННЫХ
# ============================================

class DB:
    def __init__(self, path: str = DB_PATH):
        self.path = path

    async def init(self):
        async with aiosqlite.connect(self.path) as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    username TEXT,
                    full_name TEXT,
                    server TEXT,
                    amount REAL,
                    total REAL,
                    method TEXT,
                    receipt_file_id TEXT,
                    status TEXT DEFAULT 'pending',
                    created_at TEXT,
                    updated_at TEXT
                )
            ''')
            await conn.execute('PRAGMA journal_mode=WAL')
            await conn.commit()

    async def create_order(self, user_id, username, full_name, server, amount, total, method, receipt_file_id) -> int:
        now = datetime.now().isoformat()
        async with aiosqlite.connect(self.path) as conn:
            cur = await conn.execute('''
                INSERT INTO orders (user_id, username, full_name, server, amount, total, method, receipt_file_id, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
            ''', (user_id, username, full_name, server, float(amount), float(total), method, receipt_file_id, now, now))
            await conn.commit()
            return cur.lastrowid

    async def get_order(self, order_id: int):
        async with aiosqlite.connect(self.path) as conn:
            cur = await conn.execute('SELECT * FROM orders WHERE id = ?', (order_id,))
            return await cur.fetchone()

    async def set_status(self, order_id: int, status: str):
        async with aiosqlite.connect(self.path) as conn:
            await conn.execute(
                'UPDATE orders SET status = ?, updated_at = ? WHERE id = ?',
                (status, datetime.now().isoformat(), order_id)
            )
            await conn.commit()

    async def get_pending(self):
        async with aiosqlite.connect(self.path) as conn:
            cur = await conn.execute(
                "SELECT id, user_id, username, full_name, server, amount, total, method, created_at FROM orders WHERE status = 'pending' ORDER BY created_at DESC"
            )
            return await cur.fetchall()

    async def get_user_orders(self, user_id: int):
        async with aiosqlite.connect(self.path) as conn:
            cur = await conn.execute(
                'SELECT id, server, amount, total, status, created_at FROM orders WHERE user_id = ? ORDER BY created_at DESC LIMIT 10',
                (user_id,)
            )
            return await cur.fetchall()


db = DB()


# ============================================
# FSM
# ============================================

class OrderFlow(StatesGroup):
    server = State()
    amount = State()
    method = State()
    receipt = State()


# ============================================
# КЛАВИАТУРЫ
# ============================================

def main_kb():
    b = InlineKeyboardBuilder()
    b.button(text="💰 Купить вирты", callback_data="buy")
    b.button(text="📋 Реквизиты", callback_data="details")
    b.button(text="📊 Курс", callback_data="rate")
    b.button(text="📦 Мои заказы", callback_data="my_orders")
    b.button(text="👨‍💼 Менеджер", url=f"https://t.me/{ADMIN_USERNAME.lstrip('@')}")
    b.adjust(1)
    return b.as_markup()


def methods_kb():
    b = InlineKeyboardBuilder()
    b.button(text="🏪 Маркетплейс", callback_data="method_market")
    b.button(text="🤝 Обмен имуществом", callback_data="method_exchange")
    b.button(text="🔄 Трейд", callback_data="method_trade")
    b.button(text="👨‍👩‍👦 Семейный склад", callback_data="method_family")
    b.adjust(1)
    return b.as_markup()


def payment_kb():
    b = InlineKeyboardBuilder()
    b.button(text="⭐ Оплатить Stars", callback_data="pay_stars")
    b.button(text="🌐 Официальный сайт доната", url=DONATE_URL)
    b.button(text="📸 Отправить чек", callback_data="send_receipt")
    b.button(text="❌ Отменить", callback_data="cancel")
    b.adjust(1)
    return b.as_markup()


def admin_order_kb(order_id: int):
    b = InlineKeyboardBuilder()
    b.button(text="✅ Одобрить", callback_data=f"approve_{order_id}")
    b.button(text="❌ Отклонить", callback_data=f"reject_{order_id}")
    b.adjust(2)
    return b.as_markup()


# ============================================
# СТАРТ — ПРИВЕТСТВИЕ + КНОПКИ
# ============================================

@dp.message(CommandStart())
async def start(message: Message, state: FSMContext):
    await state.clear()
    text = (
        "🏪 <b>Shonex Black Market</b>\n\n"
        "Добро пожаловать! Здесь ты можешь оформить сделку быстро и удобно. ⚡️\n\n"
        "Выбери нужный раздел:"
    )
    await message.answer(text, reply_markup=main_kb())


@dp.message(Command("myid"))
async def myid(message: Message):
    await message.answer(f"🆔 Ваш Telegram ID: <code>{message.from_user.id}</code>")


@dp.message(Command("admin"))
async def admin_panel(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    pending = await db.get_pending()
    if not pending:
        await message.answer("📭 Нет ожидающих заказов.")
        return
    text = f"📋 <b>Ожидающие заказы ({len(pending)})</b>\n\n"
    for o in pending[:10]:
        text += (
            f"#{o[0]} | {o[4]} | {o[5]:g}кк | {o[6]:g}₽ | {o[7]}\n"
            f"  от {o[3]} (@{o[2] or 'anon'})\n\n"
        )
    await message.answer(text, parse_mode=ParseMode.HTML)


# ============================================
# CALLBACK: ИНФО
# ============================================

@dp.callback_query(F.data == "details")
async def details(call: CallbackQuery):
    await call.message.answer(
        "💳 <b>Реквизиты для оплаты</b>\n\n"
        "Оплата через официальный сайт доната или Stars.\n"
        "После оплаты отправь чек в бота.",
        reply_markup=payment_kb()
    )
    await call.answer()


@dp.callback_query(F.data == "rate")
async def rate(call: CallbackQuery):
    await call.message.answer(
        f"📊 <b>Текущий курс</b>\n\n"
        f"💰 1kk = <b>{RATE_RUB_PER_1KK} ₽</b>"
    )
    await call.answer()


@dp.callback_query(F.data == "my_orders")
async def my_orders(call: CallbackQuery):
    orders = await db.get_user_orders(call.from_user.id)
    if not orders:
        await call.message.answer("📦 У тебя пока нет заказов.")
        await call.answer()
        return
    statuses = {
        'pending': '⏳ На проверке',
        'approved': '✅ Одобрен',
        'rejected': '❌ Отклонён',
    }
    text = "📦 <b>Твои последние заказы</b>\n\n"
    for o in orders:
        text += f"#{o[0]} | {o[1]} | {o[2]:g}кк | {o[3]:g}₽ | {statuses.get(o[4], o[4])}\n"
    await call.message.answer(text)
    await call.answer()


# ============================================
# КУПИТЬ ВИРТЫ — ВВОД СЕРВЕРА
# ============================================

@dp.callback_query(F.data == "buy")
async def buy(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(OrderFlow.server)
    await call.message.answer(
        "🎮 <b>Шаг 1/3 — выбери сервер</b>\n\n"
        "Напиши название сервера (можно в любом регистре, буквы A-Z):\n"
        "<i>Например: NORILSK, Moscow, ASTANA</i>"
    )
    await call.answer()


@dp.message(OrderFlow.server)
async def server_input(message: Message, state: FSMContext):
    raw = message.text.strip().upper()

    if raw not in SERVERS:
        await message.answer(
            "❌ Такого сервера нет в списке.\n\n"
            "Проверь название и попробуй снова.\n"
            "<i>Например: NORILSK, MOSCOW, ASTANA</i>"
        )
        return

    await state.update_data(server=raw)
    await state.set_state(OrderFlow.amount)

    await message.answer(
        f"✅ Сервер: <b>{raw}</b>\n\n"
        f"🎮 <b>Шаг 2/3 — введи количество виртов</b>\n\n"
        f"Напиши сумму в миллионах (кк).\n"
        f"<i>Например: 100 — это 100кк</i>"
    )


# ============================================
# ШАГ 2: СУММА
# ============================================

@dp.message(OrderFlow.amount)
async def amount_input(message: Message, state: FSMContext):
    raw = message.text.replace(",", ".").replace("кк", "").replace("kk", "").strip()
    try:
        amount = Decimal(raw)
        if amount <= 0:
            raise ValueError
    except (InvalidOperation, ValueError):
        await message.answer("❌ Введи корректное число, например: <code>100</code>.")
        return

    total = amount * RATE_RUB_PER_1KK
    await state.update_data(amount=amount, total=total)
    await state.set_state(OrderFlow.method)

    data = await state.get_data()
    await message.answer(
        f"✅ Сервер: <b>{data['server']}</b>\n"
        f"✅ Количество: <b>{amount:g}кк</b>\n"
        f"💳 К оплате: <b>{total:g} ₽</b>\n\n"
        f"🎮 <b>Шаг 3/3 — выбери способ передачи</b>",
        reply_markup=methods_kb()
    )


# ============================================
# ШАГ 3: СПОСОБ ПЕРЕДАЧИ
# ============================================

@dp.callback_query(OrderFlow.method, F.data.startswith("method_"))
async def method_input(call: CallbackQuery, state: FSMContext):
    names = {
        "method_market": "Маркетплейс",
        "method_exchange": "Обмен имуществом",
        "method_trade": "Трейд",
        "method_family": "Семейный склад",
    }
    selected = names.get(call.data, "Не указан")
    data = await state.get_data()
    await state.update_data(method=selected)

    # ← НИК СВЕТИТСЯ ТОЛЬКО ЗДЕСЬ, ПРИ ОПЛАТЕ
    await call.message.answer(
        "📦 <b>Заказ сформирован</b>\n\n"
        f"🎮 Сервер: <b>{data['server']}</b>\n"
        f"👤 Ник: <b>{GAME_NICK}</b>\n"
        f"💰 Объём: <b>{data['amount']:g}кк</b>\n"
        f"💳 Сумма: <b>{data['total']:g} ₽</b>\n"
        f"🔄 Способ: <b>{selected}</b>\n\n"
        "Оплати через Stars или официальный сайт, затем отправь чек.",
        reply_markup=payment_kb()
    )
    await state.set_state(OrderFlow.receipt)
    await call.answer()


# ============================================
# ОТПРАВКА ЧЕКА
# ============================================

@dp.callback_query(F.data == "send_receipt")
async def send_receipt(call: CallbackQuery, state: FSMContext):
    await state.set_state(OrderFlow.receipt)
    await call.message.answer("📸 Отправь скриншот или фото чека об оплате.")
    await call.answer()


# ============================================
# ОПЛАТА STARS
# ============================================

@dp.callback_query(F.data == "pay_stars")
async def pay_stars(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    server = data.get('server')
    amount = data.get('amount')
    total = data.get('total')

    if not amount or not total or not server:
        await call.answer("Сначала оформи заказ", show_alert=True)
        return

    stars_amount = int(total / 2)

    await bot.send_invoice(
        chat_id=call.from_user.id,
        title=f"Вирты Black Russia ({amount:g}кк)",
        description=f"Сервер {server}, ник {GAME_NICK}",
        payload=f"order_{call.from_user.id}_{server}_{amount}",
        provider_token="",
        currency="XTR",
        prices=[LabeledPrice(label="Вирты", amount=stars_amount)]
    )
    await call.answer()


@dp.pre_checkout_query()
async def pre_checkout(query: PreCheckoutQuery):
    await query.answer(ok=True)


@dp.message(F.successful_payment)
async def payment_received(message: Message, state: FSMContext):
    data = await state.get_data()
    server = data.get('server', 'Не указан')
    amount = data.get('amount', Decimal(0))
    total = data.get('total', Decimal(0))
    method = data.get('method', 'Не указан')

    order_id = await db.create_order(
        user_id=message.from_user.id,
        username=message.from_user.username,
        full_name=message.from_user.full_name,
        server=server,
        amount=amount,
        total=total,
        method=f"{method} (Stars)",
        receipt_file_id="STARS_PAYMENT",
    )

    admin_text = (
        f"⭐ <b>Оплата Stars — Заявка #{order_id}</b>\n\n"
        f"👤 Клиент: {message.from_user.full_name}\n"
        f"🆔 ID: <code>{message.from_user.id}</code>\n"
        f"📱 Username: @{message.from_user.username or 'нет'}\n\n"
        f"🎮 Сервер: <b>{server}</b>\n"
        f"👤 Ник: <b>{GAME_NICK}</b>\n"
        f"💰 Объём: <b>{amount:g}кк</b>\n"
        f"💳 Сумма: <b>{total:g} ₽</b>\n"
        f"🔄 Способ: <b>{method}</b>\n"
        f"⭐ Оплачено Stars"
    )

    try:
        await bot.send_message(
            chat_id=ADMIN_ID,
            text=admin_text,
            reply_markup=admin_order_kb(order_id)
        )
    except Exception as e:
        logger.error(f"Ошибка отправки админу: {e}")

    await message.answer(
        f"✅ <b>Заявка #{order_id} принята!</b>\n\n"
        "Оплата Stars получена. Менеджер проверит и свяжется с тобой.\n"
        "Статус: «📦 Мои заказы».",
        reply_markup=main_kb()
    )
    await state.clear()


# ============================================
# ПОЛУЧЕНИЕ ЧЕКА (ФОТО)
# ============================================

@dp.message(OrderFlow.receipt, F.photo)
async def receipt_photo(message: Message, state: FSMContext):
    data = await state.get_data()
    server = data.get('server')
    amount = data.get('amount')
    total = data.get('total')
    method = data.get('method', 'Не указан')

    if not amount or not total or not server:
        await message.answer("❌ Ошибка: данные заказа потеряны. Начни заново /start")
        await state.clear()
        return

    file_id = message.photo[-1].file_id
    order_id = await db.create_order(
        user_id=message.from_user.id,
        username=message.from_user.username,
        full_name=message.from_user.full_name,
        server=server,
        amount=amount,
        total=total,
        method=method,
        receipt_file_id=file_id,
    )

    admin_text = (
        f"🔔 <b>Новая заявка #{order_id}</b>\n\n"
        f"👤 Клиент: {message.from_user.full_name}\n"
        f"🆔 ID: <code>{message.from_user.id}</code>\n"
        f"📱 Username: @{message.from_user.username or 'нет'}\n\n"
        f"🎮 Сервер: <b>{server}</b>\n"
        f"👤 Ник: <b>{GAME_NICK}</b>\n"
        f"💰 Объём: <b>{amount:g}кк</b>\n"
        f"💳 Сумма: <b>{total:g} ₽</b>\n"
        f"🔄 Способ: <b>{method}</b>"
    )

    try:
        await bot.send_photo(
            chat_id=ADMIN_ID,
            photo=file_id,
            caption=admin_text,
            reply_markup=admin_order_kb(order_id)
        )
    except Exception as e:
        logger.error(f"Ошибка отправки админу: {e}")

    await message.answer(
        f"✅ <b>Заявка #{order_id} принята!</b>\n\n"
        "Менеджер проверит чек и свяжется с тобой.\n"
        "Статус можно посмотреть в разделе «📦 Мои заказы».",
        reply_markup=main_kb()
    )
    await state.clear()


@dp.message(OrderFlow.receipt)
async def receipt_not_photo(message: Message):
    await message.answer("📸 Пожалуйста, отправь именно фото/скриншот чека.")


@dp.callback_query(F.data == "cancel")
async def cancel(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.answer("❌ Заявка отменена.", reply_markup=main_kb())
    await call.answer()


# ============================================
# ОДОБРЕНИЕ / ОТКЛОНЕНИЕ (АДМИН)
# ============================================

@dp.callback_query(F.data.startswith("approve_"))
async def approve(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return

    order_id = int(call.data.split("_")[1])
    order = await db.get_order(order_id)
    if not order:
        await call.answer("Заказ не найден", show_alert=True)
        return

    await db.set_status(order_id, 'approved')

    try:
        await bot.send_message(
            order[1],
            f"✅ <b>Заявка #{order_id} одобрена!</b>\n\n"
            f"🎮 Сервер: <b>{order[4]}</b>\n"
            f"💰 Объём: <b>{order[5]:g}кк</b>\n"
            f"💳 Сумма: <b>{order[6]:g} ₽</b>\n"
            f"🔄 Способ: <b>{order[7]}</b>\n\n"
            f"Менеджер свяжется с тобой для передачи виртов.\n"
            f"Спасибо за сделку! ⚡️",
            reply_markup=main_kb()
        )
    except Exception as e:
        logger.error(f"Ошибка уведомления клиента: {e}")

    if call.message.photo:
        await call.message.edit_caption(
            caption=f"✅ Заявка #{order_id} <b>ОДОБРЕНА</b>",
            parse_mode=ParseMode.HTML
        )
    else:
        await call.message.edit_text(
            f"✅ Заявка #{order_id} <b>ОДОБРЕНА</b>",
            parse_mode=ParseMode.HTML
        )
    await call.answer("Одобрено")


@dp.callback_query(F.data.startswith("reject_"))
async def reject(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return

    order_id = int(call.data.split("_")[1])
    order = await db.get_order(order_id)
    if not order:
        await call.answer("Заказ не найден", show_alert=True)
        return

    await db.set_status(order_id, 'rejected')

    try:
        await bot.send_message(
            order[1],
            f"❌ <b>Заявка #{order_id} отклонена.</b>\n\n"
            f"Если ты уверен, что оплата была — свяжись с менеджером: {ADMIN_USERNAME}",
            reply_markup=main_kb()
        )
    except Exception as e:
        logger.error(f"Ошибка уведомления клиента: {e}")

    if call.message.photo:
        await call.message.edit_caption(
            caption=f"❌ Заявка #{order_id} <b>ОТКЛОНЕНА</b>",
            parse_mode=ParseMode.HTML
        )
    else:
        await call.message.edit_text(
            f"❌ Заявка #{order_id} <b>ОТКЛОНЕНА</b>",
            parse_mode=ParseMode.HTML
        )
    await call.answer("Отклонено")


# ============================================
# FASTAPI + WEBHOOK
# ============================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init()
    await bot.delete_webhook(drop_pending_updates=True)
    await asyncio.sleep(0.5)
    await bot.set_webhook(
        url=f"{WEBHOOK_URL}/webhook",
        secret_token=WEBHOOK_SECRET,
        allowed_updates=["message", "callback_query", "pre_checkout_query", "successful_payment"],
        drop_pending_updates=True
    )
    logger.info(f"✅ Webhook: {WEBHOOK_URL}/webhook")
    yield
    await bot.session.close()


app = FastAPI(title="Shonex Black Market", lifespan=lifespan)


@app.post("/webhook")
async def webhook(request: Request):
    secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
    if secret != WEBHOOK_SECRET:
        logger.warning("Неверный секрет вебхука")
        return Response(status_code=403)

    try:
        data = await request.json()
        update = Update.model_validate(data, context={"bot": bot})
        await dp.feed_update(bot, update)
    except Exception as e:
        logger.error(f"Webhook error: {e}")
    return Response(content="OK", status_code=200)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/")
async def root():
    return {"status": "Shonex Black Market is running"}


# ============================================
# ЗАПУСК
# ============================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
