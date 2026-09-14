#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SHONEX Market — P2P маркетплейс услуг
Стек: aiogram 3.x + FastAPI + asyncpg (Supabase PostgreSQL) + Telegram Stars
Escrow: Stars на балансе бота. Курс: 1 Star = 1.5 ₽. Комиссия: 20%.
"""

import asyncio
import logging
import os
import asyncpg
from contextlib import asynccontextmanager
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Request, Response
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery, LabeledPrice, Message, PreCheckoutQuery, Update
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

# ============================================
# КОНФИГ
# ============================================

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "https://shonex-market.onrender.com")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "shonex_market_secret")
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")

RATE_STAR_TO_RUB = Decimal("1.5")
DEFAULT_COMMISSION = Decimal("0.20")
MAX_STARS_PER_INVOICE = 2500

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задан")
if not SUPABASE_DB_URL:
    raise RuntimeError("SUPABASE_DB_URL не задан")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("shonex")

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
router = Router()
dp.include_router(router)

# Глобальный пул соединений
_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        # statement_cache_size=0 — критично для Supabase Session Pooler
        _pool = await asyncpg.create_pool(
            SUPABASE_DB_URL,
            min_size=2,
            max_size=10,
            statement_cache_size=0,
        )
        log.info("Supabase connection pool created")
    return _pool


async def close_pool():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        log.info("Connection pool closed")


# ============================================
# СХЕМА БАЗЫ
# ============================================

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    role TEXT DEFAULT 'client',
    stars_balance INTEGER DEFAULT 0,
    stars_frozen INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    is_blocked INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS executors (
    user_id BIGINT PRIMARY KEY REFERENCES users(user_id),
    display_name TEXT,
    specialization TEXT,
    experience TEXT,
    portfolio TEXT,
    percent INTEGER DEFAULT 60,
    status TEXT DEFAULT 'pending',
    rating DOUBLE PRECISION DEFAULT 0,
    completed INTEGER DEFAULT 0,
    cancelled INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    client_id BIGINT REFERENCES users(user_id),
    executor_id BIGINT REFERENCES users(user_id),
    category TEXT,
    service TEXT,
    description TEXT,
    amount_stars INTEGER,
    commission_stars INTEGER,
    executor_stars INTEGER,
    status TEXT DEFAULT 'created',
    result_text TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    user_id BIGINT,
    order_id INTEGER,
    amount INTEGER,
    type TEXT,
    status TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS reviews (
    id SERIAL PRIMARY KEY,
    order_id INTEGER,
    executor_id BIGINT,
    client_id BIGINT,
    rating INTEGER,
    comment TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
"""


async def init_schema():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(SCHEMA_SQL)
    log.info("Schema initialized")


# ============================================
# DB FUNCTIONS
# ============================================

async def add_user(uid: int, username: str, first_name: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO users (user_id, username, first_name, created_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (user_id) DO UPDATE
            SET username = EXCLUDED.username, first_name = EXCLUDED.first_name
        """, uid, username, first_name, datetime.now())


async def get_user(uid: int) -> Optional[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", uid)
        return dict(row) if row else None


async def get_balance(uid: int) -> Dict[str, int]:
    user = await get_user(uid)
    if not user:
        return {"available": 0, "frozen": 0}
    return {"available": user["stars_balance"], "frozen": user["stars_frozen"]}


async def freeze_balance(uid: int, amount: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET stars_balance = stars_balance - $1, stars_frozen = stars_frozen + $2 WHERE user_id = $3",
            amount, amount, uid
        )


async def release_frozen(client_id: int, executor_id: int, amount: int, executor_reward: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "UPDATE users SET stars_frozen = stars_frozen - $1 WHERE user_id = $2",
                amount, client_id
            )
            await conn.execute(
                "UPDATE users SET stars_balance = stars_balance + $1 WHERE user_id = $2",
                executor_reward, executor_id
            )


async def create_order(client_id: int, category: str, service: str, description: str, amount_stars: int, commission_stars: int, executor_stars: int) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO orders (client_id, category, service, description, amount_stars, commission_stars, executor_stars, status, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending_payment', $8, $9)
            RETURNING id
        """, client_id, category, service, description, amount_stars, commission_stars, executor_stars, datetime.now(), datetime.now())
        return row["id"]


async def get_order(order_id: int) -> Optional[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM orders WHERE id = $1", order_id)
        return dict(row) if row else None


async def set_order_status(order_id: int, status: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE orders SET status = $1, updated_at = $2 WHERE id = $3",
            status, datetime.now(), order_id
        )


async def assign_executor(order_id: int, executor_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE orders SET executor_id = $1, status = 'in_progress', updated_at = $2 WHERE id = $3",
            executor_id, datetime.now(), order_id
        )


async def submit_result(order_id: int, result_text: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE orders SET result_text = $1, status = 'pending_client', updated_at = $2 WHERE id = $3",
            result_text, datetime.now(), order_id
        )


async def list_available_orders() -> List[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM orders WHERE status = 'pending_executor' AND executor_id IS NULL ORDER BY id DESC LIMIT 20"
        )
        return [dict(r) for r in rows]


async def list_client_orders(uid: int) -> List[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM orders WHERE client_id = $1 OR executor_id = $2 ORDER BY id DESC LIMIT 20",
            uid, uid
        )
        return [dict(r) for r in rows]


async def create_executor_application(uid: int, display_name: str, specialization: str, experience: str, portfolio: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO executors (user_id, display_name, specialization, experience, portfolio, created_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (user_id) DO UPDATE
            SET display_name = EXCLUDED.display_name,
                specialization = EXCLUDED.specialization,
                experience = EXCLUDED.experience,
                portfolio = EXCLUDED.portfolio,
                status = 'pending'
        """, uid, display_name, specialization, experience, portfolio, datetime.now())


async def set_executor_status(uid: int, status: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE executors SET status = $1 WHERE user_id = $2", status, uid)
        if status == "active":
            await conn.execute("UPDATE users SET role = 'executor' WHERE user_id = $1", uid)


async def get_executor(uid: int) -> Optional[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM executors WHERE user_id = $1", uid)
        return dict(row) if row else None


async def list_pending_executors() -> List[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM executors WHERE status = 'pending'")
        return [dict(r) for r in rows]


async def log_transaction(user_id: int, order_id: int, amount: int, tx_type: str, status: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO transactions (user_id, order_id, amount, type, status, created_at)
            VALUES ($1, $2, $3, $4, $5, $6)
        """, user_id, order_id, amount, tx_type, status, datetime.now())


async def add_review(order_id: int, executor_id: int, client_id: int, rating: int, comment: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("""
                INSERT INTO reviews (order_id, executor_id, client_id, rating, comment, created_at)
                VALUES ($1, $2, $3, $4, $5, $6)
            """, order_id, executor_id, client_id, rating, comment, datetime.now())
            row = await conn.fetchrow(
                "SELECT AVG(rating) as avg, COUNT(*) as cnt FROM reviews WHERE executor_id = $1",
                executor_id
            )
            if row["cnt"]:
                await conn.execute(
                    "UPDATE executors SET rating = $1 WHERE user_id = $2",
                    round(row["avg"], 2), executor_id
                )


# ============================================
# FSM
# ============================================

class OrderState(StatesGroup):
    category = State()
    service = State()
    description = State()
    amount = State()


class ExecutorState(StatesGroup):
    name = State()
    specialization = State()
    experience = State()
    portfolio = State()


class ReviewState(StatesGroup):
    waiting = State()


# ============================================
# КЛАВИАТУРЫ
# ============================================

def main_kb(role: str = "client"):
    b = InlineKeyboardBuilder()
    if role == "client":
        b.button(text="📦 Создать заказ", callback_data="client:create_order")
        b.button(text="📋 Мои заказы", callback_data="client:my_orders")
        b.button(text="👨‍💻 Стать исполнителем", callback_data="executor:apply")
        b.button(text="⭐ Баланс", callback_data="client:balance")
    elif role == "executor":
        b.button(text="📦 Доступные заказы", callback_data="executor:available")
        b.button(text="📋 Мои заказы", callback_data="client:my_orders")
        b.button(text="⭐ Баланс", callback_data="client:balance")
    b.adjust(1)
    return b.as_markup()


def categories_kb():
    b = InlineKeyboardBuilder()
    b.button(text="💻 IT", callback_data="cat:it")
    b.button(text="🎮 Standoff 2", callback_data="cat:standoff")
    b.button(text="🪖 PUBG", callback_data="cat:pubg")
    b.button(text="🇷🇺 Black Russia", callback_data="cat:blackrussia")
    b.button(text="📚 Учебная помощь", callback_data="cat:study")
    b.button(text="🎨 Дизайн", callback_data="cat:design")
    b.button(text="🔙 Назад", callback_data="menu")
    b.adjust(2)
    return b.as_markup()


def in_progress_kb(order_id: int):
    b = InlineKeyboardBuilder()
    b.button(text="📤 Отправить результат", callback_data=f"executor:submit:{order_id}")
    b.adjust(1)
    return b.as_markup()


def client_decision_kb(order_id: int):
    b = InlineKeyboardBuilder()
    b.button(text="✅ Подтвердить", callback_data=f"client:confirm:{order_id}")
    b.button(text="⚠️ Открыть спор", callback_data=f"client:dispute:{order_id}")
    b.adjust(2)
    return b.as_markup()


# ============================================
# ESCROW (Telegram Stars)
# ============================================

class EscrowService:
    @staticmethod
    async def create_invoice(chat_id: int, order: dict):
        stars = order["amount_stars"]
        if stars > MAX_STARS_PER_INVOICE:
            await bot.send_message(chat_id, f"❌ Сумма превышает лимит Stars ({MAX_STARS_PER_INVOICE}).")
            return
        await bot.send_invoice(
            chat_id=chat_id,
            title=f"Заказ #{order['id']}",
            description=f"{order['service']} | {order['description'][:100]}",
            payload=f"order:{order['id']}",
            provider_token="",
            currency="XTR",
            prices=[LabeledPrice(label="Оплата заказа", amount=stars)],
        )

    @staticmethod
    async def on_successful_payment(message: Message):
        payload = message.successful_payment.invoice_payload
        if not payload.startswith("order:"):
            return
        order_id = int(payload.split(":")[1])
        order = await get_order(order_id)
        if not order:
            return
        await freeze_balance(order["client_id"], order["amount_stars"])
        await set_order_status(order_id, "pending_executor")
        await log_transaction(order["client_id"], order_id, order["amount_stars"], "payment", "frozen")
        await message.answer(f"✅ Заказ #{order_id} оплачен. Ищем исполнителя...")
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(admin_id, f"🔔 Новый заказ #{order_id} на {order['amount_stars']} ⭐")
            except Exception:
                pass


# ============================================
# HANDLERS
# ============================================

@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await add_user(message.from_user.id, message.from_user.username or "", message.from_user.first_name or "")
    user = await get_user(message.from_user.id)
    role = "client"
    if user and user.get("role") == "executor":
        ex = await get_executor(message.from_user.id)
        if ex and ex["status"] == "active":
            role = "executor"
    await message.answer(
        f"👋 Привет, <b>{message.from_user.first_name}</b>!\n\n"
        "Это <b>SHONEX Market</b> — маркетплейс услуг.\n"
        f"💰 Курс: 1 ⭐ = {RATE_STAR_TO_RUB} ₽\n\n"
        "Выбери действие:",
        reply_markup=main_kb(role),
    )


@router.callback_query(F.data == "menu")
async def cb_menu(call: CallbackQuery, state: FSMContext):
    await state.clear()
    user = await get_user(call.from_user.id)
    role = "client"
    if user and user.get("role") == "executor":
        ex = await get_executor(call.from_user.id)
        if ex and ex["status"] == "active":
            role = "executor"
    await call.message.edit_text("🏠 <b>Главное меню</b>", reply_markup=main_kb(role))
    await call.answer()


@router.callback_query(F.data == "client:create_order")
async def cb_create_order(call: CallbackQuery, state: FSMContext):
    await state.set_state(OrderState.category)
    await call.message.edit_text("📦 <b>Выбери категорию:</b>", reply_markup=categories_kb())
    await call.answer()


@router.callback_query(OrderState.category, F.data.startswith("cat:"))
async def cb_category(call: CallbackQuery, state: FSMContext):
    await state.update_data(category=call.data.split(":")[1])
    await state.set_state(OrderState.service)
    await call.message.edit_text("✍️ Напиши название услуги (кратко):")
    await call.answer()


@router.message(OrderState.service)
async def order_service(message: Message, state: FSMContext):
    await state.update_data(service=message.text.strip())
    await state.set_state(OrderState.description)
    await message.answer("📝 Опиши задачу подробнее:")


@router.message(OrderState.description)
async def order_description(message: Message, state: FSMContext):
    await state.update_data(description=message.text.strip())
    await state.set_state(OrderState.amount)
    await message.answer(f"💰 Укажи стоимость в рублях (число).\nКурс: 1 ⭐ = {RATE_STAR_TO_RUB} ₽")


@router.message(OrderState.amount)
async def order_amount(message: Message, state: FSMContext):
    try:
        amount = float(message.text.strip().replace(",", "."))
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи корректную сумму, например: 1500")
        return

    data = await state.get_data()
    amount_stars = int(Decimal(str(amount)) / RATE_STAR_TO_RUB)
    commission = int(amount_stars * DEFAULT_COMMISSION)
    executor_reward = amount_stars - commission

    order_id = await create_order(
        client_id=message.from_user.id,
        category=data["category"],
        service=data["service"],
        description=data["description"],
        amount_stars=amount_stars,
        commission_stars=commission,
        executor_stars=executor_reward,
    )
    order = await get_order(order_id)
    await state.clear()
    await message.answer(
        f"📦 <b>Заказ #{order_id} создан</b>\n\n"
        f"Категория: {order['category']}\n"
        f"Услуга: {order['service']}\n"
        f"Сумма: {order['amount_stars']} ⭐ ({amount} ₽)\n\n"
        "Оплати, чтобы мы начали искать исполнителя.",
    )
    await EscrowService.create_invoice(message.chat.id, order)


@router.callback_query(F.data == "client:balance")
async def cb_balance(call: CallbackQuery):
    balance = await get_balance(call.from_user.id)
    await call.message.edit_text(
        f"⭐ <b>Твой баланс</b>\n\n"
        f"💰 Доступно: <b>{balance['available']}</b> ⭐\n"
        f"🔒 В заказах: <b>{balance['frozen']}</b> ⭐",
        reply_markup=main_kb(),
    )
    await call.answer()


@router.callback_query(F.data == "client:my_orders")
async def cb_my_orders(call: CallbackQuery):
    orders = await list_client_orders(call.from_user.id)
    if not orders:
        await call.message.edit_text("📋 У тебя пока нет заказов.", reply_markup=main_kb())
        await call.answer()
        return
    text = "📋 <b>Твои заказы:</b>\n\n"
    for o in orders:
        emoji = {
            "pending_payment": "⏳",
            "pending_executor": "🔍",
            "in_progress": "🟣",
            "pending_client": "🟠",
            "completed": "✅",
            "disputed": "🔴",
        }.get(o["status"], "❓")
        text += f"{emoji} #{o['id']} | {o['service']} | {o['amount_stars']} ⭐ | {o['status']}\n"
    await call.message.edit_text(text, reply_markup=main_kb())
    await call.answer()


@router.callback_query(F.data.startswith("client:confirm:"))
async def cb_client_confirm(call: CallbackQuery, state: FSMContext):
    order_id = int(call.data.split(":")[2])
    order = await get_order(order_id)
    if not order or order["client_id"] != call.from_user.id:
        await call.answer("Нет доступа", show_alert=True)
        return
    if order["status"] != "pending_client":
        await call.answer("Заказ не в статусе подтверждения", show_alert=True)
        return
    await release_frozen(
        client_id=order["client_id"],
        executor_id=order["executor_id"],
        amount=order["amount_stars"],
        executor_reward=order["executor_stars"],
    )
    await set_order_status(order_id, "completed")
    await log_transaction(order["executor_id"], order_id, order["executor_stars"], "payout", "completed")
    await call.message.edit_text(f"✅ Заказ #{order_id} подтверждён. Исполнитель получил {order['executor_stars']} ⭐.")
    try:
        await bot.send_message(order["executor_id"], f"💰 Тебе начислено {order['executor_stars']} ⭐ за заказ #{order_id}!")
    except Exception:
        pass
    await call.answer()
    await state.set_state(ReviewState.waiting)
    await state.update_data(review_order=order_id, review_executor=order["executor_id"])
    await call.message.answer("⭐ Оцени исполнителя от 1 до 5 (просто напиши число):")


@router.callback_query(F.data.startswith("client:dispute:"))
async def cb_client_dispute(call: CallbackQuery):
    order_id = int(call.data.split(":")[2])
    order = await get_order(order_id)
    if not order or order["client_id"] != call.from_user.id:
        await call.answer("Нет доступа", show_alert=True)
        return
    await set_order_status(order_id, "disputed")
    await call.message.edit_text(f"⚠️ Спор по заказу #{order_id} открыт. Админ свяжется с тобой.")
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, f"⚠️ Спор по заказу #{order_id}. Клиент: {order['client_id']}, исполнитель: {order['executor_id']}")
        except Exception:
            pass
    await call.answer()


@router.message(ReviewState.waiting)
async def review_input(message: Message, state: FSMContext):
    data = await state.get_data()
    try:
        rating = int(message.text.strip())
        if rating < 1 or rating > 5:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи число от 1 до 5")
        return
    await add_review(
        order_id=data["review_order"],
        executor_id=data["review_executor"],
        client_id=message.from_user.id,
        rating=rating,
        comment="",
    )
    await state.clear()
    await message.answer(f"✅ Спасибо за оценку {rating} ⭐!", reply_markup=main_kb())


# ============================================
# EXECUTOR HANDLERS
# ============================================

@router.callback_query(F.data == "executor:apply")
async def cb_executor_apply(call: CallbackQuery, state: FSMContext):
    await state.set_state(ExecutorState.name)
    await call.message.edit_text("👨‍💻 <b>Заявка исполнителя</b>\n\nШаг 1/4 — как тебя звать (псевдоним):")
    await call.answer()


@router.message(ExecutorState.name)
async def executor_name(message: Message, state: FSMContext):
    await state.update_data(name=message.text.strip())
    await state.set_state(ExecutorState.specialization)
    await message.answer("🎯 Шаг 2/4 — специализация (например: Python, боты, дизайн):")


@router.message(ExecutorState.specialization)
async def executor_specialization(message: Message, state: FSMContext):
    await state.update_data(spec=message.text.strip())
    await state.set_state(ExecutorState.experience)
    await message.answer("📝 Шаг 3/4 — опыт работы (кратко):")


@router.message(ExecutorState.experience)
async def executor_experience(message: Message, state: FSMContext):
    await state.update_data(exp=message.text.strip())
    await state.set_state(ExecutorState.portfolio)
    await message.answer("🔗 Шаг 4/4 — портфолио (ссылки, примеры):")


@router.message(ExecutorState.portfolio)
async def executor_portfolio(message: Message, state: FSMContext):
    data = await state.get_data()
    await create_executor_application(
        uid=message.from_user.id,
        display_name=data["name"],
        specialization=data["spec"],
        experience=data["exp"],
        portfolio=message.text.strip(),
    )
    await state.clear()
    await message.answer("✅ Заявка отправлена на модерацию. Ожидай одобрения.", reply_markup=main_kb())
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                f"👨‍💻 <b>Новая заявка исполнителя</b>\n\n"
                f"ID: <code>{message.from_user.id}</code>\n"
                f"Имя: {data['name']}\n"
                f"Спец: {data['spec']}\n"
                f"Опыт: {data['exp']}\n"
                f"Портфолио: {message.text.strip()}\n\n"
                f"Одобрить? /approve_{message.from_user.id}"
            )
        except Exception:
            pass


@router.callback_query(F.data == "executor:available")
async def cb_executor_available(call: CallbackQuery):
    ex = await get_executor(call.from_user.id)
    if not ex or ex["status"] != "active":
        await call.answer("Ты не активный исполнитель", show_alert=True)
        return
    orders = await list_available_orders()
    if not orders:
        await call.message.edit_text("📦 Свободных заказов нет.", reply_markup=main_kb("executor"))
        await call.answer()
        return
    text = "📦 <b>Свободные заказы:</b>\n\n"
    for o in orders[:10]:
        text += f"#{o['id']} | {o['category']} | {o['service']} | {o['executor_stars']} ⭐\n"
    await call.message.edit_text(text, reply_markup=main_kb("executor"))
    await call.answer()


@router.callback_query(F.data.startswith("executor:take:"))
async def cb_executor_take(call: CallbackQuery):
    order_id = int(call.data.split(":")[2])
    order = await get_order(order_id)
    if not order or order["status"] != "pending_executor":
        await call.answer("Заказ уже занят", show_alert=True)
        return
    await assign_executor(order_id, call.from_user.id)
    await call.message.edit_text(
        f"✅ Ты взял заказ #{order_id}.\n"
        f"Клиент: <code>{order['client_id']}</code>\n"
        f"Задача: {order['description']}\n\n"
        "Когда выполнишь — отправь результат.",
        reply_markup=in_progress_kb(order_id),
    )
    try:
        await bot.send_message(order["client_id"], f"🟣 Исполнитель взял заказ #{order_id} в работу.")
    except Exception:
        pass
    await call.answer()


@router.callback_query(F.data.startswith("executor:submit:"))
async def cb_executor_submit(call: CallbackQuery, state: FSMContext):
    order_id = int(call.data.split(":")[2])
    order = await get_order(order_id)
    if not order or order["executor_id"] != call.from_user.id:
        await call.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(OrderState.description)  # переиспользуем
    await state.update_data(submit_order=order_id)
    await call.message.edit_text("📤 Отправь результат (текст, ссылку или описание):")
    await call.answer()


# ============================================
# ADMIN HANDLERS
# ============================================

@router.message(Command("admin"))
async def cmd_admin(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    pending = await list_pending_executors()
    await message.answer(
        f"🔐 <b>Админ-панель</b>\n\n"
        f"Заявок на модерацию: {len(pending)}"
    )


@router.message(Command(re.compile(r"approve_(\d+)")))
async def admin_approve(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    uid = int(message.text.split("_")[1])
    await set_executor_status(uid, "active")
    await message.answer(f"✅ Исполнитель {uid} одобрен")
    try:
        await bot.send_message(uid, "✅ Твоя заявка одобрена! Теперь ты можешь брать заказы.")
    except Exception:
        pass


@router.message(Command(re.compile(r"reject_(\d+)")))
async def admin_reject(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    uid = int(message.text.split("_")[1])
    await set_executor_status(uid, "rejected")
    await message.answer(f"❌ Исполнитель {uid} отклонён")


# ============================================
# WEBHOOK + FASTAPI
# ============================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_schema()
    await bot.delete_webhook(drop_pending_updates=True)
    await asyncio.sleep(0.5)
    await bot.set_webhook(
        url=f"{WEBHOOK_URL}/webhook",
        secret_token=WEBHOOK_SECRET,
        allowed_updates=["message", "callback_query", "pre_checkout_query", "successful_payment"],
        drop_pending_updates=True,
    )
    log.info("Webhook: %s/webhook", WEBHOOK_URL)
    yield
    await close_pool()
    await bot.session.close()


app = FastAPI(title="SHONEX Market", lifespan=lifespan)


@app.post("/webhook")
async def webhook(request: Request):
    if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        return Response(status_code=403)
    try:
        data = await request.json()
        update = Update.model_validate(data, context={"bot": bot})
        await dp.feed_update(bot, update)
    except Exception as e:
        log.error("Webhook error: %s", e)
    return Response(content="OK", status_code=200)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "shonex-market"}


@app.get("/")
async def root():
    return {"status": "SHONEX Market is running"}


# ============================================
# PAYMENTS
# ============================================

@router.pre_checkout_query()
async def pre_checkout(query: PreCheckoutQuery):
    await query.answer(ok=True)


@router.message(F.successful_payment)
async def on_payment(message: Message):
    await EscrowService.on_successful_payment(message)


# ============================================
# ЗАПУСК
# ============================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
