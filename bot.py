#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SHONEX ULTRA v3.2 — Production Ready (OOP + Full Interface)
Исправлены баги, добавлены кнопки, уведомления и защита.
"""

import asyncio
import logging
import os
import re
from contextlib import asynccontextmanager
from decimal import Decimal
from typing import Optional, Dict, Any, List

import asyncpg
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery, LabeledPrice, Message, PreCheckoutQuery, Update
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from fastapi import FastAPI, Request, Response

# ============================================
# КОНФИГ
# ============================================

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "https://shonex-market.onrender.com")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "shonex_market_secret_v2")
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")

RATE_STAR_TO_RUB = Decimal("1.5")
DEFAULT_COMMISSION = Decimal("0.20")
MAX_STARS_PER_INVOICE = 2500

if not BOT_TOKEN:
    raise RuntimeError("❌ BOT_TOKEN не задан")
if not ADMIN_IDS:
    raise RuntimeError("❌ ADMIN_IDS пуст! Добавь свой Telegram ID")
if not SUPABASE_DB_URL:
    raise RuntimeError("❌ SUPABASE_DB_URL не задан")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
)
log = logging.getLogger("shonex")

# ============================================
# FSM СОСТОЯНИЯ
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
# СЛОЙ БАЗЫ ДАННЫХ (Data Layer)
# ============================================

class Database:
    """Инкапсуляция работы с Supabase/PostgreSQL."""
    
    def __init__(self, dsn: str):
        self.dsn = dsn
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        if self._pool is None:
            # statement_cache_size=0 — критично для Supabase Pooler
            self._pool = await asyncpg.create_pool(
                self.dsn, min_size=2, max_size=10, statement_cache_size=0
            )
            log.info("✅ Database pool created")

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None
            log.info("❌ Database pool closed")

    @asynccontextmanager
    async def connection(self):
        if not self._pool:
            raise RuntimeError("Database not connected")
        async with self._pool.acquire() as conn:
            yield conn

    async def init_schema(self) -> None:
        schema = """
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
        async with self.connection() as conn:
            await conn.execute(schema)
        log.info("✅ Schema initialized")

    # --- USERS ---
    async def add_user(self, uid: int, username: str, first_name: str) -> None:
        async with self.connection() as conn:
            await conn.execute("""
                INSERT INTO users (user_id, username, first_name, created_at)
                VALUES ($1, $2, $3, NOW())
                ON CONFLICT (user_id) DO UPDATE
                SET username = EXCLUDED.username, first_name = EXCLUDED.first_name
            """, uid, username, first_name)

    async def get_user(self, uid: int) -> Optional[Dict[str, Any]]:
        async with self.connection() as conn:
            row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", uid)
            return dict(row) if row else None

    async def get_balance(self, uid: int) -> Dict[str, int]:
        user = await self.get_user(uid)
        if not user:
            return {"available": 0, "frozen": 0}
        return {"available": user["stars_balance"], "frozen": user["stars_frozen"]}

    async def freeze_balance(self, uid: int, amount: int) -> bool:
        """Замораживает средства. Возвращает True при успехе."""
        async with self.connection() as conn:
            result = await conn.execute(
                "UPDATE users SET stars_balance = stars_balance - $1, "
                "stars_frozen = stars_frozen + $2 WHERE user_id = $3 AND stars_balance >= $1",
                amount, amount, uid
            )
            return result == "UPDATE 1"

    async def release_frozen(self, client_id: int, executor_id: int,
                             amount: int, executor_reward: int) -> bool:
        """Освобождает замороженные средства. Транзакция + проверка статуса."""
        async with self.connection() as conn:
            async with conn.transaction():
                # Блокируем строку заказа, чтобы избежать двойного списания
                order_check = await conn.fetchrow(
                    "SELECT status FROM orders WHERE client_id = $1 AND amount_stars = $2 "
                    "AND status = 'pending_client' FOR UPDATE",
                    client_id, amount
                )
                if not order_check:
                    log.warning(f"⚠️ Попытка двойного подтверждения для клиента {client_id}")
                    return False
                
                await conn.execute(
                    "UPDATE users SET stars_frozen = stars_frozen - $1 WHERE user_id = $2",
                    amount, client_id
                )
                await conn.execute(
                    "UPDATE users SET stars_balance = stars_balance + $1 WHERE user_id = $2",
                    executor_reward, executor_id
                )
                return True

    # --- ORDERS ---
    async def create_order(self, client_id: int, category: str, service: str,
                           description: str, amount_stars: int,
                           commission_stars: int, executor_stars: int) -> int:
        async with self.connection() as conn:
            row = await conn.fetchrow("""
                INSERT INTO orders (client_id, category, service, description,
                    amount_stars, commission_stars, executor_stars, status, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending_payment', NOW(), NOW())
                RETURNING id
            """, client_id, category, service, description[:500],
                 amount_stars, commission_stars, executor_stars)
            return row["id"]

    async def get_order(self, order_id: int) -> Optional[Dict[str, Any]]:
        async with self.connection() as conn:
            row = await conn.fetchrow("SELECT * FROM orders WHERE id = $1", order_id)
            return dict(row) if row else None

    async def set_order_status(self, order_id: int, status: str) -> None:
        async with self.connection() as conn:
            await conn.execute(
                "UPDATE orders SET status = $1, updated_at = NOW() WHERE id = $2",
                status, order_id
            )

    async def assign_executor(self, order_id: int, executor_id: int) -> None:
        async with self.connection() as conn:
            await conn.execute(
                "UPDATE orders SET executor_id = $1, status = 'in_progress', "
                "updated_at = NOW() WHERE id = $2",
                executor_id, order_id
            )

    async def submit_result(self, order_id: int, result_text: str) -> None:
        async with self.connection() as conn:
            await conn.execute(
                "UPDATE orders SET result_text = $1, status = 'pending_client', "
                "updated_at = NOW() WHERE id = $2",
                result_text[:1000], order_id
            )

    async def list_available_orders(self) -> List[Dict[str, Any]]:
        async with self.connection() as conn:
            rows = await conn.fetch(
                "SELECT * FROM orders WHERE status = 'pending_executor' "
                "AND executor_id IS NULL ORDER BY id DESC LIMIT 10"
            )
            return [dict(r) for r in rows]

    async def list_client_orders(self, uid: int) -> List[Dict[str, Any]]:
        async with self.connection() as conn:
            rows = await conn.fetch(
                "SELECT * FROM orders WHERE client_id = $1 OR executor_id = $2 "
                "ORDER BY id DESC LIMIT 20",
                uid, uid
            )
            return [dict(r) for r in rows]

    # --- EXECUTORS ---
    async def create_executor_application(self, uid: int, display_name: str,
                                          specialization: str, experience: str,
                                          portfolio: str) -> None:
        async with self.connection() as conn:
            await conn.execute("""
                INSERT INTO executors (user_id, display_name, specialization,
                    experience, portfolio, created_at)
                VALUES ($1, $2, $3, $4, $5, NOW())
                ON CONFLICT (user_id) DO UPDATE
                SET display_name = EXCLUDED.display_name,
                    specialization = EXCLUDED.specialization,
                    experience = EXCLUDED.experience,
                    portfolio = EXCLUDED.portfolio,
                    status = 'pending'
            """, uid, display_name, specialization, experience, portfolio)

    async def set_executor_status(self, uid: int, status: str) -> None:
        async with self.connection() as conn:
            await conn.execute("UPDATE executors SET status = $1 WHERE user_id = $2", status, uid)
            if status == "active":
                await conn.execute("UPDATE users SET role = 'executor' WHERE user_id = $1", uid)

    async def get_executor(self, uid: int) -> Optional[Dict[str, Any]]:
        async with self.connection() as conn:
            row = await conn.fetchrow("SELECT * FROM executors WHERE user_id = $1", uid)
            return dict(row) if row else None

    async def list_pending_executors(self) -> List[Dict[str, Any]]:
        async with self.connection() as conn:
            rows = await conn.fetch("SELECT * FROM executors WHERE status = 'pending'")
            return [dict(r) for r in rows]

    # --- TRANSACTIONS & REVIEWS ---
    async def log_transaction(self, user_id: int, order_id: int, amount: int,
                              tx_type: str, status: str) -> None:
        async with self.connection() as conn:
            await conn.execute("""
                INSERT INTO transactions (user_id, order_id, amount, type, status, created_at)
                VALUES ($1, $2, $3, $4, $5, NOW())
            """, user_id, order_id, amount, tx_type, status)

    async def add_review(self, order_id: int, executor_id: int,
                         client_id: int, rating: int, comment: str = "") -> None:
        async with self.connection() as conn:
            async with conn.transaction():
                await conn.execute("""
                    INSERT INTO reviews (order_id, executor_id, client_id, rating, comment, created_at)
                    VALUES ($1, $2, $3, $4, $5, NOW())
                """, order_id, executor_id, client_id, rating, comment)
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
# СЛОЙ КЛАВИАТУР (Presentation Layer)
# ============================================

class Keyboards:
    """Фабрика всех клавиатур."""

    @staticmethod
    def main_menu(role: str = "client") -> InlineKeyboardBuilder.as_markup:
        builder = InlineKeyboardBuilder()
        if role == "client":
            builder.button(text="📦 Создать заказ", callback_data="client:create_order")
            builder.button(text="📋 Мои заказы", callback_data="client:my_orders")
            builder.button(text="⭐ Баланс", callback_data="client:balance")
            builder.button(text="👨‍💻 Стать исполнителем", callback_data="executor:apply")
            builder.button(text="🆘 Помощь", callback_data="help")
        elif role == "executor":
            builder.button(text="📦 Доступные заказы", callback_data="executor:available")
            builder.button(text="📋 Мои заказы", callback_data="client:my_orders")
            builder.button(text="⭐ Баланс", callback_data="client:balance")
            builder.button(text="👤 Профиль", callback_data="executor:profile")
            builder.button(text="🆘 Помощь", callback_data="help")
        builder.adjust(2, 2, 1)
        return builder.as_markup()

    @staticmethod
    def categories() -> InlineKeyboardBuilder.as_markup:
        builder = InlineKeyboardBuilder()
        builder.button(text="💻 IT", callback_data="cat:it")
        builder.button(text="🎮 Standoff 2", callback_data="cat:standoff")
        builder.button(text="🪖 PUBG", callback_data="cat:pubg")
        builder.button(text="🇷🇺 Black Russia", callback_data="cat:blackrussia")
        builder.button(text="📚 Учебная помощь", callback_data="cat:study")
        builder.button(text="🎨 Дизайн", callback_data="cat:design")
        builder.button(text="🔙 Назад", callback_data="menu")
        builder.adjust(2, 2, 2, 1)
        return builder.as_markup()

    @staticmethod
    def available_orders(orders: list) -> InlineKeyboardBuilder.as_markup:
        builder = InlineKeyboardBuilder()
        for order in orders[:10]:
            builder.button(
                text=f"📦 #{order['id']} | {order['service'][:20]} | {order['executor_stars']} ⭐",
                callback_data=f"executor:take:{order['id']}"
            )
        builder.button(text="🔙 Назад", callback_data="menu")
        builder.adjust(1)
        return builder.as_markup()

    @staticmethod
    def in_progress(order_id: int) -> InlineKeyboardBuilder.as_markup:
        builder = InlineKeyboardBuilder()
        builder.button(text="📤 Отправить результат", callback_data=f"executor:submit:{order_id}")
        builder.adjust(1)
        return builder.as_markup()

    @staticmethod
    def client_decision(order_id: int) -> InlineKeyboardBuilder.as_markup:
        builder = InlineKeyboardBuilder()
        builder.button(text="✅ Подтвердить", callback_data=f"client:confirm:{order_id}")
        builder.button(text="⚠️ Открыть спор", callback_data=f"client:dispute:{order_id}")
        builder.adjust(2)
        return builder.as_markup()

    @staticmethod
    def executor_moderation(uid: int) -> InlineKeyboardBuilder.as_markup:
        builder = InlineKeyboardBuilder()
        builder.button(text="✅ Одобрить", callback_data=f"admin:approve:{uid}")
        builder.button(text="❌ Отклонить", callback_data=f"admin:reject:{uid}")
        builder.adjust(2)
        return builder.as_markup()

    @staticmethod
    def help_menu() -> InlineKeyboardBuilder.as_markup:
        builder = InlineKeyboardBuilder()
        builder.button(text="📖 Как это работает", callback_data="help:how")
        builder.button(text="❓ FAQ", callback_data="help:faq")
        builder.button(text="🆘 Поддержка", callback_data="help:support")
        builder.button(text="🔙 Назад", callback_data="menu")
        builder.adjust(1)
        return builder.as_markup()

# ============================================
# СЛОЙ ХЕНДЛЕРОВ (Business Layer)
# ============================================

class BaseHandler:
    def __init__(self, db: Database, bot: Bot):
        self.db = db
        self.bot = bot


class MainHandlers(BaseHandler):
    """Главное меню, клиентские сценарии, FSM заказа."""

    def __init__(self, db: Database, bot: Bot, router: Router):
        super().__init__(db, bot)
        self.router = router
        self._register()

    def _register(self):
        self.router.message(CommandStart())(self.cmd_start)
        self.router.message(Command("cancel"))(self.cmd_cancel)
        self.router.callback_query(F.data == "menu")(self.cb_menu)
        self.router.callback_query(F.data == "client:create_order")(self.cb_create_order)
        self.router.callback_query(OrderState.category, F.data.startswith("cat:"))(self.cb_category)
        self.router.message(OrderState.service)(self.order_service)
        self.router.message(OrderState.description)(self.order_description)
        self.router.message(OrderState.amount)(self.order_amount)
        self.router.callback_query(F.data == "client:balance")(self.cb_balance)
        self.router.callback_query(F.data == "client:my_orders")(self.cb_my_orders)
        self.router.callback_query(F.data.startswith("client:confirm:"))(self.cb_client_confirm)
        self.router.callback_query(F.data.startswith("client:dispute:"))(self.cb_client_dispute)
        self.router.message(ReviewState.waiting)(self.review_input)
        # Executor application flow
        self.router.callback_query(F.data == "executor:apply")(self.cb_executor_apply)
        self.router.message(ExecutorState.name)(self.executor_name)
        self.router.message(ExecutorState.specialization)(self.executor_specialization)
        self.router.message(ExecutorState.experience)(self.executor_experience)
        self.router.message(ExecutorState.portfolio)(self.executor_portfolio)
        self.router.callback_query(F.data == "executor:available")(self.cb_executor_available)
        self.router.callback_query(F.data.startswith("executor:take:"))(self.cb_executor_take)
        self.router.callback_query(F.data.startswith("executor:submit:"))(self.cb_executor_submit)
        self.router.callback_query(F.data == "executor:profile")(self.cb_executor_profile)
        # Help
        self.router.callback_query(F.data == "help")(self.cb_help)
        self.router.callback_query(F.data.startswith("help:"))(self.cb_help_section)

    async def cmd_start(self, message: Message, state: FSMContext):
        await state.clear()
        await self.db.add_user(message.from_user.id,
                               message.from_user.username or "",
                               message.from_user.first_name or "")
        user = await self.db.get_user(message.from_user.id)
        role = "client"
        if user and user.get("role") == "executor":
            ex = await self.db.get_executor(message.from_user.id)
            if ex and ex["status"] == "active":
                role = "executor"
        await message.answer(
            f"👋 Привет, <b>{message.from_user.first_name}</b>!\n\n"
            "Это <b>SHONEX Market</b> — маркетплейс услуг.\n"
            f"💰 Курс: 1 ⭐ = {RATE_STAR_TO_RUB} ₽\n\n"
            "Выбери действие:",
            reply_markup=Keyboards.main_menu(role)
        )

    async def cmd_cancel(self, message: Message, state: FSMContext):
        await state.clear()
        await message.answer("❌ Действие отменено.", reply_markup=Keyboards.main_menu())

    async def cb_menu(self, call: CallbackQuery, state: FSMContext):
        await state.clear()
        user = await self.db.get_user(call.from_user.id)
        role = "client"
        if user and user.get("role") == "executor":
            ex = await self.db.get_executor(call.from_user.id)
            if ex and ex["status"] == "active":
                role = "executor"
        await call.message.edit_text("🏠 <b>Главное меню</b>", reply_markup=Keyboards.main_menu(role))
        await call.answer()

    # --- Order FSM ---
    async def cb_create_order(self, call: CallbackQuery, state: FSMContext):
        await state.set_state(OrderState.category)
        await call.message.edit_text("📦 <b>Выбери категорию:</b>", reply_markup=Keyboards.categories())
        await call.answer()

    async def cb_category(self, call: CallbackQuery, state: FSMContext):
        await state.update_data(category=call.data.split(":")[1])
        await state.set_state(OrderState.service)
        await call.message.edit_text("✍️ Напиши название услуги (кратко):")
        await call.answer()

    async def order_service(self, message: Message, state: FSMContext):
        await state.update_data(service=message.text.strip()[:100])
        await state.set_state(OrderState.description)
        await message.answer("📝 Опиши задачу подробнее:")

    async def order_description(self, message: Message, state: FSMContext):
        await state.update_data(description=message.text.strip()[:500])
        await state.set_state(OrderState.amount)
        await message.answer(f"💰 Укажи стоимость в рублях (число).\nКурс: 1 ⭐ = {RATE_STAR_TO_RUB} ₽")

    async def order_amount(self, message: Message, state: FSMContext):
        try:
            amount = float(message.text.strip().replace(",", "."))
            if amount <= 0:
                raise ValueError
        except ValueError:
            await message.answer("❌ Введи корректную сумму, например: 1500")
            return

        amount_stars = int(Decimal(str(amount)) / RATE_STAR_TO_RUB)
        if amount_stars > MAX_STARS_PER_INVOICE:
            await message.answer(f"❌ Сумма превышает лимит Stars ({MAX_STARS_PER_INVOICE} ⭐ = {MAX_STARS_PER_INVOICE * RATE_STAR_TO_RUB} ₽).")
            return

        data = await state.get_data()
        commission = int(amount_stars * DEFAULT_COMMISSION)
        executor_reward = amount_stars - commission

        order_id = await self.db.create_order(
            client_id=message.from_user.id,
            category=data["category"],
            service=data["service"],
            description=data["description"],
            amount_stars=amount_stars,
            commission_stars=commission,
            executor_stars=executor_reward,
        )
        order = await self.db.get_order(order_id)
        await state.clear()

        # Создаём инвойс (Escrow)
        await self.bot.send_invoice(
            chat_id=message.chat.id,
            title=f"Заказ #{order_id}",
            description=f"{order['service']} | {order['description'][:100]}",
            payload=f"order:{order_id}",
            provider_token="",
            currency="XTR",
            prices=[LabeledPrice(label="Оплата заказа", amount=amount_stars)],
        )
        await message.answer(
            f"📦 <b>Заказ #{order_id} создан</b>\n\n"
            f"Категория: {order['category']}\n"
            f"Услуга: {order['service']}\n"
            f"Сумма: {order['amount_stars']} ⭐ ({amount} ₽)\n\n"
            "Оплати, чтобы мы начали искать исполнителя."
        )

    # --- Balance & Orders ---
    async def cb_balance(self, call: CallbackQuery):
        balance = await self.db.get_balance(call.from_user.id)
        await call.message.edit_text(
            f"⭐ <b>Твой баланс</b>\n\n"
            f"💰 Доступно: <b>{balance['available']}</b> ⭐\n"
            f"🔒 В заказах: <b>{balance['frozen']}</b> ⭐",
            reply_markup=Keyboards.main_menu()
        )
        await call.answer()

    async def cb_my_orders(self, call: CallbackQuery):
        orders = await self.db.list_client_orders(call.from_user.id)
        if not orders:
            await call.message.edit_text("📋 У тебя пока нет заказов.", reply_markup=Keyboards.main_menu())
            await call.answer()
            return
        text = "📋 <b>Твои заказы:</b>\n\n"
        for o in orders:
            emoji = {
                "pending_payment": "⏳", "pending_executor": "🔍",
                "in_progress": "🟣", "pending_client": "🟠",
                "completed": "✅", "disputed": "🔴",
            }.get(o["status"], "❓")
            text += f"{emoji} #{o['id']} | {o['service']} | {o['amount_stars']} ⭐ | {o['status']}\n"
        await call.message.edit_text(text, reply_markup=Keyboards.main_menu())
        await call.answer()

    # --- Client Confirm / Dispute ---
    async def cb_client_confirm(self, call: CallbackQuery, state: FSMContext):
        order_id = int(call.data.split(":")[2])
        order = await self.db.get_order(order_id)
        if not order or order["client_id"] != call.from_user.id:
            await call.answer("Нет доступа", show_alert=True)
            return
        if order["status"] != "pending_client":
            await call.answer("Заказ не в статусе подтверждения", show_alert=True)
            return
        
        success = await self.db.release_frozen(
            client_id=order["client_id"],
            executor_id=order["executor_id"],
            amount=order["amount_stars"],
            executor_reward=order["executor_stars"],
        )
        if not success:
            await call.answer("Ошибка: заказ уже подтверждён", show_alert=True)
            return

        await self.db.set_order_status(order_id, "completed")
        await self.db.log_transaction(order["executor_id"], order_id,
                                      order["executor_stars"], "payout", "completed")
        await call.message.edit_text(
            f"✅ Заказ #{order_id} подтверждён. Исполнитель получил {order['executor_stars']} ⭐."
        )
        try:
            await self.bot.send_message(order["executor_id"],
                                        f"💰 Тебе начислено {order['executor_stars']} ⭐ за заказ #{order_id}!")
        except Exception:
            pass
        await call.answer()
        await state.set_state(ReviewState.waiting)
        await state.update_data(review_order=order_id, review_executor=order["executor_id"])
        await call.message.answer("⭐ Оцени исполнителя от 1 до 5 (просто напиши число):")

    async def cb_client_dispute(self, call: CallbackQuery):
        order_id = int(call.data.split(":")[2])
        order = await self.db.get_order(order_id)
        if not order or order["client_id"] != call.from_user.id:
            await call.answer("Нет доступа", show_alert=True)
            return
        await self.db.set_order_status(order_id, "disputed")
        await call.message.edit_text(f"⚠️ Спор по заказу #{order_id} открыт. Админ свяжется с тобой.")
        for admin_id in ADMIN_IDS:
            try:
                await self.bot.send_message(admin_id,
                    f"⚠️ Спор по заказу #{order_id}. Клиент: {order['client_id']}, "
                    f"исполнитель: {order['executor_id']}")
            except Exception:
                pass
        await call.answer()

    # --- Review ---
    async def review_input(self, message: Message, state: FSMContext):
        data = await state.get_data()
        try:
            rating = int(message.text.strip())
            if rating < 1 or rating > 5:
                raise ValueError
        except ValueError:
            await message.answer("❌ Введи число от 1 до 5")
            return
        await self.db.add_review(
            order_id=data["review_order"],
            executor_id=data["review_executor"],
            client_id=message.from_user.id,
            rating=rating,
        )
        await state.clear()
        await message.answer(f"✅ Спасибо за оценку {rating} ⭐!", reply_markup=Keyboards.main_menu())

    # --- Executor Application ---
    async def cb_executor_apply(self, call: CallbackQuery, state: FSMContext):
        await state.set_state(ExecutorState.name)
        await call.message.edit_text("👨‍💻 <b>Заявка исполнителя</b>\n\nШаг 1/4 — как тебя звать (псевдоним):\n\n/cancel — отменить")
        await call.answer()

    async def executor_name(self, message: Message, state: FSMContext):
        await state.update_data(name=message.text.strip()[:50])
        await state.set_state(ExecutorState.specialization)
        await message.answer("🎯 Шаг 2/4 — специализация (например: Python, боты, дизайн):")

    async def executor_specialization(self, message: Message, state: FSMContext):
        await state.update_data(spec=message.text.strip()[:100])
        await state.set_state(ExecutorState.experience)
        await message.answer("📝 Шаг 3/4 — опыт работы (кратко):")

    async def executor_experience(self, message: Message, state: FSMContext):
        await state.update_data(exp=message.text.strip()[:200])
        await state.set_state(ExecutorState.portfolio)
        await message.answer("🔗 Шаг 4/4 — портфолио (ссылки, примеры):")

    async def executor_portfolio(self, message: Message, state: FSMContext):
        data = await state.get_data()
        await self.db.create_executor_application(
            uid=message.from_user.id,
            display_name=data["name"],
            specialization=data["spec"],
            experience=data["exp"],
            portfolio=message.text.strip()[:300],
        )
        await state.clear()
        await message.answer("✅ Заявка отправлена на модерацию. Ожидай одобрения.",
                             reply_markup=Keyboards.main_menu())
        for admin_id in ADMIN_IDS:
            try:
                await self.bot.send_message(
                    admin_id,
                    f"👨‍💻 <b>Новая заявка исполнителя</b>\n\n"
                    f"ID: <code>{message.from_user.id}</code>\n"
                    f"Имя: {data['name']}\n"
                    f"Спец: {data['spec']}\n"
                    f"Опыт: {data['exp']}\n"
                    f"Портфолио: {message.text.strip()}",
                    reply_markup=Keyboards.executor_moderation(message.from_user.id)
                )
            except Exception:
                pass

    async def cb_executor_available(self, call: CallbackQuery):
        ex = await self.db.get_executor(call.from_user.id)
        if not ex or ex["status"] != "active":
            await call.answer("Ты не активный исполнитель", show_alert=True)
            return
        orders = await self.db.list_available_orders()
        if not orders:
            await call.message.edit_text("📦 Свободных заказов нет.",
                                         reply_markup=Keyboards.main_menu("executor"))
            await call.answer()
            return
        await call.message.edit_text(
            "📦 <b>Свободные заказы:</b>\n\nВыбери заказ:",
            reply_markup=Keyboards.available_orders(orders)
        )
        await call.answer()

    async def cb_executor_take(self, call: CallbackQuery):
        order_id = int(call.data.split(":")[2])
        order = await self.db.get_order(order_id)
        if not order or order["status"] != "pending_executor":
            await call.answer("Заказ уже занят", show_alert=True)
            return
        await self.db.assign_executor(order_id, call.from_user.id)
        await call.message.edit_text(
            f"✅ Ты взял заказ #{order_id}.\n"
            f"Клиент: <code>{order['client_id']}</code>\n"
            f"Задача: {order['description']}\n\n"
            "Когда выполнишь — отправь результат.",
            reply_markup=Keyboards.in_progress(order_id),
        )
        try:
            await self.bot.send_message(order["client_id"],
                                        f"🟣 Исполнитель взял заказ #{order_id} в работу.")
        except Exception:
            pass
        await call.answer()

    async def cb_executor_submit(self, call: CallbackQuery, state: FSMContext):
        order_id = int(call.data.split(":")[2])
        order = await self.db.get_order(order_id)
        if not order or order["executor_id"] != call.from_user.id:
            await call.answer("Нет доступа", show_alert=True)
            return
        await state.set_state(OrderState.description)  # reuse для результата
        await state.update_data(submit_order=order_id)
        await call.message.edit_text("📤 Отправь результат (текст, ссылку или описание):\n\n/cancel — отменить")
        await call.answer()

    async def cb_executor_profile(self, call: CallbackQuery):
        ex = await self.db.get_executor(call.from_user.id)
        if not ex:
            await call.answer("Ты не исполнитель", show_alert=True)
            return
        text = (
            f"👤 <b>Профиль</b>\n\n"
            f"Имя: {ex['display_name']}\n"
            f"Спец: {ex['specialization']}\n"
            f"Рейтинг: ⭐ {ex['rating']}\n"
            f"Выполнено: {ex['completed']}\n"
            f"Отменено: {ex['cancelled']}"
        )
        await call.message.edit_text(text, reply_markup=Keyboards.main_menu("executor"))
        await call.answer()

    # --- Help ---
    async def cb_help(self, call: CallbackQuery):
        await call.message.edit_text(
            "🆘 <b>Помощь</b>\n\nВыбери раздел:",
            reply_markup=Keyboards.help_menu()
        )
        await call.answer()

    async def cb_help_section(self, call: CallbackQuery):
        section = call.data.split(":")[1]
        texts = {
            "how": "📖 <b>Как это работает</b>\n\n1. Создаёшь заказ\n2. Исполнитель берёт его\n3. Выполняет и отправляет результат\n4. Ты подтверждаешь — он получает ⭐",
            "faq": "❓ <b>FAQ</b>\n\n• Что если исполнитель не выполнил? — Открой спор\n• Сколько берёт комиссия? — 20%",
            "support": "🆘 <b>Поддержка</b>\n\nНапиши админу: @your_admin_username"
        }
        await call.message.edit_text(texts.get(section, "Раздел в разработке"), reply_markup=Keyboards.help_menu())
        await call.answer()

    # --- Обработка текста результата (нужно добавить в _register) ---
    async def executor_submit_text(self, message: Message, state: FSMContext):
        data = await state.get_data()
        order_id = data.get("submit_order")
        if not order_id:
            return
        await self.db.submit_result(order_id, message.text.strip())
        order = await self.db.get_order(order_id)
        await state.clear()
        await message.answer("✅ Результат отправлен клиенту на проверку.")
        try:
            await self.bot.send_message(
                order["client_id"],
                f"📬 Исполнитель сдал работу по заказу #{order_id}.\n\n"
                f"Результат: {message.text[:300]}\n\n"
                "Подтверди выполнение или открой спор.",
                reply_markup=Keyboards.client_decision(order_id)
            )
        except Exception:
            pass

class AdminHandlers(BaseHandler):
    """Админские обработчики."""

    def __init__(self, db: Database, bot: Bot, router: Router):
        super().__init__(db, bot)
        self.router = router
        self._register()

    def _register(self):
        self.router.message(Command("admin"))(self.cmd_admin)
        self.router.callback_query(F.data == "admin:pending")(self.cb_pending)
        self.router.callback_query(F.data.startswith("admin:approve:"))(self.cb_approve)
        self.router.callback_query(F.data.startswith("admin:reject:"))(self.cb_reject)

    async def cmd_admin(self, message: Message):
        if message.from_user.id not in ADMIN_IDS:
            return
        pending = await self.db.list_pending_executors()
        await message.answer(
            f"🔐 <b>Админ-панель</b>\n\nЗаявок на модерацию: {len(pending)}",
            reply_markup=Keyboards.admin_menu()
        )

    async def cb_pending(self, call: CallbackQuery):
        if call.from_user.id not in ADMIN_IDS:
            await call.answer("⛔ Нет доступа", show_alert=True)
            return
        pending = await self.db.list_pending_executors()
        if not pending:
            await call.message.edit_text("📋 Нет заявок на модерацию.",
                                         reply_markup=Keyboards.admin_menu())
            await call.answer()
            return
        text = "📋 <b>Заявки исполнителей:</b>\n\n"
        for p in pending:
            text += f"ID: <code>{p['user_id']}</code> | {p['display_name']}\n"
        await call.message.edit_text(text, reply_markup=Keyboards.admin_menu())
        await call.answer()

    async def cb_approve(self, call: CallbackQuery):
        if call.from_user.id not in ADMIN_IDS:
            await call.answer("⛔ Нет доступа", show_alert=True)
            return
        uid = int(call.data.split(":")[2])
        await self.db.set_executor_status(uid, "active")
        await call.message.edit_text(f"✅ Исполнитель <code>{uid}</code> <b>одобрен</b>.")
        try:
            await self.bot.send_message(uid, "✅ Твоя заявка одобрена! Теперь ты можешь брать заказы.")
        except Exception:
            pass
        await call.answer("Одобрено")

    async def cb_reject(self, call: CallbackQuery):
        if call.from_user.id not in ADMIN_IDS:
            await call.answer("⛔ Нет доступа", show_alert=True)
            return
        uid = int(call.data.split(":")[2])
        await self.db.set_executor_status(uid, "rejected")
        await call.message.edit_text(f"❌ Исполнитель <code>{uid}</code> <b>отклонён</b>.")
        try:
            await self.bot.send_message(uid, "❌ Твоя заявка отклонена. Попробуй позже.")
        except Exception:
            pass
        await call.answer("Отклонено")

    @staticmethod
    def admin_menu() -> InlineKeyboardBuilder.as_markup:
        builder = InlineKeyboardBuilder()
        builder.button(text="📋 Заявки исполнителей", callback_data="admin:pending")
        builder.button(text="⬅️ Назад", callback_data="menu")
        builder.adjust(1)
        return builder.as_markup()

# ============================================
# ОРКЕСТРАТОР (Application Layer)
# ============================================

class ShonexBot:
    """Главный класс приложения. Собирает всё вместе."""

    def __init__(self):
        self.bot = Bot(
            token=BOT_TOKEN,
            default=DefaultBotProperties(parse_mode=ParseMode.HTML)
        )
        self.db = Database(SUPABASE_DB_URL)
        self.dp = Dispatcher()

        self.main_router = Router()
        self.admin_router = Router()

        # Порядок важен: админский роутер первым
        self.dp.include_router(self.admin_router)
        self.dp.include_router(self.main_router)

        self._init_handlers()

    def _init_handlers(self):
        self.main_handlers = MainHandlers(self.db, self.bot, self.main_router)
        self.admin_handlers = AdminHandlers(self.db, self.bot, self.admin_router)

        # Payment handlers
        self.main_router.pre_checkout_query()(self.pre_checkout)
        self.main_router.message(F.successful_payment)(self.on_payment)

        # Обработка текста результата исполнителя
        self.main_router.message(OrderState.description, F.text)(self.main_handlers.executor_submit_text)

    async def pre_checkout(self, query: PreCheckoutQuery):
        await query.answer(ok=True)

    async def on_payment(self, message: Message):
        payload = message.successful_payment.invoice_payload
        if not payload.startswith("order:"):
            return
        order_id = int(payload.split(":")[1])
        order = await self.db.get_order(order_id)
        if not order or order["client_id"] != message.from_user.id:
            log.warning(f"⚠️ Неверный платёж от {message.from_user.id} для заказа {order_id}")
            return
        await self.db.freeze_balance(order["client_id"], order["amount_stars"])
        await self.db.set_order_status(order_id, "pending_executor")
        await self.db.log_transaction(order["client_id"], order_id,
                                      order["amount_stars"], "payment", "frozen")
        await message.answer(f"✅ Заказ #{order_id} оплачен. Ищем исполнителя...")
        for admin_id in ADMIN_IDS:
            try:
                await self.bot.send_message(admin_id,
                    f"🔔 Новый заказ #{order_id} на {order['amount_stars']} ⭐")
            except Exception:
                pass

    async def startup(self):
        await self.db.connect()
        await self.db.init_schema()
        await self.bot.delete_webhook(drop_pending_updates=True)
        await asyncio.sleep(1)
        await self.bot.set_webhook(
            url=f"{WEBHOOK_URL}/webhook",
            secret_token=WEBHOOK_SECRET,
            allowed_updates=["message", "callback_query",
                             "pre_checkout_query", "successful_payment"],
        )
        log.info(f"✅ Webhook: {WEBHOOK_URL}/webhook")
        log.info(f"👑 Admins: {ADMIN_IDS}")

    async def shutdown(self):
        await self.db.close()
        await self.bot.session.close()
        log.info("🛑 Bot stopped")

    async def handle_update(self, update: Update):
        await self.dp.feed_update(self.bot, update)

# ============================================
# FASTAPI + WEBHOOK
# ============================================

shonex = ShonexBot()

@asynccontextmanager
async def lifespan(app: FastAPI):
    await shonex.startup()
    yield
    await shonex.shutdown()

app = FastAPI(title="SHONEX Ultra OOP", lifespan=lifespan)

@app.post("/webhook")
async def webhook(request: Request):
    if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        return Response(status_code=403)
    try:
        data = await request.json()
        update = Update.model_validate(data, context={"bot": shonex.bot})
        await shonex.handle_update(update)
    except Exception as e:
        log.error(f"Webhook error: {e}")
    return Response(content="OK", status_code=200)

@app.get("/health")
async def health():
    return {"status": "ok"}

# ============================================
# ЗАПУСК
# ============================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000))) 
