import asyncio
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.utils.exceptions import MessageNotModified
from aiogram.utils.exceptions import TelegramAPIError
from telethon import TelegramClient, events
import re
import sys
import time
import os
from datetime import datetime, timezone
from binance.client import Client
from binance.exceptions import BinanceAPIException
import math
import traceback
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage
import websockets
import json
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from decimal import Decimal, ROUND_DOWN
from aiogram.utils.markdown import escape_md
from collections import deque
from aiogram import filters
import base64
import aiohttp

class MultipleTradesState(StatesGroup):
    waiting_for_setting_choice = State()  # اختيار الإعداد المراد تعديله
    waiting_for_fixed_amount = State()  # إدخال المبلغ الثابت لكل صفقة
    waiting_for_max_concurrent_trades = State()
    waiting_for_exit_target = State()
    # حالات الصفقة اليدوية
    waiting_for_manual_symbol = State()
    waiting_for_manual_entry = State()
    waiting_for_manual_targets = State()
    waiting_for_manual_sl = State()
    waiting_for_manual_amount = State()
    waiting_for_user_id_to_add = State()
    waiting_for_user_id_to_remove = State()

class StopLossState(StatesGroup):
    waiting_for_percentage = State()

# إنشاء التخزين وتمريره للـ Dispatcher
storage = MemoryStorage()

# إعداد اللوغ العام
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# إسكات رسائل Telethon الخاصة بـ client.updates
logging.getLogger("telethon.client.updates").setLevel(logging.WARNING)

# Import configuration testnet
TESTNET_API_KEY = "Dx2n5FiMTIsN6CGrR6F76w1zO5UtwUW1sy8E6kTWmXEjbIWPm09pSmssnT6xdxce"
TESTNET_API_SECRET = "G9DOrsOgA9bLk7PSoNBP030bUCBLNnC7CoHDyxNBKQaoS62qpcQYwNPP5lsUdtTs"

# Import configuration
API_KEY = 'Yi9jU1WYSARVyKuvPno4NwQrZZr36S8i1i5p7VAiVdHiU3Ed659lj6bPqlpBkudb'
API_SECRET = '531AlsA7D4FdqXkQ2LoBD3pH8jSVdjnj4U6RosAcjMBEfl5UfTGKIETu05xVycfx'
TELEGRAM_BOT_TOKEN = '8333598028:AAHiSiw9yvth1ZBbMAn6r5Rr3wf7-v-czXI'

# --- بداية إدارة المستخدمين من ملف ---
ADMIN_USER_IDS_LIST = [1195443662]

# أضف هذا المتغير العام الجديد في بداية الكود
viewers_dict = {}

HALAL_SYMBOLS_LIST_FILE = "binance_halal.txt"  # اسم الملف
halal_symbols_set = set() # استخدام set للبحث السريع

# قائمة بأسماء البوتات التي تريد الاستماع إليها
#BOT_USERNAMES = ["SignalBOTM11","testitdz"]

SIGNAL_SOURCES = {
    '5min_frame': "CryptoSignalProo",
    'both_channels': [-1002195590881, -1002540670158],
    'buy_signals': ["MDBORRSA", -1002040583721],
    'novo_signals': ['scalpbitcoinfree']
}

SYMBOL_INFO_CACHE_FILE = "symbol_info_cache.json"

# الآن نستخدم قائمة لتخزين المصادر النشطة
ACTIVE_BOT_USERNAME = SIGNAL_SOURCES['5min_frame']
logger.info(f"Default signal source set to: {ACTIVE_BOT_USERNAME}")

use_testnet = True
client = None
# Binance API and Telegram settings
#client = Client(TESTNET_API_KEY, TESTNET_API_SECRET, testnet=use_testnet)
bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher(bot, storage=storage)

# متغيرات التحكم
bot_active = False
total_trades = 0
successful_trades = 0
failed_trades = 0
pending_trades = 0
active_trades = {}
trade_lock = asyncio.Lock()
# --- ✅ قفل لحماية symbol_info_cache من السباقات ---
symbol_info_lock = asyncio.Lock()
# ---
account_switch_lock = asyncio.Lock()
net_profit_usdt = 0.0

# تعريف المتغيرات العالمية
fixed_trade_amount = 10.0  # المبلغ الثابت لكل صفقة (لوضع "عدة صفقات")
max_concurrent_trades = 1  # الحد الأقصى للصفقات النشطة
exit_at_target_number = 3  # يمكن أن تكون القيمة 'auto', 1, 2, or 3
MAX_INFINITE_TARGETS = 10

# قاموس لتخزين العملة ووقت التعرف عليها
currency_last_seen = {}

# قاموس لتخزين نتائج عرض الأصول مؤقتًا
assets_display_cache = {}

stop_loss_mode = "auto"  # الوضع الافتراضي تلقائي
custom_stop_loss_percentage = None  # في حالة إدخال نسبة مخصصة

# إعداد Telethon لمراقبة القناة
telethon_api_id = 7609640
telethon_api_hash = '7aaf74745eda7b5680a44680f2869259'
client_telegram = TelegramClient('session_name2', telethon_api_id, telethon_api_hash)

# قاموس لتخزين الأسعار الحية
live_prices = {}
active_symbols = set()  # مجموعة الرموز قيد المراقبة
websocket_manager = None  # لتخزين مثيل WebSocket

# نستخدم deque بدلاً من list لتحسين الأداء عند إضافة وحذف العناصر
trade_history = deque(maxlen=200) 

symbol_info_cache = {}

SIGNAL_IGNORE_WINDOW_SECONDS = 8 * 60 * 60

# --- متغيرات وملف إحصائيات العملات ---
STATS_FILE = "bot_stats.json"
ACTIVE_TRADES_FILE = "active_trades.json"
currency_performance = {}
use_conditional_sl = True
trailing_sl_is_conditional = False # القيمة الافتراضية هي "معطل"

async def parse_novo_signal(message_text: str):
    """
    تحلل إشارات NOVO SINAL البرازيلية
    """
    global max_concurrent_trades, active_trades, trade_lock, currency_last_seen, stop_loss_mode, custom_stop_loss_percentage, logger, use_conditional_sl, account_switch_lock

    logger.info(f"--- Parsing NOVO Signal ---")
    logger.debug(f"Received message snippet: {message_text[:150]}")
    
    async with account_switch_lock:
        try:
            # فحص إذا كانت الرسالة تحتوي على إشارة NOVO
            if "NOVO SINAL" not in message_text or "Compra:" not in message_text:
                logger.debug("Message does not contain NOVO SINAL. Ignoring.")
                return None

            # التحقق من الحد الأقصى للصفقات النشطة
            async with trade_lock:
                active_trade_count = len(active_trades)
                if active_trade_count >= max_concurrent_trades:
                    log_msg = f"Cannot open new trade. Max concurrent trades ({max_concurrent_trades}) reached. Active: {active_trade_count}."
                    user_msg = f"⚠️ تم الوصول للحد الأقصى للصفقات النشطة ({max_concurrent_trades}). لا يمكن فتح صفقة جديدة حاليًا."
                    if max_concurrent_trades == 1:
                        user_msg = "⛔ لا يمكن فتح صفقة جديدة. البوت مضبوط على صفقة واحدة نشطة كحد أقصى، ويوجد بالفعل صفقة نشطة."
                    logger.info(log_msg)
                    await send_result_to_users(user_msg)
                    return None

            try:
                # استخراج اسم العملة من العنوان
                symbol_match = re.search(r'#([A-Z0-9]+)', message_text)
                if not symbol_match:
                    logger.warning("Could not extract symbol from NOVO signal")
                    return None
                
                symbol_raw = symbol_match.group(1).strip().upper()
                
                # تحويل إلى تنسيق موحد
                if symbol_raw.endswith('USDT'):
                    base_asset = symbol_raw[:-4]
                    symbol_formatted = f"{base_asset}/USDT"
                else:
                    # إضافة USDT إذا لم يكن موجود
                    base_asset = symbol_raw
                    symbol_formatted = f"{base_asset}/USDT"
                    symbol_raw = f"{base_asset}USDT"

                logger.info(f"NOVO signal detected for {symbol_formatted}")

                # --- التحقق من العملة الحلال ---
                if halal_symbols_set:
                    if base_asset not in halal_symbols_set:
                        log_msg = f"NOVO signal for {symbol_formatted} (Base: {base_asset}) is NOT in the halal symbols list. Ignoring signal."
                        logger.info(log_msg)
                        return None
                    else:
                        logger.info(f"NOVO signal for {symbol_formatted} (Base: {base_asset}) is in the halal symbols list. Proceeding.")

                # التحقق من وجود صفقة نشطة لنفس الرمز
                async with trade_lock:
                    if symbol_formatted in active_trades:
                        log_msg = f"Active trade already exists for {symbol_formatted}. Ignoring new NOVO signal."
                        logger.info(log_msg)
                        await send_result_to_users(f"⛔ يوجد صفقة نشطة بالفعل للرمز {symbol_formatted}. تم تجاهل إشارة NOVO الجديدة.")
                        return None

                # استخراج سعر الشراء
                entry_match = re.search(r'Compra:\s*([0-9]*\.?[0-9]+)', message_text)
                if not entry_match:
                    logger.warning(f"Could not extract entry price from NOVO signal for {symbol_formatted}")
                    return None
                
                entry_price_from_signal = float(entry_match.group(1))

                # استخراج الأهداف
                targets_from_signal = []
                target_patterns = [
                    r'Alvo 1:\s*([0-9]*\.?[0-9]+)',
                    r'Alvo 2:\s*([0-9]*\.?[0-9]+)', 
                    r'Alvo 3:\s*([0-9]*\.?[0-9]+)'
                ]
                
                for pattern in target_patterns:
                    target_match = re.search(pattern, message_text)
                    if target_match:
                        targets_from_signal.append(float(target_match.group(1)))

                # استخراج وقف الخسارة
                sl_match = re.search(r'StopLoss:\s*([0-9]*\.?[0-9]+)', message_text)
                initial_sl_to_use = None
                if sl_match:
                    initial_sl_to_use = float(sl_match.group(1))

                # التحقق من صحة البيانات المستخرجة
                if not targets_from_signal:
                    logger.warning(f"No targets found in NOVO signal for {symbol_formatted}")
                    return None

                if len(targets_from_signal) < 2:
                    logger.warning(f"Insufficient targets ({len(targets_from_signal)}) in NOVO signal for {symbol_formatted}")
                    return None

                # التحقق من صحة الأهداف (يجب أن تكون أعلى من سعر الدخول)
                if any(tp <= entry_price_from_signal for tp in targets_from_signal):
                    logger.warning(f"Invalid targets in NOVO signal for {symbol_formatted}. Some targets are below entry price.")
                    return None

                # الحصول على السعر الحالي للمقارنة
                current_price = live_prices.get(symbol_raw) or await get_current_price(symbol_raw)
                if not current_price:
                    logger.error(f"Could not get current price for {symbol_formatted}")
                    await send_result_to_users(f"⚠️ تعذر الحصول على السعر الحالي لـ {symbol_formatted}")
                    return None

                # فحص انحراف السعر (مرونة 2% للإشارات البرازيلية)
                price_deviation = abs(current_price - entry_price_from_signal) / entry_price_from_signal
                max_deviation = 0.02  # 2%
                
                if price_deviation > max_deviation:
                    min_price = entry_price_from_signal * (1 - max_deviation)
                    max_price = entry_price_from_signal * (1 + max_deviation)
                    logger.warning(f"❌ تم تجاهل إشارة NOVO {symbol_formatted} بسبب انحراف السعر الحالي ({current_price}) خارج النطاق المسموح به [{min_price:.8f} - {max_price:.8f}]")
                    await send_result_to_users(f"❌ تم تجاهل إشارة NOVO {symbol_formatted}\nالسعر الحالي `{current_price:.4f}` خارج نطاق `{entry_price_from_signal:.4f} ± {max_deviation*100:.1f}%`\nلذلك، لم يتم فتح الصفقة تفاديًا للدخول المتأخر أو العشوائي.")
                    return None

                # تطبيق إعدادات وقف الخسارة
                sl_source_info = ""
                if stop_loss_mode == "auto":
                    if initial_sl_to_use:
                        sl_source_info = "من الإشارة البرازيلية (تلقائي)"
                        logger.info(f"AUTO mode: Using SL {initial_sl_to_use} from NOVO signal for {symbol_formatted}.")
                    else:
                        initial_sl_to_use = None
                        sl_source_info = "لا يوجد (تلقائي)"
                        logger.warning(f"AUTO mode: No SL in NOVO signal for {symbol_formatted}.")
                
                elif stop_loss_mode == "custom":
                    if custom_stop_loss_percentage is not None:
                        initial_sl_to_use = entry_price_from_signal * (1 - (custom_stop_loss_percentage / 100))
                        sl_source_info = f"محسوب {custom_stop_loss_percentage}% (مخصص)"
                        logger.info(f"CUSTOM mode: Calculated SL {initial_sl_to_use} for NOVO signal {symbol_formatted}.")
                    else:
                        default_custom_perc = 3.0
                        initial_sl_to_use = entry_price_from_signal * (1 - (default_custom_perc / 100))
                        sl_source_info = f"محسوب {default_custom_perc}% (مخصص افتراضي)"
                        logger.warning(f"CUSTOM mode: No custom % for NOVO signal {symbol_formatted}. Defaulting to {default_custom_perc}% SL: {initial_sl_to_use}.")
                
                elif stop_loss_mode == "disabled":
                    initial_sl_to_use = None
                    sl_source_info = "معطل من الإعدادات"
                    logger.info(f"DISABLED mode: No SL for NOVO signal {symbol_formatted}.")

                # التحقق النهائي من وقف الخسارة
                if initial_sl_to_use is not None and initial_sl_to_use >= entry_price_from_signal:
                    log_msg = f"Final SL ({initial_sl_to_use}) for NOVO signal {symbol_formatted} is not below entry ({entry_price_from_signal}). Ignoring."
                    logger.warning(log_msg)
                    await send_result_to_users(f"⚠️ وقف الخسارة المحدد ({initial_sl_to_use:.8f}) لـ {symbol_formatted} غير صالح. تم تجاهل الإشارة.")
                    return None

                # القرار حول الشرطية
                is_4h_conditional_sl = use_conditional_sl 
                sl_display_text = f"{initial_sl_to_use:.8f}" if initial_sl_to_use is not None else "لا يوجد"
                conditional_sl_text = " (مشروط بـ 4 ساعات)" if is_4h_conditional_sl and initial_sl_to_use is not None else ""
                
                log_final_info = f"Processing valid NOVO signal: {symbol_formatted}, Entry: {entry_price_from_signal}, SL: {sl_display_text}{conditional_sl_text}, Targets: {targets_from_signal}, SL Source: {sl_source_info}"
                logger.info(log_final_info)

                # تمرير المعلومات لفتح الصفقة
                await place_trade_orders(
                    symbol_key=symbol_formatted,
                    entry_price=entry_price_from_signal,
                    targets=targets_from_signal[:3],  # أخذ أول 3 أهداف فقط
                    stop_loss_price=initial_sl_to_use,
                    amount_usdt=fixed_trade_amount,
                    is_4h_conditional=is_4h_conditional_sl
                )

            except Exception as e:
                logger.error(f"MAIN TRY-EXCEPT in parse_novo_signal: {e}", exc_info=True)
                await send_result_to_users(f"⚠️ خطأ عام فادح في تحليل إشارة NOVO: {str(e)}")
                return None
                
        except Exception as e:
            logger.error(f"!!! CRITICAL UNHANDLED EXCEPTION in parse_novo_signal !!!: {e}", exc_info=True)
            await send_result_to_users(f"🚨 حدث خطأ فادح أثناء تحليل إشارة NOVO. يرجى مراجعة السجلات.")
            return None

async def parse_buy_signal(message_text: str, photo_data: bytes = None):
   """
   تحلل إشارات BUY مع أو بدون صورة
   """
   global max_concurrent_trades, active_trades, trade_lock, currency_last_seen, stop_loss_mode, custom_stop_loss_percentage, logger, use_conditional_sl, account_switch_lock

   logger.info(f"--- Parsing BUY Signal ---")
   logger.debug(f"Received message snippet: {message_text[:150]}")
   
   async with account_switch_lock:
       try:
           # فحص إذا كانت الرسالة تحتوي على إشارة BUY
           buy_match = re.search(r"BUY\s+([A-Z0-9]+)", message_text, re.IGNORECASE)
           if not buy_match:
               logger.debug("Message does not contain BUY signal. Ignoring.")
               return None

           # التحقق من الحد الأقصى للصفقات النشطة
           async with trade_lock:
               active_trade_count = len(active_trades)
               if active_trade_count >= max_concurrent_trades:
                   log_msg = f"Cannot open new trade. Max concurrent trades ({max_concurrent_trades}) reached. Active: {active_trade_count}."
                   user_msg = f"⚠️ تم الوصول للحد الأقصى للصفقات النشطة ({max_concurrent_trades}). لا يمكن فتح صفقة جديدة حاليًا."
                   if max_concurrent_trades == 1:
                       user_msg = "⛔ لا يمكن فتح صفقة جديدة. البوت مضبوط على صفقة واحدة نشطة كحد أقصى، ويوجد بالفعل صفقة نشطة."
                   logger.info(log_msg)
                   await send_result_to_users(user_msg)
                   return None

           try:
               # استخراج اسم العملة
               symbol_raw = buy_match.group(1).strip().upper()
               
               # تحويل إلى تنسيق موحد
               if symbol_raw.endswith('USDT'):
                   base_asset = symbol_raw[:-4]
                   symbol_formatted = f"{base_asset}/USDT"
               else:
                   base_asset = symbol_raw
                   symbol_formatted = f"{base_asset}/USDT"
                   symbol_raw = f"{base_asset}USDT"

               # --- التحقق من العملة الحلال ---
               if halal_symbols_set:
                   if base_asset not in halal_symbols_set:
                       log_msg = f"BUY signal for {symbol_formatted} (Base: {base_asset}) is NOT in the halal symbols list. Ignoring signal."
                       logger.info(log_msg)
                       return None
                   else:
                       logger.info(f"BUY signal for {symbol_formatted} (Base: {base_asset}) is in the halal symbols list. Proceeding.")

               # التحقق من وجود صفقة نشطة لنفس الرمز
               async with trade_lock:
                   if symbol_formatted in active_trades:
                       log_msg = f"Active trade already exists for {symbol_formatted}. Ignoring new BUY signal."
                       logger.info(log_msg)
                       await send_result_to_users(f"⛔ يوجد صفقة نشطة بالفعل للرمز {symbol_formatted}. تم تجاهل إشارة BUY الجديدة.")
                       return None

               # الحصول على السعر الحالي
               entry_price_from_signal = live_prices.get(symbol_raw) or await get_current_price(symbol_raw)
               if not entry_price_from_signal:
                   logger.error(f"Could not get current price for {symbol_formatted}")
                   await send_result_to_users(f"⚠️ تعذر الحصول على السعر الحالي لـ {symbol_formatted}")
                   return None

               # تحليل الصورة أو استخدام الأهداف الافتراضية
               targets_from_signal = []
               initial_sl_to_use = None
               
               if photo_data:
                   logger.info(f"BUY signal has photo. Analyzing chart image for {symbol_formatted}...")
                   try:
                       # حفظ الصورة مؤقتاً
                       temp_image_path = f"temp_chart_{int(time.time())}.jpg"
                       with open(temp_image_path, 'wb') as f:
                           f.write(photo_data)
                       
                       # تحليل الصورة
                       analysis_result = await analyze_chart_image(temp_image_path)
                       
                       if analysis_result:
                           targets_from_signal = analysis_result.get('targets', [])
                           initial_sl_to_use = analysis_result.get('stop_loss')
                           chart_entry = analysis_result.get('entry')
                           
                           # التحقق من تطابق سعر الدخول (اختياري)
                           if chart_entry and abs(chart_entry - entry_price_from_signal) / entry_price_from_signal > 0.05:
                               logger.warning(f"Chart entry price {chart_entry} differs significantly from market price {entry_price_from_signal}")
                       
                       # حذف الملف المؤقت
                       import os
                       try:
                           os.remove(temp_image_path)
                       except:
                           pass
                           
                   except Exception as e:
                       logger.error(f"Error analyzing chart image for {symbol_formatted}: {e}")
                       photo_data = None  # سنستخدم الأهداف الافتراضية

               # إذا لم تنجح الصورة أو لا توجد صورة، استخدم الأهداف الافتراضية
               if not targets_from_signal:
                   logger.info(f"Using default targets for BUY signal {symbol_formatted}")
                   targets_from_signal = [
                       entry_price_from_signal * 1.03,  # +3%
                       entry_price_from_signal * 1.05,  # +5%
                       entry_price_from_signal * 1.07   # +7%
                   ]
                   initial_sl_to_use = entry_price_from_signal * 0.97  # -3%

               # التحقق من صحة الأهداف
               if not targets_from_signal or any(tp <= entry_price_from_signal for tp in targets_from_signal):
                   logger.warning(f"Invalid targets for BUY signal {symbol_formatted}. Ignoring.")
                   await send_result_to_users(f"⚠️ أهداف غير صالحة لإشارة BUY {symbol_formatted}. تم تجاهل الإشارة.")
                   return None

               # تطبيق إعدادات وقف الخسارة
               sl_source_info = ""
               if stop_loss_mode == "auto":
                   if initial_sl_to_use:
                       sl_source_info = "من الصورة/افتراضي (تلقائي)"
                       logger.info(f"AUTO mode: Using SL {initial_sl_to_use} for BUY signal {symbol_formatted}.")
                   else:
                       initial_sl_to_use = None
                       sl_source_info = "لا يوجد (تلقائي)"
                       logger.warning(f"AUTO mode: No SL for BUY signal {symbol_formatted}.")
               
               elif stop_loss_mode == "custom":
                   if custom_stop_loss_percentage is not None:
                       initial_sl_to_use = entry_price_from_signal * (1 - (custom_stop_loss_percentage / 100))
                       sl_source_info = f"محسوب {custom_stop_loss_percentage}% (مخصص)"
                       logger.info(f"CUSTOM mode: Calculated SL {initial_sl_to_use} for BUY signal {symbol_formatted}.")
                   else:
                       default_custom_perc = 3.0
                       initial_sl_to_use = entry_price_from_signal * (1 - (default_custom_perc / 100))
                       sl_source_info = f"محسوب {default_custom_perc}% (مخصص افتراضي)"
                       logger.warning(f"CUSTOM mode: No custom % for BUY signal {symbol_formatted}. Defaulting to {default_custom_perc}% SL: {initial_sl_to_use}.")
               
               elif stop_loss_mode == "disabled":
                   initial_sl_to_use = None
                   sl_source_info = "معطل من الإعدادات"
                   logger.info(f"DISABLED mode: No SL for BUY signal {symbol_formatted}.")

               # التحقق النهائي من وقف الخسارة
               if initial_sl_to_use is not None and initial_sl_to_use >= entry_price_from_signal:
                   log_msg = f"Final SL ({initial_sl_to_use}) for BUY signal {symbol_formatted} is not below entry ({entry_price_from_signal}). Ignoring."
                   logger.warning(log_msg)
                   await send_result_to_users(f"⚠️ وقف الخسارة المحدد ({initial_sl_to_use:.8f}) لـ {symbol_formatted} غير صالح. تم تجاهل الإشارة.")
                   return None

               # القرار حول الشرطية
               is_4h_conditional_sl = use_conditional_sl 
               sl_display_text = f"{initial_sl_to_use:.8f}" if initial_sl_to_use is not None else "لا يوجد"
               conditional_sl_text = " (مشروط بـ 4 ساعات)" if is_4h_conditional_sl and initial_sl_to_use is not None else ""
               
               log_final_info = f"Processing valid BUY signal: {symbol_formatted}, Entry: {entry_price_from_signal}, SL: {sl_display_text}{conditional_sl_text}, Targets: {targets_from_signal}, SL Source: {sl_source_info}"
               logger.info(log_final_info)

               # تمرير المعلومات لفتح الصفقة
               await place_trade_orders(
                   symbol_key=symbol_formatted,
                   entry_price=entry_price_from_signal,
                   targets=targets_from_signal[:3],  # أخذ أول 3 أهداف فقط
                   stop_loss_price=initial_sl_to_use,
                   amount_usdt=fixed_trade_amount,
                   is_4h_conditional=is_4h_conditional_sl
               )

           except Exception as e:
               logger.error(f"MAIN TRY-EXCEPT in parse_buy_signal: {e}", exc_info=True)
               await send_result_to_users(f"⚠️ خطأ عام فادح في تحليل إشارة BUY: {str(e)}")
               return None
               
       except Exception as e:
           logger.error(f"!!! CRITICAL UNHANDLED EXCEPTION in parse_buy_signal !!!: {e}", exc_info=True)
           await send_result_to_users(f"🚨 حدث خطأ فادح أثناء تحليل إشارة BUY. يرجى مراجعة السجلات.")
           return None

async def analyze_chart_image(image_path: str):
    """
    تحليل الصورة باستخدام عدة نماذج OpenRouter API
    """
    MODELS = [
        "openai/gpt-4o-2024-11-20",         # النموذج الحالي أولاً
        "x-ai/grok-4",
        "google/gemini-2.5-flash-lite-preview-06-17",
        "meta-llama/llama-guard-4-12b",
        "amazon/nova-lite-v1",
        "openai/gpt-4.1-nano"
    ]
    
    MAX_RETRIES_PER_MODEL = 3  # محاولات لكل نموذج
    
    try:
        import base64
        with open(image_path, "rb") as f:
            image_base64 = base64.b64encode(f.read()).decode("utf-8")
    except Exception as e:
        logger.error(f"Failed to read image file: {e}")
        return None
    
    # تجربة كل نموذج
    for model_index, model_name in enumerate(MODELS):
        logger.info(f"Trying model {model_index + 1}/{len(MODELS)}: {model_name}")
        
        # إعادة المحاولة لكل نموذج
        for retry in range(MAX_RETRIES_PER_MODEL):
            try:
                payload = {
                    "model": model_name,
                    "messages": [
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "text",
                                    "text": """
                                        أنت محلل مالي. لدي صورة لشارت فيها:
                                        - سعر الدخول (بالأزرق)
                                        - 3 أهداف (بالبنفسجي)
                                        - وقف الخسارة (بالأحمر)
                                        📌 أرجع لي فقط نتيجة بصيغة JSON مثل:
                                        {
                                          "entry": 0.2332,
                                          "targets": [0.2418, 0.2498, 0.2567],
                                          "stop_loss": 0.2264
                                        }
                                        بدون شرح أو تعليقات، فقط JSON.
                                        """
                                },
                                {
                                    "type": "image_url",
                                    "image_url": {
                                        "url": f"data:image/jpeg;base64,{image_base64}"
                                    }
                                }
                            ]
                        }
                    ]
                }
                
                headers = {
                    "Authorization": f"Bearer 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",
                    "Content-Type": "application/json"
                }
                
                import aiohttp
                async with aiohttp.ClientSession() as session:
                    async with session.post("https://openrouter.ai/api/v1/chat/completions", headers=headers, json=payload) as response:
                        if response.status == 200:
                            result_data = await response.json()
                            content = result_data["choices"][0]["message"]["content"]
                            
                            logger.info(f"Model {model_name} response (attempt {retry + 1}): {content[:100]}...")
                            
                            cleaned_content = re.sub(r'```json\s*|\s*```', '', content.strip())
                            logger.info(f"Cleaned content: {cleaned_content}")
                            analysis = json.loads(cleaned_content)
                            
                            # التحقق من صحة البيانات
                            if (analysis.get('entry') and 
                                analysis.get('targets') and 
                                len(analysis.get('targets', [])) >= 3 and
                                analysis.get('stop_loss')):
                                
                                logger.info(f"✅ Chart analysis successful with {model_name}: {analysis}")
                                return analysis
                            else:
                                logger.warning(f"Model {model_name} returned incomplete data: {analysis}")
                                
                        else:
                            error_text = await response.text()
                            logger.error(f"Model {model_name} API error {response.status}: {error_text}")
                            
            except json.JSONDecodeError as e:
                logger.error(f"Model {model_name} JSON decode error (attempt {retry + 1}): {e}")
            except Exception as e:
                logger.error(f"Model {model_name} attempt {retry + 1} failed: {e}")
            
            # انتظار قصير بين المحاولات لنفس النموذج
            if retry < MAX_RETRIES_PER_MODEL - 1:
                await asyncio.sleep(1)
        
        # انتظار أطول قبل تجربة النموذج التالي
        if model_index < len(MODELS) - 1:
            logger.info(f"Model {model_name} failed all attempts. Trying next model...")
            await asyncio.sleep(1)
    
    logger.error("❌ All models failed to analyze chart. Using default targets.")
    return None

# --- ✅ دالة توحيد معلومات الرمز (normalize_symbol_info) ---
def normalize_symbol_info(raw_info: dict) -> dict:
    """
    تحول معلومات الرمز الخام (من Binance API) إلى شكل قياسي موحد.
    يضمن أن جميع الدوال تستخدم نفس أسماء الحقول (snake_case).
    """
    if not raw_info:
        return {}
    
    # استخراج الفلاتر
    filters = {f['filterType']: f for f in raw_info.get('filters', [])}
    lot_size_filter = filters.get('LOT_SIZE', {})
    min_notional_filter = filters.get('MIN_NOTIONAL', {})
    price_filter = filters.get('PRICE_FILTER', {})

    # حساب الدقة (precision) من tickSize و stepSize
    def calculate_precision(value: str) -> int:
        if '.' in value:
            return len(value.split('.')[1].rstrip('0'))
        return 0

    price_precision = calculate_precision(price_filter.get('tickSize', '0.00000001'))
    quantity_precision = calculate_precision(lot_size_filter.get('stepSize', '0.00000001'))

    return {
        'symbol': raw_info.get('symbol'),
        'status': raw_info.get('status', 'UNKNOWN'),
        'baseAsset': raw_info.get('baseAsset'),
        'quoteAsset': raw_info.get('quoteAsset'),
        'min_notional': float(min_notional_filter.get('minNotional', 10.0)),
        'min_qty': float(lot_size_filter.get('minQty', 0.0)),
        'step_size': float(lot_size_filter.get('stepSize', 0.00000001)),
        'tick_size': float(price_filter.get('tickSize', 0.00000001)),
        'price_precision': price_precision,
        'quantity_precision': quantity_precision,
    }
# --- ✅ انتهاء الدالة ---

@dp.message_handler(lambda message: message.text == "📞 تواصل معنا للاشتراك")
async def contact_us_handler(message: types.Message):
    keyboard = InlineKeyboardMarkup().add(
        InlineKeyboardButton("💬 تواصل معنا", url="https://t.me/Tayebdz12")
    )
    await message.answer(
        "📩 للتواصل والاشتراك في البوت، يرجى الضغط على الزر أدناه:",
        reply_markup=keyboard
    )

def save_active_trades():
    """يحفظ القاموس active_trades في ملف JSON."""
    try:
        # نقوم بإنشاء نسخة لا تحتوي على كائنات 'monitor_task' لأنها لا يمكن تحويلها لـ JSON
        trades_to_save = {}
        for symbol, data in active_trades.items():
            # ننسخ كل البيانات ما عدا المهمة
            trades_to_save[symbol] = {k: v for k, v in data.items() if k != 'monitor_task'}

        with open(ACTIVE_TRADES_FILE, 'w') as f:
            json.dump(trades_to_save, f, indent=4)
        logger.info(f"Successfully saved {len(trades_to_save)} active trades to disk.")
    except Exception as e:
        logger.error(f"Failed to save active trades: {e}", exc_info=True)

async def load_and_resume_trades():

    global active_trades, pending_trades

    try:
        if os.path.exists(ACTIVE_TRADES_FILE):
            with open(ACTIVE_TRADES_FILE, 'r') as f:
                loaded_trades = json.load(f)
            
            if not loaded_trades: 
                return

            logger.warning(f"Found {len(loaded_trades)} trades from previous session. Resuming...")

            async with trade_lock:
                active_trades = loaded_trades

            for symbol, trade_data in active_trades.items():
                current_index = trade_data.get('current_target_index', None)
                exit_target = trade_data.get('exit_at_target_number', None)
                logger.info(f"Loading trade {symbol} with current_target_index={current_index} and exit_at_target_number={exit_target}")

                if trade_data.get('state') == 'CLOSING':
                    logger.warning(f"Trade {symbol} was in 'CLOSING' state. Initiating failsafe sell to confirm closure.")
                    await cancel_and_sell_market(symbol, trade_data, "استئناف من حالة إغلاق سابقة")
                else:
                    symbol_api_format = symbol.replace('/', '')
                    try:
                        current_price = live_prices.get(symbol_api_format) or await get_current_price(symbol_api_format)

                    except Exception as e:
                        logger.error(f"Failed to get current price for {symbol} during resume: {e}")
                        current_price = None

                    if current_price is not None:
                        corrected_index = correct_current_target_index(current_price, trade_data.get('targets', []))
                        async with trade_lock:
                            if symbol in active_trades:
                                active_trades[symbol]['current_target_index'] = corrected_index
                            save_active_trades()  # حفظ التعديل على القرص

                    logger.info(f"Resuming monitoring for active trade: {symbol}")
                    monitor_task = asyncio.create_task(monitor_trade(symbol))
                    async with trade_lock:
                        if symbol in active_trades:
                            active_trades[symbol]['monitor_task'] = monitor_task

    except Exception as e:
        logger.error(f"Failed to load or resume trades: {e}", exc_info=True)

    # تحديث عدد الصفقات المعلقة بعد استئنافها من الملف
    pending_trades = len(active_trades)

# معالج جديد للتحكم في وقف الخسارة المشروط
@dp.message_handler(lambda message: message.text == "🕓 نوع وقف الخسارة (4 ساعات)")
async def toggle_conditional_sl_prompt(message: types.Message):
    if message.chat.id not in ADMIN_USER_IDS_LIST: return

    # عرض الحالة الحالية
    current_status = "✅ مشروط (بإغلاق 4 ساعات)" if use_conditional_sl else "⚡️ فوري"
    
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ تفعيل المشروط", callback_data="set_cond_sl:true"),
        InlineKeyboardButton("⚡️ تفعيل الفوري", callback_data="set_cond_sl:false")
    )
    
    await message.answer(
        f"**الإعداد الحالي لوقف الخسارة:** `{current_status}`\n\n"
        "اختر الوضع الذي تريده:",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )

# معالج الـ callback للزر الجديد
@dp.callback_query_handler(lambda c: c.data.startswith("set_cond_sl:"))
async def process_conditional_sl_selection(callback_query: types.CallbackQuery):
    global use_conditional_sl
    
    choice = callback_query.data.split(":")[1]
    
    if choice == "true":
        use_conditional_sl = True
        response_message = "✅ **تم التفعيل:** سيتم الآن تطبيق وقف الخسارة بشكل **مشروط** (عند إغلاق شمعة 4 ساعات)."
    else: # choice == "false"
        use_conditional_sl = False
        response_message = "⚡️ **تم التفعيل:** سيتم الآن تطبيق وقف الخسارة بشكل **فوري**."
        
    try:
        await callback_query.message.edit_text(response_message, parse_mode="Markdown")
    except MessageNotModified:
        # حل بديل: حذف الرسالة وإرسال واحدة جديدة
        await callback_query.message.delete()
        await bot.send_message(callback_query.from_user.id, response_message, parse_mode="Markdown")
    await bot.answer_callback_query(callback_query.id)

@dp.message_handler(lambda message: message.text == "🏆 أداء العملات")
async def show_currency_performance(message: types.Message):
    if message.chat.id not in ADMIN_USER_IDS_LIST:
        await message.answer("🚫 ليس لديك صلاحية لهذا الأمر.")
        return
        
    if not currency_performance:
        await message.answer("⚠️ لا توجد بيانات أداء مسجلة حتى الآن.")
        return

    # 1. حساب الإحصائيات وترتيب البيانات
    performance_list = []
    for symbol, data in currency_performance.items():
        net_profit_percent = data.get('total_profit_percent', 0.0)
        
        data['symbol'] = symbol
        data['net_profit_percent'] = net_profit_percent
        performance_list.append(data)

    # الترتيب من الأعلى ربحًا إلى الأقل
    performance_list.sort(key=lambda x: x['net_profit_percent'], reverse=True)
    
    # --- [بداية التعديل] بناء الرسالة بالشكل الجديد ---
    
    message_parts = ["🏆 **أداء العملات (الربح الصافي):**"]

    # بناء كل سطر
    for data in performance_list:
        # إزالة /USDT من اسم العملة
        base_symbol = data['symbol'].replace('/USDT', '')
        
        wins = data.get('wins', 0)
        losses = data.get('losses', 0)
        net_profit = data.get('net_profit_percent', 0.0)
        
        # تحديد الإيموجي المناسب
        profit_emoji = "✅" if net_profit > 0 else "❌" if net_profit < 0 else "⚪️"
        
        # بناء السطر
        line = f"{profit_emoji} **{base_symbol}**: {net_profit:+.2f}%  *(📈{wins} | 📉{losses})*"
        message_parts.append(line)

    # --- [نهاية التعديل] ---

    # 3. إرسال الرسالة
    if len(message_parts) > 1:
        final_message = "\n".join(message_parts)
        
        for part in split_message(final_message, 4000):
            await message.answer(part, parse_mode="Markdown")
    else:
        await message.answer("⚠️ لا توجد بيانات كافية لعرضها.")

def load_all_stats():
    global total_trades, successful_trades, failed_trades, net_profit_usdt, trade_history, currency_performance
    try:
        if os.path.exists(STATS_FILE):
            with open(STATS_FILE, 'r') as f:
                stats_data = json.load(f)

            total_trades = stats_data.get('total_trades', 0)
            successful_trades = stats_data.get('successful_trades', 0)
            failed_trades = stats_data.get('failed_trades', 0)
            net_profit_usdt = stats_data.get('net_profit_usdt', 0.0)
            raw_history = stats_data.get('trade_history', [])
            
            # ✅ تحويل السلاسل إلى قواميس إذا لزم الأمر
            trade_history_cleaned = []
            for item in raw_history:
                if isinstance(item, str):
                    try:
                        item = json.loads(item)
                    except json.JSONDecodeError:
                        continue  # تخطي العناصر المعطوبة
                trade_history_cleaned.append(item)
            
            trade_history = deque(trade_history_cleaned, maxlen=200)
            currency_performance = stats_data.get('currency_performance', {})

            logger.info("All bot statistics loaded successfully.")
        else:
            logger.warning(f"'{STATS_FILE}' not found. Starting with fresh stats.")
    except Exception as e:
        logger.error(f"Failed to load stats: {e}", exc_info=True)

def save_all_stats():
    """يحفظ جميع بيانات الإحصائيات في ملف JSON واحد."""
    global total_trades, successful_trades, failed_trades, net_profit_usdt, trade_history, currency_performance
    try:
        stats_data = {
            'total_trades': total_trades,
            'successful_trades': successful_trades,
            'failed_trades': failed_trades,
            'net_profit_usdt': net_profit_usdt,
            'trade_history': list(trade_history),
            'currency_performance': currency_performance
        }
        with open(STATS_FILE, 'w') as f:
            json.dump(stats_data, f, indent=4)
        logger.info("All bot statistics saved successfully.")
    except Exception as e:
        logger.error(f"Failed to save all stats: {e}", exc_info=True)

async def load_symbol_info_from_cache_file():
    """
    يحاول تحميل معلومات الرموز من ملف الكاش إذا كان موجودًا وحديثًا.
    """
    global symbol_info_cache
    
    try:
        with open(SYMBOL_INFO_CACHE_FILE, 'r') as f:
            cached_data = json.load(f)
        
        last_updated_ts = cached_data.get('timestamp', 0)
        current_ts = time.time()
        
        # التحقق إذا كان عمر البيانات أقل من 24 ساعة (86400 ثانية)
        if (current_ts - last_updated_ts) < 604800:
            symbol_info_cache = cached_data.get('data', {})
            if symbol_info_cache:
                logger.info(f"✅ Successfully loaded {len(symbol_info_cache)} symbols from cache file. Data is fresh.")
                return True # نجح التحميل من الكاش
    except FileNotFoundError:
        logger.warning(f"Cache file '{SYMBOL_INFO_CACHE_FILE}' not found. Will fetch from API.")
    except json.JSONDecodeError:
        logger.error(f"Error decoding cache file '{SYMBOL_INFO_CACHE_FILE}'. Will fetch from API.")
    except Exception as e:
        logger.error(f"Error loading from cache file: {e}", exc_info=True)
        
    return False # فشل التحميل من الكاش، يجب الجلب من API

# زر "إلغاء" عالمي للخروج من عملية الإدخال اليدوي
@dp.message_handler(state="*", text="إلغاء")
async def cancel_manual_trade_input(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is None:
        return
    
    logger.info(f"User {message.from_user.id} cancelled manual trade input.")
    await state.finish()
    await message.answer("✅ تم إلغاء عملية إدخال الصفقة اليدوية.", reply_markup=get_main_keyboard(message.from_user.id))

# الخطوة 1: بدء عملية الإدخال اليدوي
@dp.message_handler(lambda message: message.text == "➕ صفقة يدوية")
async def start_manual_trade(message: types.Message):
    if message.chat.id not in ADMIN_USER_IDS_LIST:
        await message.answer("🚫 ليس لديك صلاحية لهذا الأمر.")
        return
    
    cancel_keyboard = ReplyKeyboardMarkup(resize_keyboard=True).add(KeyboardButton("إلغاء"))
    await message.answer(
        "📝 **بدء إدخال صفقة يدوية...**\n\n"
        "**الخطوة 1 من 5:**\n"
        "يرجى إدخال رمز العملة (مثال: `BTCUSDT`)",
        reply_markup=cancel_keyboard,
        parse_mode="Markdown"
    )
    await MultipleTradesState.waiting_for_manual_symbol.set()

# الخطوة 2: استلام رمز العملة وطلب سعر الدخول
@dp.message_handler(state=MultipleTradesState.waiting_for_manual_symbol)
async def process_manual_symbol(message: types.Message, state: FSMContext):
    symbol = message.text.strip().upper()
    if not re.match("^[A-Z0-9]{2,10}USDT$", symbol):
        await message.answer("⚠️ تنسيق الرمز غير صالح. يرجى إدخاله بالشكل الصحيح (مثال: `BTCUSDT`).")
        return
    await state.update_data(manual_symbol=symbol)
    await message.answer(
        f"✅ تم حفظ الرمز: `{symbol}`\n\n"
        "**الخطوة 2 من 5:**\n"
        "يرجى إدخال سعر الدخول الذي تريده للصفقة.",
        parse_mode="Markdown"
    )
    await MultipleTradesState.waiting_for_manual_entry.set()

# الخطوة 3: استلام سعر الدخول وطلب الأهداف
@dp.message_handler(state=MultipleTradesState.waiting_for_manual_entry)
async def process_manual_entry(message: types.Message, state: FSMContext):
    try:
        entry_price = float(message.text.strip())
        if entry_price <= 0: raise ValueError()
    except ValueError:
        await message.answer("⚠️ سعر الدخول غير صالح. يرجى إدخال رقم صحيح موجب.")
        return
    await state.update_data(manual_entry=entry_price)
    await message.answer(
        f"✅ تم حفظ سعر الدخول: `{entry_price}`\n\n"
        "**الخطوة 3 من 5:**\n"
        "يرجى إدخال **3 أهداف** مفصولة بمسافة أو فاصلة.\n"
        "(مثال: `70000 71500 73000`)",
        parse_mode="Markdown"
    )
    await MultipleTradesState.waiting_for_manual_targets.set()

# الخطوة 4: استلام الأهداف وطلب وقف الخسارة
@dp.message_handler(state=MultipleTradesState.waiting_for_manual_targets)
async def process_manual_targets(message: types.Message, state: FSMContext):
    targets_str = re.findall(r'[\d\.]+', message.text)
    if len(targets_str) != 3:
        await message.answer("⚠️ يجب إدخال **3 أهداف بالضبط**. حاول مرة أخرى.")
        return
    try:
        targets = sorted([float(t) for t in targets_str])
        user_data = await state.get_data()
        entry_price = user_data.get('manual_entry')
        if any(t <= entry_price for t in targets):
            await message.answer(f"⚠️ يجب أن تكون جميع الأهداف أعلى من سعر الدخول (`{entry_price}`). حاول مرة أخرى.")
            return
    except ValueError:
        await message.answer("⚠️ أحد الأهداف غير صالح. يرجى إدخال أرقام صحيحة.")
        return
    await state.update_data(manual_targets=targets)
    await message.answer(
        f"✅ تم حفظ الأهداف: `{targets}`\n\n"
        "**الخطوة 4 من 5:**\n"
        "يرجى إدخال سعر وقف الخسارة.",
        parse_mode="Markdown"
    )
    await MultipleTradesState.waiting_for_manual_sl.set()

# الخطوة 5: استلام وقف الخسارة وطلب المبلغ
@dp.message_handler(state=MultipleTradesState.waiting_for_manual_sl)
async def process_manual_sl(message: types.Message, state: FSMContext):
    try:
        sl_price = float(message.text.strip())
        user_data = await state.get_data()
        entry_price = user_data.get('manual_entry')
        if sl_price >= entry_price:
            await message.answer(f"⚠️ يجب أن يكون سعر وقف الخسارة أقل من سعر الدخول (`{entry_price}`). حاول مرة أخرى.")
            return
    except ValueError:
        await message.answer("⚠️ سعر وقف الخسارة غير صالح. يرجى إدخال رقم صحيح.")
        return
    await state.update_data(manual_sl=sl_price)
    await message.answer(
        f"✅ تم حفظ وقف الخسارة: `{sl_price}`\n\n"
        "**الخطوة 5 من 5:**\n"
        "يرجى إدخال المبلغ (بالـ USDT) الذي تريد الدخول به.",
        parse_mode="Markdown"
    )
    await MultipleTradesState.waiting_for_manual_amount.set()

# الخطوة الأخيرة: استلام المبلغ وتأكيد الصفقة وتنفيذها
@dp.message_handler(state=MultipleTradesState.waiting_for_manual_amount)
async def process_manual_amount(message: types.Message, state: FSMContext):
    try:
        amount = float(message.text.strip())
        if amount <= 0: raise ValueError()
    except ValueError:
        await message.answer("⚠️ المبلغ غير صالح. يرجى إدخال رقم موجب.")
        return

    user_data = await state.get_data()
    symbol_api = user_data.get('manual_symbol')
    entry_price = user_data.get('manual_entry')
    targets = user_data.get('manual_targets')
    sl_price = user_data.get('manual_sl')
    
    await state.finish()

    confirmation_message = (
        "**📝 ملخص الصفقة اليدوية:**\n\n"
        f"- **العملة:** `{symbol_api}`\n"
        f"- **سعر الدخول:** `{entry_price}`\n"
        f"- **الأهداف:** `{targets}`\n"
        f"- **وقف الخسارة:** `{sl_price}`\n"
        f"- **المبلغ:** `{amount} USDT`\n\n"
        "⏳ **جاري الآن معالجة الصفقة ووضع الأوامر...**"
    )
    await message.answer(
        confirmation_message, 
        parse_mode="Markdown", 
        reply_markup=get_main_keyboard(message.from_user.id)
    )
    
    symbol_key = f"{symbol_api.replace('USDT', '')}/USDT"
    await place_trade_orders(
        symbol_key=symbol_key,
        entry_price=entry_price,
        targets=targets,
        stop_loss_price=sl_price,
        amount_usdt=amount,
        is_4h_conditional=False 
    )

async def initial_symbol_info_load_wrapper(): 
    logger.info("Initial symbol info load task started (blocking startup)...") # تعديل رسالة السجل
    try:
        await fetch_and_cache_all_symbol_info() # هذه هي دالتك التي تستهدف قائمة الحلال
        logger.info("Initial symbol info load task COMPLETED successfully.")
        return True # إرجاع True عند النجاح
    except Exception as e:
        logger.error(f"Error during initial_symbol_info_load: {e}", exc_info=True)
        return False # إرجاع False عند الفشل

async def get_complete_symbol_info(symbol_cleaned_for_api: str, symbol_for_log: str) -> dict | None:
    global symbol_info_cache, client

    cached_s_info = symbol_info_cache.get(symbol_cleaned_for_api)
    full_required_keys = ['minQty', 'stepSize', 'minNotional', 'tickSize', 
                          'baseAsset', 'quoteAsset', 'status', 
                          'price_precision', 'quantity_precision']

    if cached_s_info and all(k in cached_s_info for k in full_required_keys):
        logger.debug(f"GetSymbolInfo ({symbol_for_log}): Using fully populated cached info for {symbol_cleaned_for_api}.")
        return cached_s_info

    logger.warning(f"GetSymbolInfo ({symbol_for_log}): Symbol info for {symbol_cleaned_for_api} not fully populated in cache or not found. Fetching/re-calculating from API.")
    try:
        live_s_info_api = await retry_blocking_api_call(client.get_symbol_info, symbol_cleaned_for_api, symbol_for_log=symbol_for_log)
        if not live_s_info_api:
            logger.error(f"GetSymbolInfo ({symbol_for_log}): Failed to fetch live symbol_info for {symbol_cleaned_for_api} from API.")
            return None
        
        filters_api = {f['filterType']: f for f in live_s_info_api['filters']}
        price_f_api = filters_api.get('PRICE_FILTER', {})
        lot_size_f_api = filters_api.get('LOT_SIZE', {})
        min_not_f_api = filters_api.get('MIN_NOTIONAL', {}) # <--- هذا هو فلتر MIN_NOTIONAL من API

        tick_size_str = price_f_api.get('tickSize', '0.00000001')
        step_size_str = lot_size_f_api.get('stepSize', '0.00000001')
        
        tick_size_val = float(tick_size_str)
        step_size_val = float(step_size_str)
        min_qty_val = float(lot_size_f_api.get('minQty', '0'))
        
        # ====> هنا النقطة الحاسمة <====
        # اسم الحقل في استجابة API لـ MIN_NOTIONAL filter هو 'minNotional'
        min_notional_val = float(min_not_f_api.get('minNotional', '10.0')) 

        price_prec = 0
        if tick_size_val > 0 and tick_size_val < 1:
            tick_size_formatted_str = format(tick_size_val, '.10f').rstrip('0')
            if '.' in tick_size_formatted_str:
                 price_prec = len(tick_size_formatted_str.split('.')[1])
        
        qty_prec = 0
        if step_size_val > 0 and step_size_val < 1:
            step_size_formatted_str = format(step_size_val, '.10f').rstrip('0')
            if '.' in step_size_formatted_str:
                qty_prec = len(step_size_formatted_str.split('.')[1])
        
        # --- ✅ استخدام الدالة الموحّدة ---
        complete_info_dict = normalize_symbol_info(live_s_info_api)
        # ---

        symbol_info_cache[symbol_cleaned_for_api] = complete_info_dict 

        logger.info(f"GetSymbolInfo ({symbol_for_log}): Fetched/Recalculated and cached live complete symbol info for {symbol_cleaned_for_api}.")
        # قبل الإرجاع، اطبع القاموس للتأكد
        logger.debug(f"GetSymbolInfo ({symbol_for_log}): Returning dict: {complete_info_dict}")
        return complete_info_dict

    except BinanceAPIException as e_fetch_info_api:
         logger.error(f"GetSymbolInfo ({symbol_for_log}): Binance API error fetching symbol info for {symbol_cleaned_for_api}: {e_fetch_info_api}", exc_info=True)
         return None
    except Exception as e_fetch_info:
        logger.error(f"GetSymbolInfo ({symbol_for_log}): Unexpected error fetching symbol info for {symbol_cleaned_for_api}: {e_fetch_info}", exc_info=True)
        return None

def load_halal_symbols():
    """
    Loads the list of halal symbols from the specified file into a set.
    The symbols in the file should be the base asset (e.g., BTC, ETH, APT).
    """
    global halal_symbols_set
    try:
        with open(HALAL_SYMBOLS_LIST_FILE, 'r') as f:
            symbols_from_file = {line.strip().upper() for line in f if line.strip()}
        
        if symbols_from_file:
            halal_symbols_set = symbols_from_file
            logger.info(f"Successfully loaded {len(halal_symbols_set)} halal symbols from {HALAL_SYMBOLS_LIST_FILE}.")
            # يمكنك طباعة أول 5 عملات للتأكيد إذا أردت
            # logger.info(f"Sample halal symbols: {list(halal_symbols_set)[:5]}")
        else:
            logger.warning(f"{HALAL_SYMBOLS_LIST_FILE} is empty or contains no valid symbols. No halal filter will be applied.")
            halal_symbols_set = set() # تأكد أنها فارغة إذا لم يتم تحميل شيء
    except FileNotFoundError:
        logger.error(f"{HALAL_SYMBOLS_LIST_FILE} not found. No halal filter will be applied.")
        halal_symbols_set = set() # تأكد أنها فارغة
    except Exception as e:
        logger.error(f"Error loading {HALAL_SYMBOLS_LIST_FILE}: {e}", exc_info=True)
        halal_symbols_set = set() # تأكد أنها فارغة في حالة أي خطأ آخر

def get_main_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)

    # الأزرار الإضافية الخاصة بالأدمن فقط
    if user_id in ADMIN_USER_IDS_LIST:
        keyboard.row(KeyboardButton("✅ تشغيل البوت"), KeyboardButton("⛔ إيقاف البوت"))
        keyboard.row(KeyboardButton("➕ صفقة يدوية"), KeyboardButton("⚙️ إعدادات البوت"))
        keyboard.add(KeyboardButton("📝 عرض الأصول"), KeyboardButton("📊 إحصائيات الصفقات"))
        keyboard.row(KeyboardButton("📈 عرض الصفقات النشطة"), KeyboardButton("📈 عرض تاريخ الصفقات"))
        keyboard.add(KeyboardButton("🏆 أداء العملات"))

    else:
        # الأزرار التي تظهر لجميع المستخدمين المسموح لهم (بما في ذلك الأدمن)
        keyboard.row(KeyboardButton("📊 إحصائيات الصفقات"))
        keyboard.row(KeyboardButton("📈 عرض تاريخ الصفقات"))
        keyboard.row(KeyboardButton("📈 عرض الصفقات الحالية"))
        keyboard.add(types.KeyboardButton("📞 تواصل معنا للاشتراك"))
    
    return keyboard

async def fetch_and_cache_all_symbol_info():
    """
    Fetches and caches symbol information ONLY for the target symbols 
    (e.g., from halal_symbols_set) that are USDT pairs and TRADING.
    """
    global client, symbol_info_cache, halal_symbols_set

    if not halal_symbols_set:
        logger.warning("TargetSymbolInfo: halal_symbols_set is empty. No symbols to fetch info for.")
        return

    logger.info(f"TargetSymbolInfo: Fetching/updating symbol information for {len(halal_symbols_set)} target symbols...")
    
    new_symbol_info_cache = {} # بناء كاش جديد لتجنب الاحتفاظ بمعلومات رموز قديمة لم تعد مستهدفة
    new_symbol_info_cache_timestamps = {}
    processed_count = 0

    # قائمة الرموز المستهدفة يجب أن تكون بتنسيق API (مثل BTCUSDT)
    # halal_symbols_set يحتوي على العملات الأساسية (مثل BTC). يجب تحويلها.
    target_api_symbols = {f"{base_asset}USDT" for base_asset in halal_symbols_set}
    # يمكنك إضافة أزواج أخرى إذا لزم الأمر، مثلاً:
    # target_api_symbols.update({f"{base_asset}BUSD" for base_asset in halal_symbols_set if base_asset != "BUSD"})

    for target_symbol_api_format in target_api_symbols:
        try:
            # جلب معلومات الرمز الفردي
            s_info = await retry_blocking_api_call(
                client.get_symbol_info,
                symbol=target_symbol_api_format, # استخدام اسم الرمز بتنسيق API
                symbol_for_log=f"SymbolInfo for {target_symbol_api_format}"
            )

            if not s_info:
                logger.warning(f"TargetSymbolInfo: Could not fetch info for {target_symbol_api_format}. Skipping.")
                continue

            # التحقق من أن الرمز هو زوج USDT (للتأكيد) وأنه قيد التداول
            if s_info.get('quoteAsset') == 'USDT' and s_info.get('status') == 'TRADING':
                symbol_cleaned = s_info['symbol'] # يجب أن يكون target_symbol_api_format

                filters = {f['filterType']: f for f in s_info['filters']}
                lot_size_filter = filters.get('LOT_SIZE', {})
                min_notional_filter = filters.get('MIN_NOTIONAL', {})
                price_filter = filters.get('PRICE_FILTER', {})

                # حساب الدقة
                tick_size_val = float(price_filter.get('tickSize', '0.00000001'))
                step_size_val = float(lot_size_filter.get('stepSize', '0.00000001'))
                
                price_prec = 0
                if tick_size_val > 0 and tick_size_val < 1:
                    tick_size_str_temp = format(tick_size_val, '.10f').rstrip('0')
                    if '.' in tick_size_str_temp:
                         price_prec = len(tick_size_str_temp.split('.')[1])
                
                qty_prec = 0
                if step_size_val > 0 and step_size_val < 1:
                    step_size_str_temp = format(step_size_val, '.10f').rstrip('0')
                    if '.' in step_size_str_temp:
                        qty_prec = len(step_size_str_temp.split('.')[1])
                    else:
                        qty_prec = 0

                # --- ✅ استخدام normalize_symbol_info ---
                normalized_info = normalize_symbol_info(s_info)
                new_symbol_info_cache[symbol_cleaned] = normalized_info
                new_symbol_info_cache_timestamps[symbol_cleaned] = time.time()
                # --- ✅ انتهاء الاستخدام ---

                processed_count += 1
                logger.debug(f"TargetSymbolInfo: Cached info for {symbol_cleaned}: {normalized_info}")
            else:
                logger.warning(f"TargetSymbolInfo: Target symbol {target_symbol_api_format} is not a TRADING USDT pair. Status: {s_info.get('status')}, Quote: {s_info.get('quoteAsset')}. Skipping.")

        except BinanceAPIException as e_api_single:
            if e_api_single.code == -1121: # Invalid symbol
                logger.warning(f"TargetSymbolInfo: Target symbol {target_symbol_api_format} is invalid or not listed on Binance. Skipping. Error: {e_api_single.message}")
            else:
                logger.error(f"TargetSymbolInfo: Binance API error fetching info for {target_symbol_api_format}: {e_api_single}", exc_info=False) # exc_info=False لتقليل الضوضاء
        except Exception as e_single:
            logger.error(f"TargetSymbolInfo: Unexpected error fetching info for {target_symbol_api_format}: {e_single}", exc_info=False)
        
        await asyncio.sleep(0.1) # تأخير بسيط بين طلبات API لتجنب ضرب حدود المعدل بشدة

    # تحديث الكاش العام بالكاش الجديد
    # استخدام قفل إذا كان من الممكن الوصول إلى symbol_info_cache من مهام أخرى أثناء هذا التحديث
    # لكن بما أن هذا يتم دوريًا، قد لا يكون القفل ضروريًا إذا كانت القراءات تتحمل بيانات قديمة للحظات
    symbol_info_cache = new_symbol_info_cache
    
    try:
        data_to_save = {
            'timestamp': time.time(),
            'data': new_symbol_info_cache
        }
        with open(SYMBOL_INFO_CACHE_FILE, 'w') as f:
            json.dump(data_to_save, f, indent=4)
        logger.info(f"💾 Symbol info cache successfully saved to '{SYMBOL_INFO_CACHE_FILE}'.")
    except Exception as e:
        logger.error(f"Failed to save symbol info cache to file: {e}", exc_info=True)
    
    logger.info(f"TargetSymbolInfo: Successfully fetched/updated information for {processed_count}/{len(target_api_symbols)} target trading symbols.")

async def retry_blocking_api_call(blocking_api_func, *args, **kwargs):
    """
    (النسخة النهائية المُحسّنة)
    تدعم إعادة المحاولة، تصنيف الأخطاء، ووقت انتظار أقصاه (timeout).
    """
    max_retries = 3
    retry_delay = 1
    timeout_seconds = 12  # ⏰ لا تنتظر أكثر من 12 ثانية

    # --- [بداية التصحيح] ---
    symbol_for_log = kwargs.pop('symbol_for_log', 'Unknown')
    # --- [نهاية التصحيح] ---

    NON_RETRYABLE_ERRORS = [-2010, -1121, -1013, -2015, -1104]

    for attempt in range(max_retries):
        try:
            # ✅ نضيف timeout حول الاستدعاء
            result = await asyncio.wait_for(
                asyncio.to_thread(lambda: blocking_api_func(*args, **kwargs)),
                timeout=timeout_seconds
            )
            return result

        except asyncio.TimeoutError:
            logger.warning(f"⏰ TIMEOUT ({timeout_seconds}s) for {blocking_api_func.__name__} on {symbol_for_log}")
            if attempt == max_retries - 1:
                raise Exception(f"Timeout after {max_retries} attempts")
            await asyncio.sleep(retry_delay)
            retry_delay *= 2

        except BinanceAPIException as e:
            logger.error(f"Binance API Error for {symbol_for_log}: {e.code} - {e.message}")
            
            if e.code in NON_RETRYABLE_ERRORS:
                logger.error(f"🛑 Non-retryable error ({e.code}). Aborting.")
                raise e

            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
                retry_delay *= 2
            else:
                logger.error(f"❌ All retries failed for {blocking_api_func.__name__} on {symbol_for_log}.")
                raise e

        except Exception as e:
            logger.error(f"❌ Unexpected error in retry_blocking_api_call for {symbol_for_log}: {e}", exc_info=True)
            if attempt == max_retries - 1:
                raise e
            await asyncio.sleep(retry_delay)
            retry_delay *= 2

    raise Exception("Retry logic failed unexpectedly.")

def get_usdt_symbols():
    exchange_info = client.get_exchange_info()
    symbols = exchange_info['symbols']

    usdt_pairs = [
        s['symbol'] for s in symbols
        if s['quoteAsset'] == 'USDT' and s['status'] == 'TRADING'
    ]
    return usdt_pairs

async def connect_to_price_server():
   global live_prices, halal_symbols_set, active_trades

   # الإعدادات الأساسية
   RECONNECT_DELAY_BASE = 10
   MAX_RECONNECT_DELAY = 120
   API_FALLBACK_INTERVAL = 5
   NOTIFICATION_COOLDOWN = 60

   # دالة تنظيف الأسعار القديمة
   async def cleanup_stale_live_prices(target_symbols_api_format_set):
       while True:
           await asyncio.sleep(15 * 60)
           current_live_keys = list(live_prices.keys())
           keys_to_delete = [symbol_key for symbol_key in current_live_keys 
                           if symbol_key not in target_symbols_api_format_set]
           
           if keys_to_delete:
               unique_keys_to_delete = set(keys_to_delete)
               logger.info(f"LivePricesCleanup: Removing {len(unique_keys_to_delete)} stale symbols...")
               for key in unique_keys_to_delete:
                   live_prices.pop(key, None)

   # التحقق من وجود رموز حلال
   if not halal_symbols_set:
       logger.error("GlobalWS: halal_symbols_set is empty. Cannot start WebSocket monitoring.")
       load_halal_symbols()
       if not halal_symbols_set:
           logger.critical("CRITICAL: halal_symbols_set is still empty. WebSocket disabled.")
           return

   # تحضير الرموز المستهدفة
   target_api_symbols_keys = {f"{base_asset}USDT" for base_asset in halal_symbols_set}

   if not target_api_symbols_keys:
       logger.error("GlobalWS: No valid USDT symbols from halal_symbols_set.")
       return

   asyncio.create_task(cleanup_stale_live_prices(target_api_symbols_keys))

   logger.info(f"GlobalWS: Connecting to Price Server for {len(target_api_symbols_keys)} symbols.")

   async def manage_price_server_connection():
       url = "ws://localhost:8765"
       reconnect_attempts = 0
       last_notification_time = 0
       using_api_fallback = False

       while True:
           # 1. محاولة الاتصال بـ Price Server
           try:
               logger.info("GlobalWS: Connecting to Price Server...")
               
               async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                   reconnect_attempts = 0
                   
                   # إعلام المستخدم عند استعادة الاتصال
                   if using_api_fallback:
                       using_api_fallback = False
                       active_symbols = [symbol.replace('/', '') for symbol in active_trades.keys()]
                       if active_symbols and time.time() - last_notification_time > NOTIFICATION_COOLDOWN:
                           await send_result_to_users(
                               f"✅ تم استعادة الاتصال بـ Price Server للعملات:\n"
                               f"{', '.join(active_symbols[:5])}{'...' if len(active_symbols) > 5 else ''}"
                           )
                           last_notification_time = time.time()

                   logger.info("✅ GlobalWS: Connected to Price Server successfully")
                   
                   # 2. استقبال البيانات من Price Server
                   while True:
                       try:
                            message = await asyncio.wait_for(ws.recv(), timeout=45)
                            price_update = json.loads(message)
                           
                            if 'symbol' in price_update and 'price' in price_update:
                                symbol = price_update['symbol']
                                price = price_update['price']
                                live_prices[symbol] = price
                            
                       except asyncio.TimeoutError:
                           try:
                               await asyncio.wait_for(ws.ping(), timeout=10)
                           except Exception as e:
                               logger.warning(f"GlobalWS: Ping to Price Server failed: {e}")
                               break
                           
                       except websockets.exceptions.ConnectionClosed as e:
                           logger.warning(f"GlobalWS: Price Server connection closed: {e}")
                           break
                       except Exception as e:
                           logger.error(f"GlobalWS: Error receiving from Price Server: {e}")
                           break

           except Exception as e:
               logger.error(f"GlobalWS: Price Server connection error: {e}")

           # 3. التحويل إلى استخدام API عند انقطاع Price Server
           if not using_api_fallback:
               using_api_fallback = True
               active_symbols = [symbol.replace('/', '') for symbol in active_trades.keys()]
               if active_symbols and time.time() - last_notification_time > NOTIFICATION_COOLDOWN:
                   await send_result_to_users(
                       f"⚠️ انقطع الاتصال بـ Price Server:\n"
                       f"سيتم استخدام تحديثات API كل {API_FALLBACK_INTERVAL} ثواني حتى يعود الاتصال"
                   )
                   last_notification_time = time.time()

           # 4. تحديث الأسعار عبر API للصفقات النشطة فقط
           async with trade_lock:
               active_symbols_for_api = [symbol.replace('/', '') for symbol in active_trades.keys()]
           
           for symbol in active_symbols_for_api:
               try:
                   ticker = await retry_blocking_api_call(
                       client.get_symbol_ticker, 
                       symbol=symbol,
                       symbol_for_log=symbol
                   )
                   live_prices[symbol] = float(ticker['price'])
               except Exception as e:
                   logger.warning(f"GlobalWS: Failed to update {symbol} via API: {e}")
               
               await asyncio.sleep(API_FALLBACK_INTERVAL / max(len(active_symbols_for_api), 1))

           # 5. إعادة المحاولة مع زيادة التأخير تدريجياً
           reconnect_attempts += 1
           delay = min(RECONNECT_DELAY_BASE * (2 ** min(reconnect_attempts, 4)), MAX_RECONNECT_DELAY)
           logger.info(f"GlobalWS: Retrying Price Server connection in {delay} seconds...")
           await asyncio.sleep(delay)

   # بدء اتصال Price Server
   task = asyncio.create_task(manage_price_server_connection())
   
   logger.info("GlobalWS: Price Server connection task started.")

async def get_current_price(symbol_for_api: str, max_attempts=10): # الآن تتوقع رمزًا مثل "BTCUSDT"
    """
    الحصول على السعر الحالي للرمز (بتنسيق API مثل "BTCUSDT") مع إعادة المحاولة.
    """
    attempts = 0
    while attempts < max_attempts:
        try:
            ticker = await asyncio.to_thread(client.get_symbol_ticker, symbol=symbol_for_api)
            if ticker and 'price' in ticker: # تحقق إضافي من أن الاستجابة تحتوي على السعر
                current_price = float(ticker['price'])
                return current_price
            else:
                logger.warning(f"Invalid ticker response for {symbol_for_api} from API (attempt {attempts + 1}).")
        except BinanceAPIException as e:
            logger.warning(f"Binance API error for {symbol_for_api} (attempt {attempts + 1}): {e.code} - {e.message}")
            if e.code == -1121: # Invalid symbol
                raise ValueError(f"⚠️ رمز غير صالح: {symbol_for_api} (API).") # ارفع خطأ هنا مباشرة
        except Exception as e:
            logger.warning(f"Generic error fetching price for {symbol_for_api} (attempt {attempts + 1}): {e}", exc_info=True)
        
        attempts += 1
        if attempts < max_attempts: # لا تنم بعد آخر محاولة
             await asyncio.sleep(0.5 * attempts) # زيادة فترة الانتظار تدريجيًا
             
    raise ValueError(f"⚠️ تعذر الحصول على السعر الحالي للرمز {symbol_for_api} بعد {max_attempts} محاولة API.")

# 1. معالج زر الأدمن ("عرض الصفقات النشطة")
@dp.message_handler(lambda message: message.text == "📈 عرض الصفقات النشطة" and message.from_user.id in ADMIN_USER_IDS_LIST)
async def show_active_trades_for_admin(message: types.Message):
    """
    هذا المعالج خاص بالأدمن فقط، ويستدعي الدالة مع تفعيل الأزرار.
    """
    await show_active_trades(message, show_buttons=True)

# 2. معالج زر المستخدم العادي ("عرض الصفقات الحالية")
@dp.message_handler(lambda message: message.text == "📈 عرض الصفقات الحالية")
async def show_current_trades_for_user(message: types.Message):
    """
    هذا المعالج لجميع المستخدمين، ويستدعي الدالة بدون أزرار (للمشاهدة فقط).
    """
    # يمكنك إضافة تحقق هنا إذا كان المستخدم مصرح له برؤية الصفقات أصلاً
    await show_active_trades(message, show_buttons=False)

# عدّل هذه الدالة - أصبحت الآن تقبل معامل show_buttons
async def show_active_trades(message: types.Message, show_buttons: bool = True):
    global live_prices, active_trades

    if not active_trades:
        await message.answer("⚠️ لا توجد صفقات نشطة حاليًا.")
        return

    loading_message = await message.answer("⏳ جارٍ جلب بيانات الصفقات...")

    message_text = "📋 الصفقات النشطة:\n\n"
    
    # سنقوم بإنشاء لوحة المفاتيح فقط إذا كان مطلوبًا عرض الأزرار
    keyboard = InlineKeyboardMarkup(row_width=2) if show_buttons else None

    # استخدام نسخة آمنة من القاموس للتكرار
    for symbol_key, trade in list(active_trades.items()):
        symbol_for_api = symbol_key.replace('/', '')

        entry_price = trade.get('entry_price', 0)
        current_price = live_prices.get(symbol_for_api)

        if current_price is None:
            try:
                current_price = await get_current_price(symbol_for_api)
            except ValueError:
                current_price = 0

        if current_price and entry_price > 0:
            price_display = f"{current_price:.8f}$"
            price_change = ((current_price - entry_price) / entry_price) * 100
            price_change_display = f"{price_change:+.2f}%"
        else:
            price_display = "غير متوفر"
            price_change_display = "N/A"

        duration = time.time() - trade.get('start_time', time.time())
        duration_text = format_time(duration)
        quantity = trade.get('quantity', 0)
        current_value_usdt = quantity * current_price if current_price else 0
        value_display = f"`{current_value_usdt:.2f}$`" if current_value_usdt > 0 else "غير متوفر"
        progress_emoji = "🟢" if price_change != "N/A" and price_change >= 0 else "🔴"
        
        message_text += (
            f"🛒 **العملة: `{symbol_key}`**\n"
            f"   - 💰 القيمة الحالية: {value_display}\n"
            f"   - 📈 سعر الدخول: `{entry_price:.8f}$`\n"
            f"   - 📊 السعر الحالي: `{price_display}`\n"
            f"   - {progress_emoji} التقدم: `{price_change_display}`\n"
            f"   - ⏳ المدة: {duration_text}\n"
            f"--------------------\n"
        )
                
        # --- [ تعديل مهم ] ---
        # أضف الأزرار فقط إذا كان show_buttons يساوي True
        if show_buttons:
            sell_button = InlineKeyboardButton(
                text=f"❌ بيع {symbol_key}",
                callback_data=f"sell_active_trade:{symbol_for_api.replace('USDT', '')}"
            )
            keyboard.add(sell_button)

    # --- [ تعديل مهم ] ---
    # أضف زر "إغلاق الكل" فقط إذا كان show_buttons يساوي True وهناك صفقات
    if show_buttons and active_trades:
        sell_all_button = InlineKeyboardButton(
            text="⏹️ إغلاق جميع الصفقات النشطة",
            callback_data="sell_all_active_trades"
        )
        keyboard.add(sell_all_button)

    await bot.delete_message(chat_id=message.chat.id, message_id=loading_message.message_id)

    if len(message_text) > len("📋 الصفقات النشطة:\n\n"):
        for i, part in enumerate(split_message(message_text, max_length=4000)):
            # إرفاق لوحة المفاتيح مع آخر جزء من الرسالة فقط (إذا كانت موجودة)
            is_last_part = (i == len(split_message(message_text, max_length=4000)) - 1)
            await message.answer(
                part,
                parse_mode="Markdown",
                reply_markup=keyboard if is_last_part and keyboard else None
            )
    else:
        await message.answer("⚠️ لم يتم العثور على تفاصيل للصفقات النشطة.")

@dp.callback_query_handler(lambda c: c.data.startswith('sell_active_trade:'))
async def process_sell_active_trade(callback_query: types.CallbackQuery):
    """
    (نسخة جديدة ومباشرة) تعالج البيع اليدوي خطوة بخطوة مع تسجيل شامل.
    """
    await callback_query.answer("⏳ جارٍ تنفيذ الأمر...")
    
    symbol_key = f"{callback_query.data.split(':')[1]}/USDT"
    log_prefix = f"ManualSell ({symbol_key}):"
    
    logger.info(f"{log_prefix} --- MANUAL SELL PROCESS STARTED ---")

    # --- الخطوة 1: الحصول على نسخة من بيانات الصفقة ---
    async with trade_lock:
        trade_snapshot = active_trades.get(symbol_key, {}).copy()

    if not trade_snapshot:
        logger.warning(f"{log_prefix} Trade not found in active_trades.")
        await bot.send_message(callback_query.from_user.id, f"⚠️ لم يتم العثور على صفقة نشطة لـ {symbol_key}.")
        return

    try:
        # --- الخطوة 2: إلغاء الأوامر المفتوحة ---
        logger.info(f"{log_prefix} Attempting to cancel any open orders...")
        symbol_api_format = symbol_key.replace('/', '')
        open_orders = await retry_blocking_api_call(client.get_open_orders, symbol=symbol_api_format)
        if open_orders:
            cancel_tasks = [retry_blocking_api_call(client.cancel_order, symbol=symbol_api_format, orderId=o['orderId']) for o in open_orders]
            await asyncio.gather(*cancel_tasks, return_exceptions=True)
            logger.info(f"{log_prefix} Cancellation process complete for {len(open_orders)} orders.")
        else:
            logger.info(f"{log_prefix} No open orders found to cancel.")

        # --- الخطوة 3: البيع بسعر السوق ---
        await asyncio.sleep(0.5) # انتظار بعد الإلغاء
        s_info = trade_snapshot['symbol_info']
        balance = await retry_blocking_api_call(client.get_asset_balance, asset=s_info['baseAsset'])
        qty_to_sell = float(balance['free'])

        final_price = 0
        executed_qty = 0

        if qty_to_sell > 0:
            corrected_qty = adjust_quantity(qty_to_sell, s_info)
            if corrected_qty > 0:
                logger.info(f"{log_prefix} Selling {corrected_qty} at market price...")
                sell_order = await retry_blocking_api_call(client.order_market_sell, symbol=symbol_api_format, quantity=corrected_qty)
                if sell_order and not sell_order.get('error'):
                    final_price = float(sell_order['cummulativeQuoteQty']) / float(sell_order['executedQty'])
                    executed_qty = float(sell_order['executedQty'])
                    logger.info(f"{log_prefix} Sell successful. Final price: {final_price}")
                else:
                    logger.error(f"{log_prefix} Market sell was rejected: {sell_order.get('message')}")
            else:
                logger.warning(f"{log_prefix} Adjusted quantity is zero.")
        else:
            logger.warning(f"{log_prefix} No balance to sell. Position might be already closed.")

        # إذا لم يتم البيع، نستخدم السعر الحالي كمرجع
        if final_price == 0:
            final_price = live_prices.get(symbol_api_format) or trade_snapshot.get('entry_price')

        # --- الخطوة 4: الحذف والتسجيل (الأهم) ---
        logger.info(f"{log_prefix} Entering final block to update stats and remove trade.")
        await close_and_report(symbol_key, "إغلاق يدوي", final_price, executed_qty)
        
        await bot.send_message(callback_query.from_user.id, f"✅ تم إرسال أمر إغلاق صفقة {symbol_key}.")

    except Exception as e:
        logger.error(f"{log_prefix} CRITICAL FAILURE in manual sell process: {e}", exc_info=True)
        await bot.send_message(callback_query.from_user.id, f"⛔ حدث خطأ فادح أثناء محاولة بيع {symbol_key}.")

@dp.callback_query_handler(lambda c: c.data == "sell_all_active_trades")
async def sell_all_active_trades_handler(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id, "⏳ جارٍ بدء عملية البيع الجماعي...")
    await callback_query.message.delete_reply_markup()

    loading_msg = await callback_query.message.answer("⏳ جارٍ فحص الصفقات النشطة...")
    
    async with trade_lock:
        # نأخذ نسخة من القاموس بالكامل للعمل عليه بأمان
        trades_to_process = active_trades.copy()

    if not trades_to_process:
        await loading_msg.edit_text("⚠️ لا توجد صفقات نشطة لإغلاقها.")
        return

    await loading_msg.edit_text(f"⏳ سيتم محاولة إغلاق {len(trades_to_process)} صفقات نشطة...")
    
    sold_count = 0
    failed_count = 0

    # --- [بداية التعديل] ---
    for symbol_key, trade_data in trades_to_process.items():
        logger.info(f"Mass-Sell: Processing {symbol_key}...")
        try:
            # نستدعي الدالة الآمنة لكل صفقة
            # ستتولى هذه الدالة الإلغاء والبيع وإرسال التقرير
            await cancel_and_sell_market(symbol_key, trade_data, "إغلاق جماعي يدوي")
            sold_count += 1
        except Exception as e:
            logger.error(f"Mass-Sell: Failed to process {symbol_key}. Error: {e}")
            failed_count += 1
        
        await asyncio.sleep(1) # تأخير بسيط بين كل عملية بيع لتجنب الضغط على API
    # --- [نهاية التعديل] ---

    summary_message = (
        f"--- ✅ اكتملت عملية البيع الجماعي ---\n\n"
        f"- عدد الصفقات التي تم إرسال أمر إغلاق لها: {sold_count}\n"
        f"- عدد الصفقات التي فشلت معالجتها: {failed_count}"
    )
    
    await loading_msg.edit_text(summary_message)

@dp.message_handler(lambda message: message.text == "📈 عرض تاريخ الصفقات")
async def show_trade_history(message: types.Message):
           
    if not trade_history:
        await message.answer("⚠️ لا توجد صفقات مغلقة حتى الآن.")
        return

    # ترتيب الصفقات حسب الوقت (من الأحدث إلى الأقدم)
    sorted_trades = sorted(trade_history, key=lambda x: x['timestamp'], reverse=True)
    
    history_text = ""  # ✅ تهيئة المتغير
    for trade in sorted_trades:
        symbol = trade['symbol']
        emoji = "✅" if trade['profit_loss'] > 0 else "❌"
        history_text += (
            f"{emoji} {symbol}: "
            f"{'+' if trade['profit_loss'] > 0 else ''}{trade['profit_loss']:.2f}% | "
            f"⏱️ {trade['duration']}\n"
        )

    await message.answer(history_text)

def create_client():
    global use_testnet
    if use_testnet:
        return Client(TESTNET_API_KEY, TESTNET_API_SECRET, testnet=True)  # حساب تجريبي
    else:
        return Client(API_KEY, API_SECRET)  # حساب حقيقي

# هذا هو الزر الذي سيستخدمه المستخدم لتعيين مبلغ الصفقة
@dp.message_handler(lambda message: message.text == "💰 تخصيص رأس المال")
async def set_the_fixed_trade_amount_prompt(message: types.Message): # اسم جديد للدالة
    global fixed_trade_amount, client # تأكد أن client متاح هنا
    
    if message.chat.id not in ADMIN_USER_IDS_LIST: # تحقق من الأدمن
        await message.answer("🚫 ليس لديك صلاحية الوصول لهذه الإعدادات.")
        return
        
    try:
        balance_data = await asyncio.to_thread(client.get_asset_balance, asset='USDT')
        
        free_balance = float(balance_data['free']) if balance_data and 'free' in balance_data else 0.0
        
        current_amount_text = f"المبلغ الحالي لكل صفقة: {fixed_trade_amount} USDT" if fixed_trade_amount is not None else "لم يتم تحديد مبلغ ثابت لكل صفقة بعد."
        
        await message.answer(
            f"💰 رصيدك الحالي: {free_balance:.2f} USDT.\n"
            f"{current_amount_text}\n\n"
            "🔢 يرجى إدخال المبلغ الثابت (بالـ USDT):"
        )
        await MultipleTradesState.waiting_for_fixed_amount.set() # استخدام نفس الحالة
    except BinanceAPIException as e_binance: # التقاط أخطاء Binance API بشكل خاص
        logger.error(f"Binance API error in set_the_fixed_trade_amount_prompt: {e_binance}", exc_info=True)
        await message.answer(f"⚠️ خطأ من Binance أثناء جلب الرصيد: {e_binance.message}")
    except Exception as e:
        logger.error(f"Unexpected error in set_the_fixed_trade_amount_prompt: {e}", exc_info=True)
        await message.answer(f"⚠️ حدث خطأ غير متوقع: {str(e)}")

# معالج إدخال المبلغ الثابت (لديك بالفعل دالة مشابهة، سنبقيها ونبسطها إذا لزم الأمر)
@dp.message_handler(state=MultipleTradesState.waiting_for_fixed_amount)
async def process_fixed_amount_input(message: types.Message, state: FSMContext): # اسم جديد للدالة
    global fixed_trade_amount # المتغير الذي سنقوم بتعيينه

    try:
        input_value = message.text.strip()
        try:
            amount = float(input_value)
        except ValueError:
            await message.answer("⚠️ يرجى إدخال مبلغ رقمي صحيح (مثل '20' أو '15.5').")
            return # البقاء في نفس الحالة للسماح بإعادة المحاولة

        if amount <= 0:
            await message.answer("⚠️ يرجى إدخال مبلغ أكبر من الصفر.")
            return

        # التحقق من أن المبلغ لا يتجاوز الرصيد (اختياري هنا، لأن هذا إعداد عام)
        balance_data = await asyncio.to_thread(client.get_asset_balance, asset='USDT')
        free_balance = float(balance_data['free']) if balance_data and 'free' in balance_data else 0.0
        if amount > free_balance:
            await message.answer(f"⚠️ المبلغ المحدد ({amount:.2f} USDT) أكبر من رصيدك الحالي ({free_balance:.2f} USDT).")
            return

        fixed_trade_amount = amount

        settings_markup = ReplyKeyboardMarkup(resize_keyboard=True)
        settings_markup.add(KeyboardButton("📡 اختيار الفريم"))
        settings_markup.add(KeyboardButton("💵 تحديد الحساب"))
        settings_markup.add(KeyboardButton("⛔ تحديد وقف الخسارة"))
        settings_markup.add(KeyboardButton("🎯 تحديد هدف الإغلاق"))
        settings_markup.add(KeyboardButton("📊 الحد الأقصى للصفقات النشطة"))
        settings_markup.add(KeyboardButton("💰 تخصيص رأس المال"))
        settings_markup.add(KeyboardButton("🔙 العودة"))

        await message.answer(
            f"✅ تم تعيين المبلغ الثابت لكل صفقة إلى: {fixed_trade_amount:.2f} USDT.", 
            reply_markup=settings_markup # <-- استخدام لوحة مفاتيح الإعدادات
        )
        
        await state.finish()
    except Exception as e:
        await message.answer(f"⚠️ حدث خطأ غير متوقع: {str(e)}")
        await state.finish()

@dp.message_handler(lambda message: message.text == "📊 الحد الأقصى للصفقات النشطة")
async def set_max_concurrent_trades_prompt(message: types.Message):
    if message.chat.id not in ADMIN_USER_IDS_LIST: # تحقق من الأدمن
        await message.answer("🚫 ليس لديك صلاحية الوصول لهذه الإعدادات.")
        return
        
    try:
        # إرسال رسالة تطلب من المستخدم إدخال العدد
        await message.answer(
            "🔢 يرجى إدخال الحد الأقصى لعدد الصفقات النشطة التي ترغب بها")
        # الانتقال إلى حالة انتظار إدخال المستخدم
        await MultipleTradesState.waiting_for_max_concurrent_trades.set()
    except Exception as e:
        # في حالة حدوث أي خطأ، إعلام المستخدم وإنهاء العملية بأمان
        logging.error(f"Error in set_max_concurrent_trades_prompt: {e}")
        await message.answer(f"⚠️ حدث خطأ أثناء محاولة طلب إدخال الحد الأقصى للصفقات: {str(e)}")

@dp.message_handler(state=MultipleTradesState.waiting_for_max_concurrent_trades)
async def process_max_concurrent_trades_input(message: types.Message, state: FSMContext):
    global max_concurrent_trades # لا يزال يستخدم المتغير العام، يمكن تحسينه لاحقًا إذا أردت
    
    if message.chat.id not in ADMIN_USER_IDS_LIST: # تحقق من الأدمن
        await message.answer("🚫 ليس لديك صلاحية الوصول لهذه الإعدادات.")
        return
        
    try:
        user_input = message.text.strip()

        # 1. التحقق إذا كان الإدخال رقمًا صحيحًا
        if not user_input.isdigit():
            await message.answer("⚠️ الإدخال غير صالح. يرجى إدخال رقم صحيح موجب (مثل: 3).")
            # البقاء في نفس الحالة للسماح للمستخدم بإعادة المحاولة
            return

        num_trades = int(user_input)

        # 2. التحقق من أن الرقم أكبر من صفر
        if num_trades <= 0:
            await message.answer("⚠️ يجب أن يكون عدد الصفقات أكبر من الصفر.\n")
            # البقاء في نفس الحالة
            return

        # 3. (اختياري) وضع حد أقصى منطقي لعدد الصفقات
        MAX_ALLOWED_TRADES = 200 # مثال، يمكنك تعديل هذا الرقم
        if num_trades > MAX_ALLOWED_TRADES:
            await message.answer(f"⚠️ العدد كبير جدًا. الحد الأقصى المسموح به هو {MAX_ALLOWED_TRADES} صفقة.")
            # البقاء في نفس الحالة
            return
            
        # إذا نجحت كل التحققات
        max_concurrent_trades = num_trades

        settings_markup = ReplyKeyboardMarkup(resize_keyboard=True)
        settings_markup.add(KeyboardButton("📡 اختيار الفريم"))
        settings_markup.add(KeyboardButton("💵 تحديد الحساب"))  # زر تحديد الحساب
        settings_markup.add(KeyboardButton("⛔ تحديد وقف الخسارة"))  # زر تحديد وقف الخسارة
        settings_markup.add(KeyboardButton("🎯 تحديد هدف الإغلاق"))
        settings_markup.add(KeyboardButton("🕓 نوع وقف الخسارة (4 ساعات)"))
        settings_markup.add(KeyboardButton("🛡️ تأمين الوقف المتحرك (4 ساعات)"))
        settings_markup.add(KeyboardButton("📊 الحد الأقصى للصفقات النشطة"))
        settings_markup.add(KeyboardButton("💰 تخصيص رأس المال"))
        settings_markup.add(KeyboardButton("🔙 العودة"))  # زر العودة للقائمة الرئيسية

        if max_concurrent_trades == 1:
            await message.answer(
                f"✅ تم تعيين الحد الأقصى للصفقات النشطة إلى صفقة واحدة.",
                reply_markup=settings_markup # <-- استخدام لوحة مفاتيح الإعدادات
            )
        else:
            await message.answer(
                f"✅ تم تعيين الحد الأقصى للصفقات النشطة إلى: {max_concurrent_trades} صفقات.",
                reply_markup=settings_markup # <-- استخدام لوحة مفاتيح الإعدادات
            )
            
        await state.finish()

    except ValueError: # قد لا يتم الوصول إليه بسبب isdigit()، لكنه احتياط جيد
        logging.warning(f"ValueError during max_concurrent_trades input: {message.text}")
        await message.answer(
            "⚠️ حدث خطأ أثناء معالجة الرقم. يرجى التأكد من إدخال رقم صحيح.\n"
            "حاول مرة أخرى أو أرسل /cancel للإلغاء."
        )
    except Exception as e:
        logging.error(f"Unexpected error in process_max_concurrent_trades_input: {e}", exc_info=True)
        await message.answer(
            f"⚠️ حدث خطأ غير متوقع: {str(e)}.\n"
            "تم إلغاء العملية."
        )
        await state.finish() # إنهاء الحالة في حالة خطأ فادح

@dp.message_handler(lambda message: message.text == "🔙 العودة")
async def back_to_main_menu(message: types.Message, state: FSMContext):
    
    if message.chat.id not in ADMIN_USER_IDS_LIST:
        await message.answer("🚫 ليس لديك صلاحية لهذا الأمر.")
        return
    
    # إنهاء أي حالة FSM قد يكون المستخدم فيها (مثل انتظار إدخال رقم)
    await state.finish()
    
    # الحصول على لوحة المفاتيح الرئيسية وإرسالها
    main_keyboard = get_main_keyboard(message.from_user.id)
    
    await message.answer(
        "🏠 القائمة الرئيسية", 
        reply_markup=main_keyboard
    )

@dp.message_handler(lambda message: message.text == "⛔ تحديد وقف الخسارة")
async def set_stop_loss_prompt(message: types.Message):
    if message.chat.id not in ADMIN_USER_IDS_LIST:
        await message.answer("🚫 ليس لديك صلاحية الوصول لهذه الإعدادات.")
        return

    # عرض الإعداد الحالي
    if stop_loss_mode == 'auto':
        current_setting = "تلقائي (من الإشارة)"
    elif stop_loss_mode == 'disabled':
        current_setting = "بدون وقف خسارة"
    else: # custom
        current_setting = f"نسبة مئوية مخصصة: {custom_stop_loss_percentage}%"

    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("🚀 تلقائي (من الإشارة)", callback_data="set_sl:auto"),
        InlineKeyboardButton("✍️ نسبة مئوية مخصصة", callback_data="set_sl:custom"),
        InlineKeyboardButton("❌ بدون وقف خسارة", callback_data="set_sl:disabled")
    )
    
    await message.answer(
        f"**الإعداد الحالي:** `{current_setting}`.\n\n"
        "اختر طريقة تحديد وقف الخسارة:",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )

@dp.callback_query_handler(lambda c: c.data.startswith("set_sl:"))
async def process_stop_loss_selection(callback_query: types.CallbackQuery, state: FSMContext):
    global stop_loss_mode, custom_stop_loss_percentage
    
    choice = callback_query.data.split(":")[1]
    response_message = ""

    if choice == "auto":
        stop_loss_mode = "auto"
        response_message = "✅ **تم الضبط:** سيتم استخدام وقف الخسارة من الإشارة (تلقائي)."
        try:
            await callback_query.message.edit_text(response_message, parse_mode="Markdown")
        except MessageNotModified:
            # حل بديل: حذف الرسالة وإرسال واحدة جديدة
            await callback_query.message.delete()
            await bot.send_message(callback_query.from_user.id, response_message, parse_mode="Markdown")
        await bot.answer_callback_query(callback_query.id)

    elif choice == "disabled":
        stop_loss_mode = "disabled"
        response_message = "⚠️ **تم الضبط:** سيتم فتح الصفقات **بدون** وقف خسارة."
        try:
            await callback_query.message.edit_text(response_message, parse_mode="Markdown")
        except MessageNotModified:
            # حل بديل: حذف الرسالة وإرسال واحدة جديدة
            await callback_query.message.delete()
            await bot.send_message(callback_query.from_user.id, response_message, parse_mode="Markdown")
        await bot.answer_callback_query(callback_query.id)

    elif choice == "custom":
        # هنا سنطلب من المستخدم إدخال النسبة
        await bot.answer_callback_query(callback_query.id)
        await callback_query.message.edit_text(
            "✍️ يرجى إدخال نسبة وقف الخسارة المئوية (مثال: `2` لـ 2%، أو `1.5` لـ 1.5%):",
            parse_mode="Markdown"
        )
        await StopLossState.waiting_for_percentage.set()

@dp.message_handler(state=StopLossState.waiting_for_percentage)
async def process_custom_sl_percentage(message: types.Message, state: FSMContext):
    global stop_loss_mode, custom_stop_loss_percentage
    try:
        percentage = float(message.text.strip())
        if not (0 < percentage < 100):
             raise ValueError("Percentage out of range.")

        stop_loss_mode = "custom"
        custom_stop_loss_percentage = percentage
        
        response_message = f"✅ **تم الضبط:** سيتم احتساب وقف الخسارة بنسبة `{percentage}%` من سعر الدخول."
        
        await message.answer(response_message, parse_mode="Markdown")

    except ValueError:
        await message.answer("⚠️ إدخال غير صالح. يرجى إدخال رقم موجب (مثل 2.5).")

    finally:
        await state.finish()

@dp.message_handler(lambda message: message.text == "⚙️ إعدادات البوت")
async def bot_settings(message: types.Message):
    if message.chat.id not in ADMIN_USER_IDS_LIST: # تحقق من الأدمن
        await message.answer("🚫 ليس لديك صلاحية الوصول لهذه الإعدادات.")
        return
    
    settings_menu = ReplyKeyboardMarkup(resize_keyboard=True)
    settings_menu.add(KeyboardButton("📡 اختيار الفريم"))
    settings_menu.add(KeyboardButton("💵 تحديد الحساب"))  # زر تحديد الحساب
    settings_menu.add(KeyboardButton("⛔ تحديد وقف الخسارة"))  # زر تحديد وقف الخسارة
    settings_menu.add(KeyboardButton("🎯 تحديد هدف الإغلاق"))
    settings_menu.add(KeyboardButton("🕓 نوع وقف الخسارة (4 ساعات)"))
    settings_menu.add(KeyboardButton("🛡️ تأمين الوقف المتحرك (4 ساعات)"))
    settings_menu.add(KeyboardButton("📊 الحد الأقصى للصفقات النشطة"))
    settings_menu.add(KeyboardButton("💰 تخصيص رأس المال"))
    settings_menu.add(KeyboardButton("🔙 العودة"))  # زر العودة للقائمة الرئيسية
    
    await message.answer("⚙️ إعدادات البوت:", reply_markup=settings_menu)

# --- 1. المعالج الذي يعرض الخيارات (عند الضغط على الزر من ReplyKeyboard) ---
@dp.message_handler(lambda message: message.text.startswith("🛡️ تأمين الوقف المتحرك"))
async def toggle_trailing_sl_prompt(message: types.Message):
    
    if message.chat.id not in ADMIN_USER_IDS_LIST:
        await message.answer("🚫 ليس لديك صلاحية لهذا الأمر.")
        return
        
    # عرض الحالة الحالية
    current_status = "✅ مفعل (الوقف المرفوع سيكون مشروطًا)" if trailing_sl_is_conditional else "⚡️ معطل (الوقف المرفوع سيكون فوريًا)"
    
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ تفعيل الشرط", callback_data="set_trail_sl:true"),
        InlineKeyboardButton("⚡️ تعطيل الشرط", callback_data="set_trail_sl:false")
    )
    
    await message.answer(
        f"**إعداد تأمين الوقف المتحرك:** `{current_status}`\n\n"
        "عند تفعيل هذا الخيار، حتى بعد تحقيق TP1 ورفع الوقف، سيبقى الوقف الجديد مشروطًا بإغلاق شمعة 4 ساعات.",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )

# --- 2. معالج الـ callback الذي ينفذ التغيير ---
@dp.callback_query_handler(lambda c: c.data.startswith("set_trail_sl:"))
async def process_trailing_sl_selection(callback_query: types.CallbackQuery):
    global trailing_sl_is_conditional
    
    choice = callback_query.data.split(":")[1]
    
    if choice == "true":
        trailing_sl_is_conditional = True
        response_message = "✅ **تم التفعيل:** الوقف المتحرك (بعد TP1) سيكون الآن **مشروطًا** بإغلاق 4 ساعات."
    else: # choice == "false"
        trailing_sl_is_conditional = False
        response_message = "⚡️ **تم التعطيل:** الوقف المتحرك (بعد TP1) سيكون الآن **فوريًا** (سيتم وضع أمر OCO)."
        
    # تعديل الرسالة الأصلية لإظهار التغيير
    try:
        await callback_query.message.edit_text(response_message, parse_mode="Markdown")
    except MessageNotModified:
        # حل بديل: حذف الرسالة وإرسال واحدة جديدة
        await callback_query.message.delete()
        await bot.send_message(callback_query.from_user.id, response_message, parse_mode="Markdown")
    # إرسال تأكيد للمستخدم (العلامة الصغيرة في أعلى الشاشة)
    await bot.answer_callback_query(callback_query.id)

@dp.message_handler(lambda message: message.text == "📡 اختيار الفريم")
async def select_signal_source_prompt(message: types.Message):
    if message.chat.id not in ADMIN_USER_IDS_LIST:
        await message.answer("🚫 ليس لديك صلاحية لهذا الأمر.")
        return
    
    options_keyboard = InlineKeyboardMarkup(row_width=1)
    options_keyboard.add(
        InlineKeyboardButton("CryptoSignalProo", callback_data="set_source:5min_frame"),
        InlineKeyboardButton("MU7D", callback_data="set_source:both_channels"),
        InlineKeyboardButton("MDBORSA ", callback_data="set_source:buy_signals"),
        InlineKeyboardButton("🇧🇷 NOVO", callback_data="set_source:novo_signals")
    )
    
    current_source_name = ""
    for name, username in SIGNAL_SOURCES.items():
        if username == ACTIVE_BOT_USERNAME:
            current_source_name = name
            break
            
    current_frame_display = "غير محدد"
    if current_source_name == '5min_frame':
        current_frame_display = "CryptoSignalProo"
    elif current_source_name == 'both_channels':
        current_frame_display = "MU7D"
    elif current_source_name == 'buy_signals':
        current_frame_display = "MDBORSA"
    elif current_source_name == 'novo_signals':
        current_frame_display = "🇧🇷 NOVO"
        
    await message.answer(
        f"**الإعداد الحالي:** يتم تحليل إشارات **{current_frame_display}**.\n\n"
        "اختر الإطار الزمني (الفريم) الذي تريد التداول عليه:",
        reply_markup=options_keyboard,
        parse_mode="Markdown"
    )

@dp.callback_query_handler(lambda c: c.data.startswith("set_source:"))
async def process_signal_source_selection(callback_query: types.CallbackQuery):
    global ACTIVE_BOT_USERNAME, client_telegram
    choice_key = callback_query.data.split(":")[1]
   
    if choice_key in SIGNAL_SOURCES:
        new_username = SIGNAL_SOURCES[choice_key]
       
        if choice_key == 'both_channels':
            # معالجة خاصة للقناتين معاً
            if new_username == ACTIVE_BOT_USERNAME:
                await bot.answer_callback_query(callback_query.id, text="قنوات MU7D يتم مراقبتها بالفعل", show_alert=True)
                return
               
            logger.info(f"Switching signal source from '{ACTIVE_BOT_USERNAME}' to both channels without disconnecting.")
            client_telegram.remove_event_handler(new_signal_handler)
            client_telegram.add_event_handler(new_signal_handler, events.NewMessage(chats=new_username))
            
            ACTIVE_BOT_USERNAME = new_username
           
            # التحقق من الاتصال بالقناتين
            try:
                for chat_id in new_username:
                    entity = await client_telegram.get_entity(chat_id)
                    logger.info(f"Successfully connected to: {entity.title} (ID: {entity.id})")
            except Exception as e:
                logger.error(f"Failed to connect to channels: {e}")
               
            response_message = "✅ تم التبديل. البوت يراقب الآن قنوات MU7D"
       
        elif choice_key == 'buy_signals':
            new_sources = SIGNAL_SOURCES[choice_key]
            
            if new_sources == ACTIVE_BOT_USERNAME:
                await bot.answer_callback_query(callback_query.id, text="إشارات MDBORSA مفعلة بالفعل!", show_alert=True)
                return
                
            logger.info(f"Switching to MDBORSA signals channels: {new_sources}")
            client_telegram.remove_event_handler(new_signal_handler)
            client_telegram.add_event_handler(new_signal_handler, events.NewMessage(chats=new_sources))
            
            ACTIVE_BOT_USERNAME = new_sources
            
            # التحقق من الاتصال
            connected_count = 0
            for chat_id in new_sources:
                try:
                    entity = await client_telegram.get_entity(chat_id)
                    logger.info(f"Successfully connected to MDBORSA channel: {entity.title} (ID: {entity.id})")
                    connected_count += 1
                except Exception as e:
                    logger.warning(f"Could not verify connection to {chat_id}: {e}")
            
            response_message = "✅ تم التبديل. البوت يراقب الآن قنوات MDBORSA."
            
            try:
                await callback_query.message.edit_text(response_message, parse_mode="Markdown")
            except MessageNotModified:
                # حل بديل: حذف الرسالة وإرسال واحدة جديدة
                await callback_query.message.delete()
                await bot.send_message(callback_query.from_user.id, response_message, parse_mode="Markdown")
            
            await bot.answer_callback_query(callback_query.id)
        
        elif choice_key == 'novo_signals':
            new_sources = SIGNAL_SOURCES[choice_key]
            
            if new_sources == ACTIVE_BOT_USERNAME:
                await bot.answer_callback_query(callback_query.id, text="إشارات NOVO مفعلة بالفعل!", show_alert=True)
                return
                
            logger.info(f"Switching to NOVO signals channels: {new_sources}")
            client_telegram.remove_event_handler(new_signal_handler)
            client_telegram.add_event_handler(new_signal_handler, events.NewMessage(chats=new_sources))
            
            ACTIVE_BOT_USERNAME = new_sources
            
            # التحقق من الاتصال بالقنوات
            try:
                if isinstance(new_sources, list):
                    for chat_id in new_sources:
                        try:
                            entity = await client_telegram.get_entity(chat_id)
                            logger.info(f"Successfully connected to NOVO channel: {entity.title} (ID: {entity.id})")
                        except Exception as e:
                            logger.warning(f"Could not verify connection to {chat_id}: {e}")
                    response_message = f"✅ تم التبديل. البوت يراقب الآت اشارات NOVO"
                else:
                    entity = await client_telegram.get_entity(new_sources)
                    logger.info(f"Successfully connected to NOVO channel: {entity.title} (ID: {entity.id})")
                    response_message = f"✅ تم التبديل. البوت يراقب الآن قناة إشارات NOVO البرازيلية."
            except Exception as e:
                logger.error(f"Error checking NOVO signals channels: {e}")
                response_message = "✅ تم التبديل. البوت يراقب الآن قنوات إشارات NOVO البرازيلية."
            
            # إضافة timestamp لتجنب MessageNotModified
            import time
            response_message += f"\n⏰ {int(time.time())}"
            
            try:
                await callback_query.message.edit_text(response_message, parse_mode="Markdown")
            except MessageNotModified:
                await callback_query.message.delete()
                await bot.send_message(callback_query.from_user.id, response_message, parse_mode="Markdown")
            
            await bot.answer_callback_query(callback_query.id)
    
        else:
            # معالجة القناة الواحدة
            if new_username == ACTIVE_BOT_USERNAME:
                await bot.answer_callback_query(callback_query.id, text="هذا الفريم مفعل بالفعل!", show_alert=True)
                return
                
            logger.info(f"Switching signal source from '{ACTIVE_BOT_USERNAME}' to '{new_username}' without disconnecting.")
            client_telegram.remove_event_handler(new_signal_handler)
            client_telegram.add_event_handler(new_signal_handler, events.NewMessage(chats=new_username))
            
            ACTIVE_BOT_USERNAME = new_username
            
            # التحقق من الاتصال
            try:
                entity = await client_telegram.get_entity(new_username)
                logger.info(f"Successfully connected to: {entity.title} (ID: {entity.id})")
            except Exception as e:
                logger.error(f"Failed to connect to {new_username}: {e}")
                
            if choice_key == '5min_frame':
                response_message = "✅ تم التبديل. البوت يعمل الآن على فريم 5 دقائق."
            else:
                response_message = "✅ تم التبديل بنجاح."
        
        try:
            await callback_query.message.edit_text(response_message, parse_mode="Markdown")
        except MessageNotModified:
            # حل بديل: حذف الرسالة وإرسال واحدة جديدة
            await callback_query.message.delete()
            await bot.send_message(callback_query.from_user.id, response_message, parse_mode="Markdown")
        await bot.answer_callback_query(callback_query.id)
    else:
        await bot.answer_callback_query(callback_query.id, text="⚠️ اختيار غير صالح.", show_alert=True)

@dp.message_handler(lambda message: message.text == "💵 تحديد الحساب")
async def select_account_type_prompt(message: types.Message):
    if message.chat.id not in ADMIN_USER_IDS_LIST:
        await message.answer("🚫 ليس لديك صلاحية الوصول لهذه الإعدادات.")
        return

    # تحديد أي حساب هو النشط حاليًا لإظهاره للمستخدم
    current_account = "تجريبي" if use_testnet else "حقيقي"
    
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ حساب حقيقي", callback_data="set_account:real"),
        InlineKeyboardButton("🧪 حساب تجريبي", callback_data="set_account:testnet")
    )
    
    await message.answer(
        f"**الإعداد الحالي:** الحساب `{current_account}` مفعل.\n\n"
        "اختر نوع الحساب الذي تريد التداول عليه:",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )

@dp.callback_query_handler(lambda c: c.data.startswith("set_account:"))
async def process_account_selection(callback_query: types.CallbackQuery):
    global use_testnet, client, account_switch_lock
    
    async with account_switch_lock:
        logger.info("Account switch process started, lock acquired.")
        
        choice = callback_query.data.split(":")[1]
    
        if choice == "real":
            if not use_testnet:
                await bot.answer_callback_query(callback_query.id, "الحساب الحقيقي مفعل بالفعل!", show_alert=True)
                return
            
            # الرد على الكويري هنا لمنع الانتظار الطويل أثناء محاولة الاتصال
            await bot.answer_callback_query(callback_query.id, "⏳ جارٍ التبديل إلى الحساب الحقيقي...")

            try:
                logger.info("Attempting to switch to REAL account...")
                new_client = Client(API_KEY, API_SECRET)
                new_client.ping()
                
                if client:
                    await asyncio.to_thread(client.close_connection)
                client = new_client
                use_testnet = False
                
                response_message = "✅ **تم التبديل بنجاح!**\nالحساب **الحقيقي** أصبح فعالاً."
                logging.info("Switched to Real account successfully.")
                try:
                    await callback_query.message.edit_text(response_message, parse_mode="Markdown")
                except MessageNotModified:
                    # حل بديل: حذف الرسالة وإرسال واحدة جديدة
                    await callback_query.message.delete()
                    await bot.send_message(callback_query.from_user.id, response_message, parse_mode="Markdown")

            except Exception as e:
                logger.error(f"Failed to switch to Real Account: {e}", exc_info=True)
                error_message = "⛔ **فشل التبديل إلى الحساب الحقيقي!**\n\n" \
                                "يرجى التأكد من صلاحية مفاتيح API الخاصة بالحساب الحقيقي."
                # نرسل رسالة جديدة هنا لأن الرسالة الأصلية قد تم تعديلها أو لا تزال موجودة
                await callback_query.message.answer(error_message, parse_mode="Markdown")
    
        elif choice == "testnet":
            if use_testnet:
                await bot.answer_callback_query(callback_query.id, "الحساب التجريبي مفعل بالفعل!", show_alert=True)
                return

            # الرد على الكويري هنا لمنع الانتظار الطويل أثناء محاولة الاتصال
            await bot.answer_callback_query(callback_query.id, "⏳ جارٍ محاولة التبديل إلى الحساب التجريبي...")

            try:
                logger.info("Attempting to switch to TESTNET account...")
                testnet_client = Client(TESTNET_API_KEY, TESTNET_API_SECRET, testnet=True)
                testnet_client.ping()

                if client:
                    await asyncio.to_thread(client.close_connection)
                client = testnet_client
                use_testnet = True
                
                response_message = "✅ **تم التبديل بنجاح!**\nالحساب **التجريبي** أصبح فعالاً."
                logging.info("Switched to Testnet account successfully.")
                try:
                    await callback_query.message.edit_text(response_message, parse_mode="Markdown")
                except MessageNotModified:
                    # حل بديل: حذف الرسالة وإرسال واحدة جديدة
                    await callback_query.message.delete()
                    await bot.send_message(callback_query.from_user.id, response_message, parse_mode="Markdown")

            except Exception as e:
                logger.error(f"Failed to switch to Testnet account: {e}", exc_info=False)
                error_message = "⚠️ **فشل التبديل إلى الحساب التجريبي!**\n\n" \
                                "يبدو أن الحساب التجريبي غير متاح حاليًا. **سيظل البوت يعمل على الحساب الحقيقي.**"
                
                # نرسل رسالة جديدة هنا لإبلاغ المستخدم
                await callback_query.message.answer(error_message, parse_mode="Markdown")

@dp.message_handler(lambda message: message.text == "✍️ أدخل قيمة وقف الخسارة")
async def ask_stop_loss_value(message: types.Message):
    if message.chat.id not in ADMIN_USER_IDS_LIST: # تحقق من الأدمن
        await message.answer("🚫 ليس لديك صلاحية الوصول لهذه الإعدادات.")
        return
        
    await message.answer("✍️ يرجى إدخال قيمة وقف الخسارة:")

@dp.message_handler(lambda message: message.text == "🎯 تحديد هدف الإغلاق")
async def set_exit_target_prompt(message: types.Message):
    if message.chat.id not in ADMIN_USER_IDS_LIST:
        await message.answer("🚫 ليس لديك صلاحية لهذا الأمر.")
        return
    global exit_at_target_number
    
    exit_options_keyboard = InlineKeyboardMarkup(row_width=1)
    exit_options_keyboard.add(
        InlineKeyboardButton("🎯 الإغلاق عند الهدف الأول", callback_data="set_exit:1"),
        InlineKeyboardButton("🎯 الإغلاق عند الهدف الثاني", callback_data="set_exit:2"),
        InlineKeyboardButton("🎯 الإغلاق عند الهدف الثالث", callback_data="set_exit:3"),
        InlineKeyboardButton("🎯 تفعيل التتبع اللانهائي (TP∞)", callback_data="set_exit:10"),
    )

    current_setting_text = f"الإعداد الحالي: "
    if exit_at_target_number == "10":
        current_setting_text += "تفعيل التتبع اللانهائي (TP∞)."
    else:
        current_setting_text += f"الإغلاق المباشر عند الهدف رقم {exit_at_target_number}."

    await message.answer(
        f"اختر استراتيجية الخروج من الصفقات:\n\n{current_setting_text}",
        reply_markup=exit_options_keyboard
    )

@dp.callback_query_handler(lambda c: c.data.startswith("set_exit:"))
async def process_exit_target_selection(callback_query: types.CallbackQuery):
    global exit_at_target_number

    choice = callback_query.data.split(":")[1]
    response_message = ""
    
    if choice == '10':
        exit_at_target_number = 10  # عدد الأهداف الكلي، التتبع اللانهائي
        response_message = "✅ **تم تفعيل التتبع اللانهائي (TP∞):**\nسيتم إضافة أهداف جديدة تلقائيًا بعد TP3 ورفع وقف الخسارة باستمرار حتى الإغلاق اليدوي."

    elif choice == '3':
        exit_at_target_number = 3
        response_message = (
            "✅ **تم تفعيل الوضع التلقائي:**\n"
            "سيتم رفع وقف الخسارة عند TP1 و TP2، ثم إغلاق الصفقة عند TP3."
        )
    else:
        try:
            target_num_int = int(choice)
            if 1 <= target_num_int <= 2:
                exit_at_target_number = target_num_int
                response_message = f"✅ **تم الضبط:**\nسيتم إغلاق الصفقات مباشرةً عند تحقيق الهدف رقم `{target_num_int}`."
            else:
                response_message = "⚠️ اختيار غير صالح."
        except ValueError:
            response_message = "⚠️ اختيار غير صالح."

    try:
        await callback_query.message.edit_text(response_message, parse_mode="Markdown")
    except MessageNotModified:
        # حل بديل: حذف الرسالة وإرسال واحدة جديدة
        await callback_query.message.delete()
        await bot.send_message(callback_query.from_user.id, response_message, parse_mode="Markdown")
    await bot.answer_callback_query(callback_query.id)

def format_time(seconds):
    """
    تحويل الثواني إلى صيغة مختصرة مثل "1H 3m 5s" أو "55m 26s".
    """
    days = seconds // (24 * 3600)
    hours = (seconds % (24 * 3600)) // 3600
    minutes = (seconds % 3600) // 60
    #remaining_seconds = seconds % 60

    duration_parts = []
    if days > 0:
        duration_parts.append(f"{int(days)}D")
    if hours > 0:
        duration_parts.append(f"{int(hours)}H")
    if minutes > 0:
        duration_parts.append(f"{int(minutes)}m")
    #if remaining_seconds > 0:
    #    duration_parts.append(f"{int(remaining_seconds)}s")

    return " ".join(duration_parts) if duration_parts else "0s"

async def periodic_symbol_info_updater(interval_seconds=24*60*60):
    logger.info(f"Periodic symbol info updater started. Will update every {interval_seconds / 3600:.1f} hours for TARGET symbols.") # تعديل بسيط في السجل
    while True:
        await fetch_and_cache_all_symbol_info() # <--- تأكد أن هذا هو اسم الدالة التي عدلتها لتعمل على halal_symbols_set
        await asyncio.sleep(interval_seconds)

async def calculate_order_quantity(symbol: str, current_market_price: float) -> dict | None:
    global fixed_trade_amount, max_concurrent_trades, active_trades, client
    # symbol_info_cache لا يُستخدم مباشرة هنا بعد الآن، بل عبر get_complete_symbol_info

    try:
        symbol_cleaned_for_api = symbol.replace('/', '')
        
        # --- جلب معلومات الرمز الكاملة ---
        symbol_details = await get_complete_symbol_info(symbol_cleaned_for_api, symbol_for_log=symbol)
        if not symbol_details:
            # get_complete_symbol_info سترسل رسالة الخطأ
            return None 
        
        if symbol_details.get('status') != 'TRADING':
            await send_result_to_users(f"⚠️ الرمز {symbol} غير قابل للتداول (حالة: {symbol_details.get('status')}).")
            return None
        
        min_qty = symbol_details['min_qty']
        step_size = symbol_details['step_size'] # لا يزال مفيدًا للتحقق الأولي
        min_notional = symbol_details['min_notional']
        # price_precision و quantity_precision موجودة الآن في symbol_details

        # --- بداية منطق حساب amount_to_use (كما هو لديك) ---
        balance_data = await retry_blocking_api_call(client.get_asset_balance, asset='USDT', symbol_for_log="USDT Balance")
        if not balance_data:
            await send_result_to_users(f"⚠️ فشل جلب رصيد USDT (CalcQty).")
            return None
        usdt_balance = float(balance_data.get('free', '0'))

        async with trade_lock: # قراءة آمنة لـ active_trades
            active_trade_count = len(active_trades)
        
        amount_to_use = 0.0
        if fixed_trade_amount is not None and fixed_trade_amount > 0 :
            amount_to_use = fixed_trade_amount
        else: 
            available_slots = max(1, max_concurrent_trades - active_trade_count)
            amount_to_use = usdt_balance / available_slots
        
        if amount_to_use <= 0 :
            await send_result_to_users(f"⚠️ المبلغ المحسوب للاستخدام ({amount_to_use:.2f} USDT) غير صالح لـ {symbol}.")
            return None
        if amount_to_use > usdt_balance:
            await send_result_to_users(f"⚠️ المبلغ المحدد للاستخدام ({amount_to_use:.2f} USDT) لـ {symbol} أكبر من الرصيد المتاح ({usdt_balance:.2f} USDT).")
            return None
        # --- نهاية منطق حساب amount_to_use ---
        
        if current_market_price <= 0:
            await send_result_to_users(f"⚠️ سعر السوق الحالي لـ {symbol} ({current_market_price}) غير صالح.")
            return None
        
        if step_size <= 0: # هذا لا يفترض أن يحدث
            logger.error(f"CalcQty: Invalid step_size {step_size} from symbol_details for {symbol_cleaned_for_api}.")
            await send_result_to_users(f"⚠️ حجم الخطوة (step_size) للرمز {symbol} غير صالح ({step_size}).")
            return None
        
        if amount_to_use < min_notional:
            await send_result_to_users(f"⚠️ المبلغ المخصص للصفقة ({amount_to_use:.2f} USDT) أقل من الحد الأدنى ({min_notional:.2f} USDT) لـ {symbol}.")
            return None
        
        # حساب الكمية الأولية
        initial_quantity = amount_to_use / current_market_price
        
        # ضبط الكمية باستخدام دالة adjust_quantity و symbol_details الكاملة
        quantity_adjusted = adjust_quantity(initial_quantity, symbol_details)
        
        # التحقق النهائي من MIN_NOTIONAL بعد تعديل الكمية
        final_order_value = quantity_adjusted * current_market_price
        if final_order_value < min_notional:
            logger.warning(f"CalcQty: Order value {final_order_value:.2f} for {symbol} is below min_notional {min_notional:.2f} after initial adjustment. Trying to adjust for min_notional.")
            qty_for_min_notional = (Decimal(str(min_notional)) / Decimal(str(current_market_price))) * Decimal("1.001") # هامش صغير
            quantity_adjusted = adjust_quantity(float(qty_for_min_notional), symbol_details) # استدعاء adjust_quantity مرة أخرى
            final_order_value = quantity_adjusted * current_market_price

            if final_order_value < min_notional:
                await send_result_to_users(f"⚠️ الرصيد غير كافٍ لتلبية الحد الأدنى ({min_notional:.2f} USDT) حتى بعد تعديل الكمية لـ {symbol}. القيمة النهائية: {final_order_value:.2f}")
                return None
        
        if quantity_adjusted <= 0: # تحقق إضافي
            await send_result_to_users(f"⚠️ الكمية النهائية المحسوبة لـ {symbol} هي صفر أو أقل ({quantity_adjusted}).")
            return None

        # إرجاع القاموس الكامل لمعلومات الرمز مع الكمية المحسوبة
        return {
            'quantity': quantity_adjusted,
            'order_value': round(final_order_value, 2),
            'symbol_info_dict': symbol_details # <--- مهم جدًا لتمريره إلى open_order
        }

    except BinanceAPIException as e_main_api:
        logger.error(f"CalcQty: Binance API error for {symbol}: {e_main_api}", exc_info=True)
        await send_result_to_users(f"⚠️ خطأ API أثناء حساب الكمية لـ {symbol}: {e_main_api.message}")
        return None
    except Exception as e_main:
        logger.error(f"CalcQty: Unexpected error for {symbol}: {e_main}", exc_info=True)
        await send_result_to_users(f"⚠️ خطأ غير متوقع أثناء حساب الكمية لـ {symbol}: {str(e_main)}")
        return None

# تعريف الدالة adjust_to_tick_size
def adjust_to_tick_size(price, tick_size):
    """
    ضبط السعر ليكون مضاعفًا لـ tick_size.
    """
    return round(int(price / tick_size) * tick_size, 8)

async def cleanup_pending_orders(symbol):
    try:
        orders = await asyncio.to_thread(client.get_open_orders, symbol=symbol.replace('/', ''))
        for o in orders:
            await asyncio.to_thread(client.cancel_order,
                                  symbol=symbol.replace('/', ''),
                                  orderId=o['orderId'])
    except Exception as e:
        logging.warning(f"Failed to cleanup orders: {str(e)}")

def adjust_quantity(quantity_to_adjust: float, symbol_info_dict: dict) -> float:
    """
    Adjusts the quantity to comply with LOT_SIZE filter (step_size and min_qty)
    and formats it to the correct quantity_precision.
    """
    step_size = Decimal(str(symbol_info_dict['step_size']))
    min_qty = Decimal(str(symbol_info_dict['min_qty']))
    quantity_precision = symbol_info_dict['quantity_precision'] 

    quantity_decimal = Decimal(str(quantity_to_adjust))

    if quantity_decimal < min_qty:
        logger.debug(f"AdjustQuantity: Requested quantity {quantity_decimal} is less than min_qty {min_qty}. Using min_qty.")
        quantity_decimal = min_qty
    
    adjusted_quantity_decimal = (quantity_decimal // step_size) * step_size
    
    if adjusted_quantity_decimal < min_qty and quantity_decimal >= min_qty :
         logger.debug(f"AdjustQuantity: Rounded quantity {adjusted_quantity_decimal} is less than min_qty {min_qty} (original was {quantity_decimal}). Forcing to min_qty.")
         adjusted_quantity_decimal = min_qty
    
    if quantity_precision == 0:
        quantizer = Decimal('1')
    else:
        quantizer = Decimal('1e-' + str(quantity_precision))
    
    formatted_quantity_decimal = adjusted_quantity_decimal.quantize(quantizer, rounding=ROUND_DOWN)
    
    logger.debug(f"AdjustQuantity: Original: {quantity_to_adjust}, MinQty: {min_qty}, Step: {step_size}, AdjustedDecimal: {adjusted_quantity_decimal}, FormattedDecimal: {formatted_quantity_decimal} (Precision: {quantity_precision})")
    return float(formatted_quantity_decimal)

async def parse_new_format_signal(message_text: str):
    """
    تحلل رسائل الإشارات الجديدة التي تبدأ بـ "صفقة جديدة عملة".
    """
    global max_concurrent_trades, active_trades, trade_lock, currency_last_seen, stop_loss_mode, custom_stop_loss_percentage, logger, use_conditional_sl, account_switch_lock

    logger.info(f"--- Parsing New Format Signal ---")
    logger.debug(f"Received message snippet: {message_text[:150]}")
    async with account_switch_lock:
        try:
            if not message_text.startswith("📢 صفقة جديدة 🔥"):
               logger.debug("Message does not match new format signal prefix. Ignoring.")
               return None

           # التحقق من الحد الأقصى للصفقات النشطة
            async with trade_lock:
               active_trade_count = len(active_trades)
               if active_trade_count >= max_concurrent_trades:
                   log_msg = f"Cannot open new trade. Max concurrent trades ({max_concurrent_trades}) reached. Active: {active_trade_count}."
                   user_msg = f"⚠️ تم الوصول للحد الأقصى للصفقات النشطة ({max_concurrent_trades}). لا يمكن فتح صفقة جديدة حاليًا."
                   if max_concurrent_trades == 1:
                       user_msg = "⛔ لا يمكن فتح صفقة جديدة. البوت مضبوط على صفقة واحدة نشطة كحد أقصى، ويوجد بالفعل صفقة نشطة."
                   logger.info(log_msg)
                   await send_result_to_users(user_msg)
                   return None

            try:
                # استخراج البيانات للنمط الجديد
                match_symbol_raw = re.search(r"عملة\s+([A-Z0-9]+/USDT)", message_text)
                match_entry_price_text = re.search(r"السعر الحالي:\s*([\d\.eE\-+]+)", message_text)
                raw_targets_as_text = re.findall(r"الهدف\s+\d+:\s*([\d\.eE\-+]+)", message_text)
                match_sl_text_from_signal = re.search(r"وقف الخسارة:.*?اسفل\s*([\d\.eE\-+]+)", message_text)
               
                # استخراج البيانات للنمط الجديد
                match_symbol_raw = re.search(r"عملة\s+([A-Z0-9]+/USDT)", message_text)
                match_entry_price_text = re.search(r"السعر الحالي:\s*([\d\.eE\-+]+)", message_text)
                raw_targets_as_text = re.findall(r"الهدف\s+\d+:\s*([\d\.eE\-+]+)", message_text)
                match_sl_text_from_signal = re.search(r"وقف الخسارة:.*?اسفل\s*([\d\.eE\-+]+)", message_text)
               
                # القرار الآن يعتمد على المتغير العام، وليس على نص المنشور
                is_4h_conditional_sl = use_conditional_sl 

                if is_4h_conditional_sl:
                    logger.info("Conditional SL mode is ACTIVE via bot settings.")
                else:
                    logger.info("Immediate SL mode is ACTIVE via bot settings.")

                if not (match_symbol_raw and match_entry_price_text and raw_targets_as_text):
                    log_msg = f"Parsing failed: Missing symbol, entry price, or targets. Snippet: {message_text[:250]}"
                    logger.warning(log_msg)
                    await send_result_to_users(f"Parsing failed: Missing symbol, entry price, or targets. Snippet: {message_text[:250]}")
                    return None

                symbol_name_raw = match_symbol_raw.group(1).strip().upper()  # مثال: METIS/USDT
                symbol_formatted = symbol_name_raw  # نستخدمه كما هو

                # --- التحقق من العملة الحلال ---
                base_asset_from_signal = ""
                if "/" in symbol_name_raw and symbol_name_raw.endswith("/USDT"):
                    base_asset_from_signal = symbol_name_raw.split("/")[0]  # يأخذ الجزء قبل /USDT
                else:
                    logger.warning(f"Signal for {symbol_name_raw} is not a valid /USDT pair. Halal check cannot be performed accurately. Ignoring signal.")
                    await send_result_to_users(f"🚫 تم تجاهل إشارة لـ {symbol_name_raw} لأنها ليست زوج USDT صالح للتحقق.")
                    return None

                if halal_symbols_set:
                    if base_asset_from_signal not in halal_symbols_set:
                        log_msg = f"Signal for {symbol_name_raw} (Base: {base_asset_from_signal}) is NOT in the halal symbols list. Ignoring signal."
                        logger.info(log_msg)
                        return None
                    else:
                        logger.info(f"Signal for {symbol_name_raw} (Base: {base_asset_from_signal}) is in the halal symbols list. Proceeding.")

                entry_price_from_signal = 0.0
                targets_from_signal_prices = []
                try:
                    entry_price_from_signal = float(match_entry_price_text.group(1))
                    targets_from_signal_prices = sorted([float(t) for t in raw_targets_as_text])
                except ValueError:
                    log_msg = f"Error converting signal data (price/targets) to numbers for {symbol_name_raw}. Snippet: {message_text[:250]}"
                    logger.error(log_msg, exc_info=True)
                    await send_result_to_users(f"⚠️ خطأ في تحويل بيانات الإشارة (السعر/الأهداف) إلى أرقام للعملة {symbol_name_raw}.")
                    return None
 
                if not targets_from_signal_prices:
                    logger.warning(f"No targets found in signal for {symbol_name_raw}.")
                    await send_result_to_users(f"⚠️ لم يتم العثور على أهداف في الإشارة للعملة {symbol_name_raw}.")
                    return None
                
                # التحقق من وجود صفقة نشطة لنفس الرمز
                async with trade_lock:
                    if symbol_formatted in active_trades:
                        log_msg = f"Active trade already exists for {symbol_formatted}. Ignoring new signal."
                        logger.info(log_msg)
                        await send_result_to_users(f"⛔ يوجد صفقة نشطة بالفعل للرمز {symbol_formatted}. تم تجاهل الإشارة الجديدة.")
                        return None
                
                # تحديد وقف الخسارة الأولي بناءً على stop_loss_mode
                initial_sl_to_use = None
                sl_source_info = ""

                if stop_loss_mode == "auto":
                    if match_sl_text_from_signal:
                        try:
                            initial_sl_to_use = float(match_sl_text_from_signal.group(1))
                            sl_source_info = "من الإشارة (تلقائي)"
                            logger.info(f"AUTO mode: Using SL {initial_sl_to_use} from signal for {symbol_formatted}.")
                        except ValueError:
                            logger.error(f"AUTO mode: Error converting SL from signal for {symbol_formatted}. SL text: '{match_sl_text_from_signal.group(1)}'. No SL.", exc_info=True)
                            await send_result_to_users(f"⚠️ خطأ قراءة SL من إشارة {symbol_formatted} (تلقائي). لن يوضع SL.")
                    else:
                        sl_source_info = "لا يوجد (لم يذكر في الإشارة - تلقائي)"
                        logger.warning(f"AUTO mode: SL not in signal for {symbol_formatted}. No SL.")
                        await send_result_to_users(f"⚠️ لم يوجد SL في إشارة {symbol_formatted} (تلقائي). لن يوضع SL.")
               
                elif stop_loss_mode == "custom":
                    if custom_stop_loss_percentage is not None:
                        initial_sl_to_use = entry_price_from_signal * (1 - (custom_stop_loss_percentage / 100))
                        sl_source_info = f"محسوب {custom_stop_loss_percentage}% (مخصص)"
                        logger.info(f"CUSTOM mode: Calculated SL {initial_sl_to_use} for {symbol_formatted}.")
                    else:
                        default_custom_perc = 5.0
                        initial_sl_to_use = entry_price_from_signal * (1 - (default_custom_perc / 100))
                        sl_source_info = f"محسوب {default_custom_perc}% (مخصص افتراضي)"
                        logger.warning(f"CUSTOM mode: No custom % for {symbol_formatted}. Defaulting to {default_custom_perc}% SL: {initial_sl_to_use}.")
                        await send_result_to_users(f"⚠️ SL مخصص بدون نسبة لـ {symbol_formatted}. استخدام {default_custom_perc}% افتراضي.")
                
                elif stop_loss_mode == "disabled":
                    initial_sl_to_use = None
                    sl_source_info = "معطل من الإعدادات"
                    logger.info(f"DISABLED mode: No SL for {symbol_formatted}.")
                    await send_result_to_users(f"⚠️ وضع وقف الخسارة 'معطل' لـ {symbol_formatted}. لن يتم وضع SL.")

                # التحققات النهائية على وقف الخسارة والأهداف
                if initial_sl_to_use is not None and initial_sl_to_use >= entry_price_from_signal:
                    log_msg = f"Final SL ({initial_sl_to_use}) for {symbol_formatted} is not below entry ({entry_price_from_signal}). Ignoring."
                    logger.warning(log_msg)
                    await send_result_to_users(f"⚠️ وقف الخسارة المحدد ({initial_sl_to_use:.8f}) لـ {symbol_formatted} غير صالح. تم تجاهل الإشارة.")
                    return None
                   
                if any(tp <= entry_price_from_signal for tp in targets_from_signal_prices):
                    log_msg = f"Targets for {symbol_formatted} are not all above entry price. Ignoring."
                    logger.warning(log_msg)
                    await send_result_to_users(f"⚠️ أهداف {symbol_formatted} غير صالحة. تم تجاهل الإشارة.")
                    return None

                sl_display_text = f"{initial_sl_to_use:.8f}" if initial_sl_to_use is not None else "لا يوجد"
                conditional_sl_text = " (مشروط بـ 4 ساعات)" if is_4h_conditional_sl and initial_sl_to_use is not None else ""
               
                log_final_info = f"Processing valid signal: {symbol_formatted}, Entry: {entry_price_from_signal}, SL: {sl_display_text}{conditional_sl_text}, Targets: {targets_from_signal_prices}, SL Source: {sl_source_info}"
                logger.info(log_final_info)

                # ✅ التحقق من السعر الحالي مقارنة بسعر الدخول
                try:
                    allowed_percentage_diff = 0.004 
                    symbol_api_format = symbol_formatted.replace('/', '')
                    current_price = live_prices.get(symbol_api_format) or await get_current_price(symbol_api_format)

                    logger.info(f"🚨 السعر الحالي لـ {symbol_formatted}: {current_price}, سعر الدخول من الإشارة: {entry_price_from_signal}")
                    
                    if current_price:
                        min_allowed_price = entry_price_from_signal * (1 - allowed_percentage_diff)
                        max_allowed_price = entry_price_from_signal * (1 + allowed_percentage_diff)
                        if not (min_allowed_price <= current_price <= max_allowed_price):
                            logger.warning(f"❌ تم تجاهل إشارة {symbol_formatted} بسبب انحراف السعر الحالي ({current_price}) خارج النطاق المسموح به [{min_allowed_price} - {max_allowed_price}]")
                            await send_result_to_users(
                                f"🚫 **تم تجاهل إشارة {symbol_formatted}**\n"
                                f"⚠️ السعر الحالي `{current_price:.4f}` خارج نطاق `{entry_price_from_signal:.4f} ± {allowed_percentage_diff*100:.2f}%`\n"
                                f"لذلك، لم يتم فتح الصفقة تفاديًا للدخول المتأخر أو العشوائي."
                            )
                            return None
                except Exception as e:
                    logger.error(f"❌ خطأ أثناء مقارنة السعر الحالي بسعر الدخول لـ {symbol_formatted}: {e}", exc_info=True)

                # تمرير المعلومات اللازمة لفتح الصفقة
                await open_order_with_price_check(
                    symbol=symbol_formatted, 
                    entry_price_from_signal=entry_price_from_signal, 
                    stop_loss_from_signal=initial_sl_to_use, 
                    targets_from_signal=targets_from_signal_prices[:3],  # أخذ أول 3 أهداف فقط
                    is_4h_conditional_sl_from_parser=is_4h_conditional_sl
                )

            except Exception as e:
                logger.error(f"MAIN TRY-EXCEPT in parse_new_format_signal: {e}", exc_info=True)
                await send_result_to_users(f"⚠️ خطأ عام فادح في تحليل إشارة النمط الجديد: {str(e)}")
                return None
               
        except Exception as e:
            logger.error(f"!!! CRITICAL UNHANDLED EXCEPTION in parse_new_format_signal !!!: {e}", exc_info=True)
            await send_result_to_users(f"🚨 حدث خطأ فادح أثناء تحليل الإشارة. يرجى مراجعة السجلات.")
            return None

async def parse_new_channel_signal(message_text: str):
    """
    تحلل رسائل الإشارات من القناة الجديدة التي تبدأ بـ "📈 تنبيه صفقة ! 📈".
    """
    global max_concurrent_trades, active_trades, trade_lock, currency_last_seen, stop_loss_mode, custom_stop_loss_percentage, logger, use_conditional_sl, account_switch_lock

    logger.info(f"--- Parsing New Channel Signal ---")
    logger.debug(f"Received message snippet: {message_text[:150]}")
    async with account_switch_lock:
        try:
            if not message_text.startswith("📈 تنبيه صفقة ! 📈"):
                logger.debug("Message does not match new channel signal prefix. Ignoring.")
                return None

            # التحقق من الحد الأقصى للصفقات النشطة
            async with trade_lock:
                active_trade_count = len(active_trades)
                if active_trade_count >= max_concurrent_trades:
                    log_msg = f"Cannot open new trade. Max concurrent trades ({max_concurrent_trades}) reached. Active: {active_trade_count}."
                    user_msg = f"⚠️ تم الوصول للحد الأقصى للصفقات النشطة ({max_concurrent_trades}). لا يمكن فتح صفقة جديدة حاليًا."
                    if max_concurrent_trades == 1:
                        user_msg = "⛔ لا يمكن فتح صفقة جديدة. البوت مضبوط على صفقة واحدة نشطة كحد أقصى، ويوجد بالفعل صفقة نشطة."
                    logger.info(log_msg)
                    await send_result_to_users(user_msg)
                    return None

            try:
                # استخراج البيانات
                match_symbol_raw = re.search(r"العملة:\s*#([A-Z0-9]+USDT)", message_text, re.IGNORECASE)
                match_entry_price_text = re.search(r"السعر:\s*([\d\.eE\-+]+)", message_text, re.IGNORECASE)
                raw_targets_as_text = re.findall(r"\d+️⃣\s*TP\d+:\s*([\d\.eE\-+]+)", message_text)
                match_sl_text_from_signal = re.search(r"🚫 وقف الخسارة:\s*([\d\.eE\-+]+)", message_text)
                
                # القرار الآن يعتمد على المتغير العام، وليس على نص المنشور
                is_4h_conditional_sl = use_conditional_sl 

                if is_4h_conditional_sl:
                    logger.info("Conditional SL mode is ACTIVE via bot settings.")
                else:
                    logger.info("Immediate SL mode is ACTIVE via bot settings.")

                if not (match_symbol_raw and match_entry_price_text and raw_targets_as_text):
                    log_msg = f"Parsing failed: Missing symbol, entry price, or targets. Snippet: {message_text[:250]}"
                    logger.warning(log_msg)
                    await send_result_to_users(f"Parsing failed: Missing symbol, entry price, or targets. Snippet: {message_text[:250]}")
                    # لا نرسل للمستخدم عن فشل التحليل الداخلي عادة إلا إذا كان الخطأ واضحًا
                    return None

                
                symbol_name_raw = match_symbol_raw.group(1).strip().upper()  # مثال: BTCUSDT, ETHUSDT

                # --- التحقق من العملة الحلال (مبسّط لأزواج USDT فقط) ---
                base_asset_from_signal = ""
                if symbol_name_raw.endswith("USDT") and len(symbol_name_raw) > 4:
                    base_asset_from_signal = symbol_name_raw[:-4]  # يزيل "USDT" من النهاية ليحصل على "BTC", "ETH"
                else:
                    # إذا لم يكن زوج USDT صالح، تجاهل الإشارة مباشرة
                    logger.warning(f"Signal for {symbol_name_raw} is not a valid USDT pair. Halal check cannot be performed accurately. Ignoring signal.")
                    await send_result_to_users(f"🚫 تم تجاهل إشارة لـ {symbol_name_raw} لأنها ليست زوج USDT صالح للتحقق.")
                    return None

                if halal_symbols_set:  # فقط قم بالتحقق إذا تم تحميل القائمة بنجاح
                    if base_asset_from_signal not in halal_symbols_set:
                        log_msg = f"Signal for {symbol_name_raw} (Base: {base_asset_from_signal}) is NOT in the halal symbols list. Ignoring signal."
                        logger.info(log_msg)
                        #await send_result_to_users(f"⚠️ تم تجاهل إشارة لـ {symbol_name_raw} لأن العملة الأساسية ({base_asset_from_signal}) غير موجودة في قائمة العملات المسموح بها.")
                        return None
                    else:
                        logger.info(f"Signal for {symbol_name_raw} (Base: {base_asset_from_signal}) is in the halal symbols list. Proceeding.")
                # --- نهاية التحقق من العملة الحلال ---
                
                symbol_formatted = f"{base_asset_from_signal}/USDT"
                # تحويل تنسيق الرمز (بافتراض USDT كعملة مقابلة رئيسية)
                if len(symbol_name_raw) > 4 and symbol_name_raw.endswith("USDT"):
                    symbol_formatted = f"{symbol_name_raw[:-4]}/{symbol_name_raw[-4:]}"
                else:
                    logger.warning(f"Symbol format {symbol_name_raw} might not be standard 'XYZUSDT'. Using as is.")
                    symbol_formatted = symbol_name_raw # أو يمكنك إرجاع None إذا كنت تريد فقط أزواج USDT
                
                entry_price_from_signal = 0.0
                targets_from_signal_prices = []
                try:
                    entry_price_from_signal = float(match_entry_price_text.group(1))
                    targets_from_signal_prices = sorted([float(t) for t in raw_targets_as_text])
                except ValueError:
                    log_msg = f"Error converting signal data (price/targets) to numbers for {symbol_name_raw}. Snippet: {message_text[:250]}"
                    logger.error(log_msg, exc_info=True)
                    await send_result_to_users(f"⚠️ خطأ في تحويل بيانات الإشارة (السعر/الأهداف) إلى أرقام للعملة {symbol_name_raw}.")
                    return None

                if not targets_from_signal_prices: # تأكد أن هناك أهدافًا
                    logger.warning(f"No targets found in signal for {symbol_name_raw}.")
                    await send_result_to_users(f"⚠️ لم يتم العثور على أهداف في الإشارة للعملة {symbol_name_raw}.")
                    return None
                
                # التحقق من وجود صفقة نشطة لنفس الرمز
                async with trade_lock:
                    if symbol_formatted in active_trades:
                        log_msg = f"Active trade already exists for {symbol_formatted}. Ignoring new signal."
                        logger.info(log_msg)
                        await send_result_to_users(f"⛔ يوجد صفقة نشطة بالفعل للرمز {symbol_formatted}. تم تجاهل الإشارة الجديدة.")
                        return None

                # (اختياري) التحقق من الإشارات المكررة زمنياً
                # current_time = time.time()
                # if symbol_formatted in currency_last_seen and (current_time - currency_last_seen[symbol_formatted] < SIGNAL_IGNORE_WINDOW_SECONDS):
                #     # ... (منطق التجاهل والرسالة) ...
                #     return None
                 
                # تحديد وقف الخسارة الأولي بناءً على stop_loss_mode
                initial_sl_to_use = None
                sl_source_info = "" # لمعلومات المستخدم

                if stop_loss_mode == "auto":
                    if match_sl_text_from_signal:
                        try:
                            initial_sl_to_use = float(match_sl_text_from_signal.group(1))
                            sl_source_info = "من الإشارة (تلقائي)"
                            logger.info(f"AUTO mode: Using SL {initial_sl_to_use} from signal for {symbol_formatted}.")
                        except ValueError:
                            logger.error(f"AUTO mode: Error converting SL from signal for {symbol_formatted}. SL text: '{match_sl_text_from_signal.group(1)}'. No SL.", exc_info=True)
                            await send_result_to_users(f"⚠️ خطأ قراءة SL من إشارة {symbol_formatted} (تلقائي). لن يوضع SL.")
                    else:
                        sl_source_info = "لا يوجد (لم يذكر في الإشارة - تلقائي)"
                        logger.warning(f"AUTO mode: SL not in signal for {symbol_formatted}. No SL.")
                        await send_result_to_users(f"⚠️ لم يوجد SL في إشارة {symbol_formatted} (تلقائي). لن يوضع SL.")
                
                elif stop_loss_mode == "custom":
                    if custom_stop_loss_percentage is not None:
                        initial_sl_to_use = entry_price_from_signal * (1 - (custom_stop_loss_percentage / 100))
                        sl_source_info = f"محسوب {custom_stop_loss_percentage}% (مخصص)"
                        logger.info(f"CUSTOM mode: Calculated SL {initial_sl_to_use} for {symbol_formatted}.")
                    else:
                        default_custom_perc = 5.0 # النسبة الافتراضية إذا لم يحدد المستخدم نسبة مخصصة
                        initial_sl_to_use = entry_price_from_signal * (1 - (default_custom_perc / 100))
                        sl_source_info = f"محسوب {default_custom_perc}% (مخصص افتراضي)"
                        logger.warning(f"CUSTOM mode: No custom % for {symbol_formatted}. Defaulting to {default_custom_perc}% SL: {initial_sl_to_use}.")
                        await send_result_to_users(f"⚠️ SL مخصص بدون نسبة لـ {symbol_formatted}. استخدام {default_custom_perc}% افتراضي.")
                
                elif stop_loss_mode == "disabled":
                    initial_sl_to_use = None
                    sl_source_info = "معطل من الإعدادات"
                    logger.info(f"DISABLED mode: No SL for {symbol_formatted}.")
                    await send_result_to_users(f"⚠️ وضع وقف الخسارة 'معطل' لـ {symbol_formatted}. لن يتم وضع SL.")

                # التحققات النهائية على وقف الخسارة والأهداف
                if initial_sl_to_use is not None and initial_sl_to_use >= entry_price_from_signal:
                    log_msg = f"Final SL ({initial_sl_to_use}) for {symbol_formatted} is not below entry ({entry_price_from_signal}). Ignoring."
                    logger.warning(log_msg)
                    await send_result_to_users(f"⚠️ وقف الخسارة المحدد ({initial_sl_to_use:.8f}) لـ {symbol_formatted} غير صالح. تم تجاهل الإشارة.")
                    return None
                    
                if any(tp <= entry_price_from_signal for tp in targets_from_signal_prices):
                    log_msg = f"Targets for {symbol_formatted} are not all above entry price. Ignoring."
                    logger.warning(log_msg)
                    await send_result_to_users(f"⚠️ أهداف {symbol_formatted} غير صالحة. تم تجاهل الإشارة.")
                    return None

                sl_display_text = f"{initial_sl_to_use:.8f}" if initial_sl_to_use is not None else "لا يوجد"
                conditional_sl_text = " (مشروط بإغلاق 4 ساعات)" if is_4h_conditional_sl and initial_sl_to_use is not None else ""
                
                log_final_info = f"Processing valid signal: {symbol_formatted}, Entry: {entry_price_from_signal}, SL: {sl_display_text}{conditional_sl_text}, Targets: {targets_from_signal_prices}, SL Source: {sl_source_info}"
                logger.info(log_final_info)
                
                # currency_last_seen[symbol_formatted] = current_time # إذا فعلت التحقق من التكرار
                
                log_final_info = f"Processing valid signal: {symbol_formatted}, Entry: {entry_price_from_signal}, SL: {sl_display_text}{conditional_sl_text}, Targets: {targets_from_signal_prices}, SL Source: {sl_source_info}"
                logger.info(log_final_info)

                # ✅ التحقق من السعر الحالي مقارنة بسعر الدخول
                try:
                    allowed_percentage_diff = 0.004 
                    symbol_api_format = symbol_formatted.replace('/', '')
                    current_price = live_prices.get(symbol_api_format) or await get_current_price(symbol_api_format)

                    logger.info(f"🚨 السعر الحالي لـ {symbol_formatted}: {current_price}, سعر الدخول من الإشارة: {entry_price_from_signal}")
                    
                    if current_price:
                        min_allowed_price = entry_price_from_signal * (1 - allowed_percentage_diff)
                        max_allowed_price = entry_price_from_signal * (1 + allowed_percentage_diff)
                        if not (min_allowed_price <= current_price <= max_allowed_price):
                            logger.warning(f"❌ تم تجاهل إشارة {symbol_formatted} بسبب انحراف السعر الحالي ({current_price}) خارج النطاق المسموح به [{min_allowed_price} - {max_allowed_price}]")
                            await send_result_to_users(
                                f"🚫 **تم تجاهل إشارة {symbol_formatted}**\n"
                                f"⚠️ السعر الحالي `{current_price:.4f}` خارج نطاق `{entry_price_from_signal:.4f} ± {allowed_percentage_diff*100:.2f}%`\n"
                                f"لذلك، لم يتم فتح الصفقة تفاديًا للدخول المتأخر أو العشوائي."
                            )
                            return None
                except Exception as e:
                    logger.error(f"❌ خطأ أثناء مقارنة السعر الحالي بسعر الدخول لـ {symbol_formatted}: {e}", exc_info=True)

                # تمرير المعلومات اللازمة لفتح الصفقة
                await open_order_with_price_check(
                    symbol=symbol_formatted, 
                    entry_price_from_signal=entry_price_from_signal, 
                    stop_loss_from_signal=initial_sl_to_use, 
                    targets_from_signal=targets_from_signal_prices,
                    is_4h_conditional_sl_from_parser=is_4h_conditional_sl
                )

            except Exception as e:
                logger.error(f"MAIN TRY-EXCEPT in parse_new_channel_signal: {e}", exc_info=True)
                await send_result_to_users(f"⚠️ خطأ عام فادح في تحليل إشارة القناة الجديدة: {str(e)}")
                return None
                
        except Exception as e:
            # هذا سيلتقط أي خطأ غير متوقع يحدث داخل الدالة
            logger.error(f"{log_prefix} !!! CRITICAL UNHANDLED EXCEPTION !!!: {e}", exc_info=True)
            await send_result_to_users(f"🚨 حدث خطأ فادح أثناء تحليل الإشارة. يرجى مراجعة السجلات.")
            return None

def update_statistics(is_success: bool):
    """دالة مركزية لتحديث عدادات الإحصائيات."""
    global successful_trades, failed_trades
    
    if is_success:
        successful_trades += 1
        logger.info(f"Statistics Updated: Successful trades = {successful_trades}")
    else:
        failed_trades += 1
        logger.info(f"Statistics Updated: Failed trades = {failed_trades}")

async def close_and_report(symbol, reason, final_price, executed_qty):
    """
    (النسخة النهائية الكاملة)
    تقوم بالحسابات، تحديث الإحصائيات بشكل آمن، التسجيل، وإرسال التقرير.
    """
    global successful_trades, failed_trades, trade_history, pending_trades, net_profit_usdt, currency_performance, active_trades, live_prices, logger
    
    log_prefix = f"CloseAndReport ({symbol}):"
    logger.info(f"{log_prefix} ----------------- PROCESS STARTED -----------------")
    logger.info(f"{log_prefix} Reason: {reason}, Final Price: {final_price}, Qty: {executed_qty}")

    try:
        # --- الخطوة 1: الحصول على نسخة من البيانات وحذف الصفقة من الذاكرة بأمان ---
        async with trade_lock:
            if symbol not in active_trades:
                logger.warning(f"{log_prefix} ABORTING. Trade was already removed from dict.")
                return
            # نحذف الصفقة ونحصل على بياناتها في خطوة واحدة
            trade_snapshot = active_trades.pop(symbol, None)
            # نحفظ الحالة الجديدة للملف فورًا بعد الحذف
            save_active_trades() 
        
        # إذا لم يتم العثور على الصفقة لسبب ما، نخرج
        if not trade_snapshot: return

        # --- الخطوة 2: إلغاء مهمة المراقبة وتنظيف الأسعار الحية (خارج القفل) ---
        #monitor_task_to_cancel = trade_snapshot.get('monitor_task')
        #if monitor_task_to_cancel and not monitor_task_to_cancel.done():
        #    monitor_task_to_cancel.cancel()
        
        live_prices.pop(symbol.replace('/', ''), None)
        logger.info(f"{log_prefix} Trade removed from memory and monitor task cancelled.")

        # --- الخطوة 3: الحسابات (تتم خارج القفل) ---
        entry_price = trade_snapshot['entry_price']
        start_time = trade_snapshot['start_time']
        s_info = trade_snapshot['symbol_info']
        
        # أضف هذا logging قبل حساب الربح:
        logger.info(f"{log_prefix} DEBUGGING PROFIT CALCULATION:")
        logger.info(f"  Entry Price: {entry_price:.8f}")
        logger.info(f"  Final Price: {final_price:.8f}")
        logger.info(f"  Executed Qty: {executed_qty:.8f}")
        logger.info(f"  Expected Investment: ~20 USDT")
        logger.info(f"  Actual Investment: {entry_price * executed_qty:.2f} USDT")

        #profit_loss_usdt = (final_price - entry_price) * executed_qty
        # أضف تقريب:
        profit_loss_usdt = round((final_price - entry_price) * executed_qty, 6)
        profit_loss_percentage = ((final_price - entry_price) / entry_price) * 100 if entry_price > 0 else 0
        trade_duration_seconds = time.time() - start_time
        duration_text = format_time(trade_duration_seconds)
        logger.info(f"{log_prefix} Calculations complete. P/L: {profit_loss_percentage:.2f}%")

        # --- الخطوة 4: تحديث الإحصائيات ---
        if pending_trades > 0: pending_trades -= 1
        if profit_loss_percentage > 0: successful_trades += 1
        else: failed_trades += 1
        
        async with trade_lock:
            logger.info(f"{log_prefix} Updating net_profit_usdt: +{profit_loss_usdt:.2f}, before: {net_profit_usdt:.2f}")
            net_profit_usdt += profit_loss_usdt
            logger.info(f"{log_prefix} net_profit_usdt updated to {net_profit_usdt:.2f}")

        perf_data = currency_performance.get(symbol, {'wins': 0, 'losses': 0, 'total_profit_percent': 0.0, 'total_duration': 0, 'trade_count': 0})
        perf_data.update({
            'trade_count': perf_data.get('trade_count', 0) + 1,
            'total_profit_percent': perf_data.get('total_profit_percent', 0.0) + profit_loss_percentage,
            'total_duration': perf_data.get('total_duration', 0) + trade_duration_seconds,
            'wins': perf_data.get('wins', 0) + (1 if profit_loss_percentage > 0 else 0),
            'losses': perf_data.get('losses', 0) + (1 if profit_loss_percentage <= 0 else 0)
        })
        currency_performance[symbol] = perf_data
        
        # استخدام .append() لأن trade_history هو deque/list
        trade_history.append({
            'symbol': symbol,
            'profit_loss': profit_loss_percentage,
            'duration': duration_text,
            'timestamp': time.time()
        })
        
        # حفظ كل الإحصائيات في ملفاتها
        save_all_stats()
        logger.info(f"{log_prefix} All statistics updated and saved.")
        
        # --- الخطوة 5: بناء وإرسال التقرير النهائي ---
        result_text_final = "🎉 صفقة رابحة" if profit_loss_percentage > 0 else "😞 صفقة خاسرة"
        quantity_precision_for_msg = s_info.get('quantity_precision', 8)
        
        report_message = (
            f"⚡️ {result_text_final} ({reason})\n"
            f"💱 العملة: {symbol}\n"
            f"💵 سعر الدخول: {entry_price:.8f}$\n"
            f"💵 سعر الخروج: {final_price:.8f}$\n"
            f"📈 الربح/الخسارة: {profit_loss_percentage:+.2f}% ({profit_loss_usdt:+.2f} USDT)\n"
            f"📦 الكمية المباعة: {executed_qty:.{quantity_precision_for_msg}f}\n"
            f"⏱️ مدة الصفقة: {duration_text}"
        )
        
        await send_result_to_users(report_message)
        logger.info(f"{log_prefix} Final report sent to users.")

    except Exception as e:
        logger.error(f"{log_prefix} CRITICAL ERROR inside close_and_report: {e}", exc_info=True)
    finally:
        logger.info(f"{log_prefix} ----------------- PROCESS FINISHED -----------------")

async def cancel_and_sell_market(symbol, trade_data, reason):
    """
    (النسخة المبسطة والمباشرة)
    تقوم بكل شيء خطوة بخطوة مع معالجة أخطاء واضحة.
    """
    symbol_api_format = symbol.replace('/', '')
    log_prefix = f"FailsafeSell ({symbol}):"
    logger.warning(f"{log_prefix} Initiated. Reason: {reason}")

    # --- الخطوة 1: تغيير الحالة إلى "قيد الإغلاق" ---
    async with trade_lock:
        if symbol not in active_trades or active_trades[symbol].get('state') == 'CLOSING':
            return
        active_trades[symbol]['state'] = 'CLOSING'
        save_active_trades()

    # --- الخطوة 2: محاولة إلغاء الأوامر المفتوحة ---
    try:
        open_orders = await asyncio.to_thread(client.get_open_orders, symbol=symbol_api_format)
        if open_orders:
            for order in open_orders:
                try:
                    await asyncio.to_thread(client.cancel_order, symbol=symbol_api_format, orderId=order['orderId'])
                except BinanceAPIException as e:
                    if e.code in [-2011, -2013]: # تجاهل خطأ "الأمر غير موجود"
                        logger.info(f"{log_prefix} Order {order['orderId']} already closed/gone.")
                    else:
                        raise # أطلق أي خطأ آخر
    except Exception as e:
        logger.error(f"{log_prefix} Error canceling orders: {e}. Will proceed anyway.")

    # --- الخطوة 3: محاولة البيع ---
    final_price = 0
    executed_qty = 0
    try:
        await asyncio.sleep(0.5)
        s_info = trade_data.get('symbol_info') or await get_complete_symbol_info(symbol_api_format, symbol)
        
        balance = await asyncio.to_thread(client.get_asset_balance, asset=s_info['baseAsset'])
        qty_to_sell = float(balance['free'])

        if qty_to_sell > 0:
            corrected_qty = adjust_quantity(qty_to_sell, s_info)
            if corrected_qty > 0:
                sell_order = await asyncio.to_thread(
                    client.order_market_sell,
                    symbol=symbol_api_format,
                    quantity=corrected_qty
                )
                final_price = float(sell_order['cummulativeQuoteQty']) / float(sell_order['executedQty'])
                executed_qty = float(sell_order['executedQty'])
    except BinanceAPIException as e:
        if e.code == -2010: # رصيد غير كافٍ
            logger.warning(f"{log_prefix} Insufficient balance for market sell. Position likely already closed.")
        else:
            logger.error(f"{log_prefix} Market sell failed with Binance error: {e}")
    except Exception as e:
        logger.error(f"{log_prefix} Market sell failed with unexpected error: {e}")

    # --- الخطوة 4: الإبلاغ والحذف (تتم دائمًا) ---
    
    # --- الخطوة 4: الإبلاغ والحذف (تتم دائمًا) ---
    if final_price == 0:
        fallback_price = trade_data.get('exit_price') or live_prices.get(symbol_api_format)
        logger.warning(f"{log_prefix} Using fallback price: {fallback_price} (exit_price: {trade_data.get('exit_price')}, live_price: {live_prices.get(symbol_api_format)})")
        final_price = fallback_price
    else:
        logger.info(f"{log_prefix} Using actual sell price from Binance: {final_price}")

    await close_and_report(symbol, reason, final_price, executed_qty)

async def place_trade_orders(symbol_key, entry_price, targets, stop_loss_price, amount_usdt, is_4h_conditional):
    """
    (النسخة النهائية) تفتح الصفقة فقط ولا تضع أي أوامر على Binance.
    """
    global active_trades, trade_lock, client, logger, exit_at_target_number, total_trades, pending_trades, trailing_sl_is_conditional

    async with trade_lock:
        if symbol_key in active_trades:
            return

    symbol_api_format = symbol_key.replace('/', '')
    
    if not targets or targets[0] <= entry_price:
        await send_result_to_users(f"⛔ فشل فتح {symbol_key}: الهدف الأول يجب أن يكون أعلى من سعر الدخول.")
        return
        
    try:
        s_info = await get_complete_symbol_info(symbol_api_format, symbol_key)
        if not s_info: return
        
        current_price = live_prices.get(symbol_api_format) or await get_current_price(symbol_api_format)


        # --- ✅ إصلاح minNotional: مقارنة مباشرة بالدولار ---
        min_notional = float(s_info.get('min_notional') or s_info.get('minNotional', 10.0))
        if amount_usdt < min_notional:
            await send_result_to_users(f"⛔ فشل فتح {symbol_key}: المبلغ ({amount_usdt:.2f}$) أقل من الحد الأدنى المطلوب ({min_notional:.2f}$).")
            return
        # --- ✅ الانتهاء من الإصلاح ---

        buy_order_result = await retry_blocking_api_call(
            client.order_market_buy,
            symbol=symbol_api_format,
            quantity=f"{adjust_quantity(amount_usdt / current_price, s_info):.{s_info['quantity_precision']}f}"
        )

        if not buy_order_result or buy_order_result.get('error') or buy_order_result.get('status') != 'FILLED':
            await send_result_to_users(f"⛔ فشل شراء {symbol_key}. السبب: {buy_order_result.get('message', 'Unknown')}")
            return

        avg_filled_price = float(buy_order_result['cummulativeQuoteQty']) / float(buy_order_result['executedQty'])
        executed_qty_float = float(buy_order_result.get('executedQty'))
        
        if avg_filled_price >= targets[0]:
            await send_result_to_users(f"⛔ تم إلغاء متابعة {symbol_key} لأن سعر الشراء أعلى من الهدف الأول.")
            await cancel_and_sell_market(symbol_key, {'quantity': executed_qty_float, 'symbol_info': s_info}, "سعر الشراء الفعلي أعلى من الهدف الأول")
            return
        
        await asyncio.sleep(0.5)
            
        # --- [ بداية التعديل الجديد ] ---
        tp_order_id = None
        tp_price = None
        oco_order_id = None
        sl_order_id = None
        
        # --- [ بداية التعديل لإضافة تفعيل OCO مباشر في حالة الشرطين معطلين وهدف أول ] ---
        if exit_at_target_number == 1 and not trailing_sl_is_conditional and not is_4h_conditional:
            try:
                tp_price = targets[0]
                qty_for_sell = adjust_quantity(executed_qty_float * 0.997, s_info)

                # وضع أمر OCO عند الهدف الأول مع وقف الخسارة               
                oco_order = await retry_blocking_api_call(
                    client.create_oco_order,
                    symbol=symbol_api_format,
                    side='SELL',
                    quantity=f"{qty_for_sell:.{s_info['quantity_precision']}f}",
                    price=f"{tp_price:.{s_info['price_precision']}f}",
                    stopPrice=f"{stop_loss_price:.{s_info['price_precision']}f}",
                    stopLimitPrice=f"{stop_loss_price:.{s_info['price_precision']}f}",
                    stopLimitTimeInForce='GTC'
                )
                
                if oco_order and not oco_order.get('error'):
                    tp_order_id = next((o['orderId'] for o in oco_order['orderReports'] if o['type'] == 'LIMIT_MAKER'), None)
                    sl_order_id = next((o['orderId'] for o in oco_order['orderReports'] if o['type'] == 'STOP_LOSS_LIMIT'), None)

                    logger.info(f"({symbol_key}) Successfully placed OCO order at TP1 with stop loss.")
                    await send_result_to_users(
                        f"🔒 تم وضع أمر حماية OCO عند الهدف الأول مع وقف الخسارة للصفقة {symbol_key}."
                    )
                else:
                    raise ValueError("Failed to create OCO order.")

            except Exception as e:
                logger.warning(f"({symbol_key}) FAILED to place OCO order at TP1. Falling back to LIMIT SELL. Error: {e}")

                # fallback to limit sell order كما في الكود الأصلي
                tp_index = exit_at_target_number - 1  # = 0
                tp_price = targets[tp_index]
                qty_for_sell = adjust_quantity(executed_qty_float * 0.997, s_info)

                limit_sell_order = await retry_blocking_api_call(
                    client.order_limit_sell,
                    symbol=symbol_api_format,
                    quantity=f"{qty_for_sell:.{s_info['quantity_precision']}f}",
                    price=f"{tp_price:.{s_info['price_precision']}f}",
                    timeInForce='GTC'
                )

                if limit_sell_order and limit_sell_order.get('orderId'):
                    tp_order_id = limit_sell_order['orderId']
                    logger.info(f"({symbol_key}) Successfully placed fallback LIMIT SELL at TP1: {tp_order_id}")
                    await send_result_to_users(
                        f"🔒 تم وضع أمر بيع مباشر للصفقة {symbol_key} عند الهدف رقم {exit_at_target_number} "
                        f"بسعر {tp_price:.{s_info['price_precision']}f}$."
                    )
                else:
                    logger.error(f"({symbol_key}) Failed to place fallback LIMIT SELL order.")

        # --- [ نهاية التعديل ] ---
        else:
            # نسمح بوضع أمر بيع مباشر لأي هدف من TP1 إلى TP3 (أو حسب ما تشاء)
            if exit_at_target_number in [1, 2, 3]:
                try:
                    tp_index = exit_at_target_number - 1  # فهرس الهدف داخل قائمة targets
                    tp_price = targets[tp_index]  # نأخذ السعر المناسب من القائمة
                    qty_for_sell = adjust_quantity(executed_qty_float * 0.997, s_info)

                    limit_sell_order = await retry_blocking_api_call(
                        client.order_limit_sell,
                        symbol=symbol_api_format,
                        quantity=f"{qty_for_sell:.{s_info['quantity_precision']}f}",
                        price=f"{tp_price:.{s_info['price_precision']}f}",
                        timeInForce='GTC'
                    )

                    if limit_sell_order and limit_sell_order.get('orderId'):
                        tp_order_id = limit_sell_order['orderId']
                        logger.info(f"({symbol_key}) Successfully placed TP{exit_at_target_number} protection order: {tp_order_id}")
                        await send_result_to_users(
                            f"🔒 تم وضع أمر بيع لـ {symbol_key} عند الهدف رقم {exit_at_target_number} (TP{exit_at_target_number}) "
                            f"بسعر {tp_price:.{s_info['price_precision']}f}$."
                        )
                    else:
                        raise ValueError("LIMIT_SELL order was not accepted by Binance.")

                except Exception as e:
                    logger.warning(f"({symbol_key}) FAILED to place TP{exit_at_target_number} protection order. Using MARKET fallback. Error: {e}")
                    await send_result_to_users(
                        f"⚠️ **تنبيه لصفقة {symbol_key}:**\n"
                        f"فشل وضع أمر البيع عند TP{exit_at_target_number}. سيتم البيع بسعر السوق عند الوصول إلى الهدف."
                    )
            # --- [ نهاية التعديل الجديد ] ---
        
        if exit_at_target_number == 10:
            price_precision = s_info['price_precision']
            last_tp = targets[-1]
            for _ in range(7):
                last_tp = round(last_tp * 1.05, price_precision)
                targets.append(last_tp)

        trade_data = {
            'entry_price': avg_filled_price, 'exit_price': tp_price, 'quantity': executed_qty_float, 'targets': targets,
            'current_stop_loss': stop_loss_price, 'is_4h_conditional_sl': is_4h_conditional,
            'use_trailing_sl_condition': trailing_sl_is_conditional, 'start_time': time.time(),
            'sl_order_id': sl_order_id, 'tp_order_id': tp_order_id, 'oco_order_id': oco_order_id,
            'current_target_index': 0, 'symbol_info': s_info,
            'monitor_task': None, 'state': 'ACTIVE', 'last_oco_attempt_time': 0, 'exit_at_target_number': exit_at_target_number
        }

        async with trade_lock:
            total_trades += 1
            pending_trades += 1
            active_trades[symbol_key] = trade_data
            save_active_trades()
        
        monitor_task = asyncio.create_task(monitor_trade(symbol_key))
        
        async with trade_lock:
            if symbol_key in active_trades: active_trades[symbol_key]['monitor_task'] = monitor_task
        
        await send_enhanced_trade_report(
            symbol=symbol_key,
            entry_price=avg_filled_price,
            targets=targets,
            sl_price=stop_loss_price,
            qty=executed_qty_float,
            is_4h=is_4h_conditional,
            s_info=s_info,
            exit_target=exit_at_target_number # <-- إضافة المعامل الناقص
        )
        logger.info(f"Trade {symbol_key} fully set up and monitoring has started.")

    except Exception as e:
        logger.error(f"FATAL error in place_trade_orders for {symbol_key}: {e}", exc_info=True)

def correct_current_target_index(current_price, targets):
    idx = 0
    while idx < len(targets) and current_price >= targets[idx]:
        idx += 1
    return max(0, idx - 1)

async def monitor_trade(symbol: str):
    """
    (النسخة المعدلة لدعم الوضع اللانهائي مع تصحيح current_target_index عند البدء)
    تراقب الصفقة، ترفع وقف الخسارة، وتضيف أهدافًا تلقائيًا إن لزم.
    """
    global active_trades, trade_lock, logger, live_prices
    
    def correct_current_target_index(current_price, targets):
        idx = 0
        while idx < len(targets) and current_price >= targets[idx]:
            idx += 1
        return max(0, idx - 1)
    
    log_prefix = f"Monitor ({symbol}):"

    try:
        # بداية: احصل على السعر الحالي
        current_price = live_prices.get(symbol.replace('/', ''))
        if not current_price:
            try:
                current_price = await get_current_price(symbol.replace('/', ''))
            except ValueError:
                current_price = None

        if current_price is None:
            logger.warning(f"{log_prefix} Failed to get initial current price.")
            return
        
        # تصحيح current_target_index بناءً على السعر الحالي
        async with trade_lock:
            trade = active_trades.get(symbol)
            if not trade or trade.get('state') == 'CLOSING':
                return
            
            targets = trade.get('targets', [])
            corrected_index = correct_current_target_index(current_price, targets)
            active_trades[symbol]['current_target_index'] = corrected_index
        
        while True:
            await asyncio.sleep(0.2)
            
            async with trade_lock:
                trade = active_trades.get(symbol)
                if not trade or trade.get('state') == 'CLOSING':
                    return
                snapshot = trade.copy()
            
            current_price = live_prices.get(symbol.replace('/', ''))
            if not current_price:
                try:
                    current_price = await get_current_price(symbol.replace('/', ''))
                except ValueError:
                    continue
            if not current_price:
                continue
            
            exit_at_target_number = snapshot.get('exit_at_target_number', 1)
            
            # --- الأولوية 1: وقف الخسارة ---
            stop_loss_triggered = await check_stop_loss(symbol, snapshot, current_price)
            if stop_loss_triggered:
                return

            # --- الأولوية 2: الأهداف ---
            cur_idx = snapshot['current_target_index']
            targets = snapshot['targets']
            entry_price = snapshot['entry_price']
            s_info = snapshot['symbol_info']
            price_precision = s_info['price_precision']

            if cur_idx < len(targets) and current_price >= targets[cur_idx]:
                target_number = cur_idx + 1

                if exit_at_target_number == 10:
                    # ✅ وضع الأهداف اللانهائية: أضف هدفًا جديدًا تلقائيًا
                    last_tp = targets[-1]
                    new_tp = round(last_tp * 1.05, price_precision)
                    targets.append(new_tp)
                    snapshot['targets'] = targets

                    # رفع وقف الخسارة مثل المعتاد
                    new_sl_price = entry_price if target_number == 1 else targets[target_number - 2]
                    if new_sl_price > snapshot['current_stop_loss']:
                        await update_stop_loss(symbol, snapshot, new_sl_price, target_number)
                        
                    async with trade_lock:
                        if symbol in active_trades:
                            active_trades[symbol]['current_target_index'] = cur_idx + 1
                            active_trades[symbol]['targets'] = targets
                else:
                    # إذا وصلنا إلى الهدف الأخير (TP النهائي)
                    
                    if target_number == exit_at_target_number:
                        logger.info(f"{log_prefix} Final TP{target_number} hit. Checking order status first...")
                        
                        # فحص إذا كان الأمر نُفذ بالفعل
                        tp_order_id = snapshot.get('tp_order_id')
                        if tp_order_id:
                            try:
                                logger.info(f"{log_prefix} Checking TP order {tp_order_id} status...")
                                order_status = await retry_blocking_api_call(
                                    client.get_order, 
                                    symbol=symbol.replace('/', ''), 
                                    orderId=tp_order_id,
                                    symbol_for_log=f"TP Order Check {symbol}"
                                )
                                
                                if order_status and order_status.get('status') == 'FILLED':
                                    # الأمر نُفذ بنجاح - استخدم سعره
                                    executed_price = float(order_status['cummulativeQuoteQty']) / float(order_status['executedQty'])
                                    executed_qty = float(order_status['executedQty'])
                                    logger.info(f"{log_prefix} TP order was FILLED! Price: {executed_price}, Qty: {executed_qty}")
                                    await close_and_report(symbol, f"تحقيق TP{target_number} (أمر محدد)", executed_price, executed_qty)
                                    return
                                elif order_status and order_status.get('status') in ['NEW', 'PARTIALLY_FILLED']:
                                    # الأمر لا يزال معلق - لا تتدخل
                                    logger.info(f"{log_prefix} TP order still pending. Status: {order_status.get('status')}. Continue monitoring...")
                                    continue  # تخطى هذه الدورة
                                elif order_status and order_status.get('status') == 'CANCELED':
                                    logger.warning(f"{log_prefix} TP order was CANCELED! Checking asset balance...")
                                    # فحص إذا كانت العملة لا تزال موجودة بكمية مهمة
                                    s_info = snapshot['symbol_info']
                                    balance = await asyncio.to_thread(client.get_asset_balance, asset=s_info['baseAsset'])
                                    
                                    qty_available = float(balance['free'])
                                    min_qty_threshold = s_info['min_qty'] * 10
                                    min_value_threshold = 1.0  # 1 دولار كحد أدنى
                                    current_value = qty_available * current_price

                                    if qty_available > min_qty_threshold and current_value > min_value_threshold:
                                        logger.warning(f"{log_prefix} Significant asset balance found: {qty_available} (${current_value:.2f}). Selling at market...")
                                        await cancel_and_sell_market(symbol, snapshot, f"الأمر ملغى - بيع بسعر السوق")
                                        return
                                    else:
                                        logger.info(f"{log_prefix} Only dust amount remaining: {qty_available} (${current_value:.2f}). Considering position closed.")
                                        await close_and_report(symbol, "تم إغلاق الصفقة (بقايا غبار)", current_price, 0)
                                        return

                                else:
                                    logger.warning(f"{log_prefix} No TP order found or other status. Checking asset balance...")
                                    # نفس الفحص للحالات الأخرى
                                    s_info = snapshot['symbol_info']
                                    balance = await asyncio.to_thread(client.get_asset_balance, asset=s_info['baseAsset'])
                                    
                                    qty_available = float(balance['free'])
                                    min_qty_threshold = s_info['min_qty'] * 10
                                    min_value_threshold = 1.0
                                    current_value = qty_available * current_price

                                    if qty_available > min_qty_threshold and current_value > min_value_threshold:
                                        logger.warning(f"{log_prefix} Asset balance found without order: {qty_available} (${current_value:.2f}). Selling at market...")
                                        await cancel_and_sell_market(symbol, snapshot, f"بيع بسعر السوق")
                                        return
                                    else:
                                        logger.info(f"{log_prefix} No significant asset balance. Position already closed.")
                                        await close_and_report(symbol, "الصفقة مُغلقة", current_price, 0)
                                        return

                            except Exception as e:
                                logger.error(f"{log_prefix} Error checking TP order: {e}")
                        
                        # فقط إذا لم يكن هناك أمر أو فشل
                        logger.info(f"{log_prefix} No valid TP order found. Using market sell...")
                        await cancel_and_sell_market(symbol, snapshot, f"تحقيق الهدف النهائي (TP{target_number})")
                        return
    
                    # رفع الوقف فقط
                    new_sl_price = entry_price if target_number == 1 else targets[target_number - 2]
                    if new_sl_price > snapshot['current_stop_loss']:
                        await update_stop_loss(symbol, snapshot, new_sl_price, target_number)

                    async with trade_lock:
                        if symbol in active_trades:
                            active_trades[symbol]['current_target_index'] = cur_idx + 1

    except asyncio.CancelledError:
        logger.warning(f"{log_prefix} Task was cancelled.")
    except Exception as e:
        logger.error(f"{log_prefix} CRITICAL error in monitor_trade: {e}", exc_info=True)
        await cancel_and_sell_market(symbol, snapshot, "خطأ فادح في المراقبة")
    finally:
        logger.info(f"{log_prefix} Task finished.")

async def update_stop_loss(symbol, snapshot, new_sl_price, target_number):
    """
    (النسخة النهائية)
    تقوم بتحديث وقف الخسارة، إما في الذاكرة فقط أو بوضع أمر OCO حقيقي على Binance مع إعادة المحاولة.
    """
    global active_trades, trade_lock, client, logger, exit_at_target_number
        
    symbol_api_format = symbol.replace('/', '')
    log_prefix = f"UpdateSL ({symbol}):"
    
    # جلب حالة الخاصية الجديدة من بيانات الصفقة
    use_trailing_sl_condition = snapshot.get('use_trailing_sl_condition', False)
    
    now = time.time()
    last_attempt = snapshot.get('last_oco_attempt_time', 0)

    # لا تعيد محاولة OCO إذا مرت أقل من 20 ثانية
    if now - last_attempt < 20:
        logger.warning(f"{log_prefix} Skipping OCO update due to recent attempt.")
        return

    # --- الحالة 1: الخاصية مفعلة (نرفع الوقف في الذاكرة فقط) ---
    if use_trailing_sl_condition:
        logger.info(f"{log_prefix} TP{target_number} hit. Trailing SL to {new_sl_price} (4h condition remains).")
        async with trade_lock:
            if symbol in active_trades:
                active_trades[symbol].update({
                    'current_stop_loss': new_sl_price,
                    'current_target_index': target_number,
                    'exit_at_target_number': snapshot.get('exit_at_target_number', 1),
                    'is_4h_conditional_sl': True,
                    'last_oco_attempt_time' : now
                })
        
        sl_level_name = "سعر الدخول" if target_number == 1 else f"TP{target_number - 1}"
        s_info = snapshot['symbol_info']
        await send_result_to_users(
            f"🎯 تحقق الهدف {target_number} لـ {symbol}!\n"
            f"🔐 تم رفع وقف الخسارة إلى {sl_level_name}: {new_sl_price:.{s_info.get('price_precision', 8)}f}$\n"
            "(لا يزال مشروطًا بإغلاق 4 ساعات)"
        )
    
    # --- الحالة 2: الخاصية معطلة (نضع أمر OCO حقيقي) ---
    else:
        logger.info(f"{log_prefix} TP{target_number} hit. Attempting to place/update OCO protection...")
        
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                # الخطوة أ: إلغاء أي أوامر حماية قديمة
                tp_order_id = snapshot.get('tp_order_id')
                sl_order_id = snapshot.get('sl_order_id')
                oco_id = snapshot.get('oco_order_id')
                
                if oco_id: await retry_blocking_api_call(client.cancel_order_list, symbol=symbol_api_format, orderListId=oco_id)
                elif tp_order_id: await retry_blocking_api_call(client.cancel_order, symbol=symbol_api_format, orderId=tp_order_id)
                elif sl_order_id: await retry_blocking_api_call(client.cancel_order, symbol=symbol_api_format, orderId=sl_order_id)

                # الخطوة ب: وضع أمر OCO الجديد
                s_info = snapshot['symbol_info']
                final_tp_price = snapshot['targets'][exit_at_target_number - 1]
                corrected_qty = adjust_quantity(snapshot['quantity'] * 0.997, s_info)
                
                new_oco = await retry_blocking_api_call(
                    client.create_oco_order,
                    symbol=symbol_api_format, side='SELL', quantity=f"{corrected_qty:.{s_info['quantity_precision']}f}",
                    price=f"{final_tp_price:.{s_info['price_precision']}f}",
                    stopPrice=f"{new_sl_price:.{s_info['price_precision']}f}",
                    stopLimitPrice=f"{new_sl_price:.{s_info['price_precision']}f}",
                    stopLimitTimeInForce='GTC'
                )

                if not new_oco or new_oco.get('error'):
                    raise ValueError(f"OCO creation rejected by Binance: {new_oco.get('msg', 'Unknown error')}")

                # --- نجحنا، نخرج من الحلقة ---
                logger.info(f"{log_prefix} OCO protection successfully set on attempt {attempt+1}.")
                
                new_tp_id = next((o['orderId'] for o in new_oco['orderReports'] if o['type'] == 'LIMIT_MAKER'), None)
                new_sl_id = next((o['orderId'] for o in new_oco['orderReports'] if o['type'] == 'STOP_LOSS_LIMIT'), None)

                async with trade_lock:
                    if symbol in active_trades:
                        active_trades[symbol].update({
                            'current_stop_loss': new_sl_price,
                            'sl_order_id': new_sl_id,
                            'tp_order_id': new_tp_id,
                            'current_target_index': target_number,
                            'exit_at_target_number': snapshot.get('exit_at_target_number', 1),
                            'is_4h_conditional_sl': False,
                            'protection_type': 'OCO',
                            'oco_order_id': new_oco.get('orderListId'),
                            'last_oco_attempt_time' : now
                        })
                await send_result_to_users(f"🎯 تحقق TP{target_number} لـ {symbol}!\n🛡️ تم تفعيل حماية OCO (وقف عند {new_sl_price:.{s_info['price_precision']}f}$).")
                
                return # نخرج من الدالة بعد النجاح
                
            except Exception as e:
                logger.error(f"{log_prefix} Attempt {attempt + 1}/{max_attempts} to set OCO failed: {e}")
                if attempt < max_attempts - 1:
                    await asyncio.sleep(2)  # انتظر قبل المحاولة التالية
                else:
                    logger.critical(f"{log_prefix} All OCO attempts failed.")
                    await send_result_to_users(
                        f"⚠️ فشل تحديث أمر حماية OCO لـ {symbol} بعد TP{target_number}.\n"
                        "سيتم متابعة الصفقة عبر مراقبة السعر المباشر."
                    )
                    # بدون إغلاق الصفقة بالسوق، تترك المراقبة مستمرة
                    return

async def check_stop_loss(symbol, snapshot, current_price):
    """
    فحص وقف الخسارة - يغلق الصفقة فقط عند إغلاق شمعة 4 ساعات تحت وقف الخسارة
    """
    global active_trades, trade_lock, client, logger, use_conditional_sl
    symbol_api_format = symbol.replace('/', '')
    log_prefix = f"CheckSL ({symbol}):"
    
    sl_price = snapshot['current_stop_loss']
    sl_order_id = snapshot.get('sl_order_id')
    is_4h_conditional = snapshot.get('is_4h_conditional_sl', False)
    
    # ✅ فقط إذا كان هناك أمر حقيقي على بايننس وتم تنفيذه
    if sl_order_id:
        try:
            order = await retry_blocking_api_call(client.get_order, symbol=symbol_api_format, orderId=sl_order_id)
            if order and order.get('status') == 'FILLED':
                logger.warning(f"{log_prefix} SL order {sl_order_id} was FILLED by Binance.")
                final_price = float(order['cummulativeQuoteQty']) / float(order['executedQty'])
                
                # إلغاء أمر جني الأرباح
                tp_order_id = snapshot.get('tp_order_id')
                if tp_order_id:
                    try:
                        await retry_blocking_api_call(client.cancel_order, symbol=symbol_api_format, orderId=tp_order_id)
                    except:
                        pass
                
                await close_and_report(symbol, "إغلاق عند وقف الخسارة (أمر بايننس)", final_price, float(order['executedQty']))
                return True
        except BinanceAPIException as e:
            if e.code in [-2013, -2011]:
                logger.warning(f"{log_prefix} SL order disappeared - but will only close on 4h candle condition.")
                # ❌ لا نغلق هنا! نستمر في الفحص
            else:
                logger.error(f"{log_prefix} Error checking SL order: {e}")
    
    # ✅ التحقق من شرط إغلاق شمعة 4 ساعات - هذا هو الشرط الوحيد للإغلاق
    if current_price <= sl_price:
        # حماية من إشارات وهمية
        price_diff_percent = ((sl_price - current_price) / sl_price) * 100
        if price_diff_percent > 10:  # فرق كبير = إشارة وهمية محتملة
            logger.warning(f"{log_prefix} Ignoring possible fake signal. Price diff: {price_diff_percent:.2f}%")
            return False
        
        # ✅ فحص شرط 4 ساعات دائماً (حتى لو كان معطل في الإعدادات)
        logger.info(f"{log_prefix} Price at SL level ({current_price} <= {sl_price}). Checking 4h candle condition...")
        
        try:
            # جلب شموع 4 ساعات
            klines = await retry_blocking_api_call(
                client.get_klines, 
                symbol=symbol_api_format, 
                interval=Client.KLINE_INTERVAL_4HOUR, 
                limit=3
            )
            
            if not klines or len(klines) < 2:
                logger.error(f"{log_prefix} ❌ Cannot get 4h candles. Will NOT close trade.")
                return False  # ❌ لا نغلق إذا فشل جلب الشموع
            
            # الشمعة المكتملة الأخيرة
            last_closed_kline = klines[-2]
            trade_start_time_ms = int(snapshot['start_time'] * 1000)
            candle_close_time = last_closed_kline[6]
            close_price = float(last_closed_kline[4])
            
            # التأكد من أن هذه شمعة جديدة
            last_checked_timestamp = snapshot.get('last_checked_4h_kline_timestamp', 0)
            
            if (last_closed_kline[0] != last_checked_timestamp and 
                candle_close_time > trade_start_time_ms):
                
                # تحديث الشمعة المفحوصة
                async with trade_lock:
                    if symbol in active_trades:
                        active_trades[symbol]['last_checked_4h_kline_timestamp'] = last_closed_kline[0]
                        save_active_trades()
                
                # ✅ الشرط الوحيد للإغلاق: إغلاق شمعة 4h تحت وقف الخسارة
                if close_price <= sl_price:
                    logger.warning(f"{log_prefix} ✅ 4H CANDLE CLOSED BELOW SL! Closing trade.")
                    logger.warning(f"{log_prefix} Candle close: {close_price}, SL: {sl_price}")
                    await cancel_and_sell_market(symbol, snapshot, f"إغلاق شمعة 4h تحت وقف الخسارة")
                    return True
                else:
                    logger.info(f"{log_prefix} ✅ 4H candle closed ABOVE SL. Trade continues.")
                    logger.info(f"{log_prefix} Candle close: {close_price}, SL: {sl_price}")
                    return False
            else:
                # الشمعة لا تزال مفتوحة أو تم فحصها من قبل
                logger.debug(f"{log_prefix} ⏳ Waiting for 4h candle to close...")
                return False
                
        except Exception as e:
            logger.error(f"{log_prefix} ❌ Error checking 4h candles: {e}", exc_info=True)
            logger.error(f"{log_prefix} Will NOT close trade due to error.")
            return False  # ❌ لا نغلق في حالة الخطأ
    
    # السعر فوق وقف الخسارة - لا إغلاق
    return False

async def send_enhanced_trade_report(symbol, entry_price, targets, sl_price, qty, is_4h, s_info, exit_target):
    """
    إرسال تقرير محسن عن فتح الصفقة
    """
    price_precision = s_info['price_precision']
    
    success_msg_parts = [
        f"✅ تم فتح صفقة {symbol} بنجاح",
        f"💰 سعر الدخول: {entry_price:.8f}$",
        f"💵 المبلغ: {entry_price * qty:.2f}$"
    ]

    if exit_target == 10:
        success_msg_parts.append("🎯 الهدف المحدد: TP3 وما بعده (Trailing)\n")
    else:
        success_msg_parts.append(f"🎯 الهدف المحدد: TP{exit_target} ({targets[exit_target-1]:.8f}$)\n")

    # نعرض فقط أول 3 أهداف في حالة اللانهائي، وإلا نعرض كل الأهداف
    if exit_target == 10:
        display_targets = targets[:3]
    else:
        display_targets = targets
    
    if exit_target == 10:
        success_msg_parts.append("♻️ سيتم استخدام نظام Trailing بعد TP3 بنسبة +5% لكل هدف جديد.")
    else:
        success_msg_parts.append("📋 جميع الأهداف:")
    
        for i, target_price in enumerate(display_targets):
            profit_percentage = ((target_price - entry_price) / entry_price) * 100
            marker = "🟢" if (exit_target != 10 and i == (exit_target - 1)) else "⚪"
            success_msg_parts.append(
                f"  {marker} الهدف {i+1}: {target_price:.8f}$ ({profit_percentage:.2f}%)"
            )
    
    if sl_price:
        loss_percentage = ((sl_price - entry_price) / entry_price) * 100
        cond_text = " (مشروط بـ 4 ساعات)" if is_4h else ""
        success_msg_parts.append(
            f"\n⛔ وقف الخسارة: {sl_price:.8f}$ ({loss_percentage:.2f}%){cond_text}"
        )

    final_success_msg = "\n".join(success_msg_parts)
    logger.info(f"OpenOrder ({symbol}): Sending enhanced success message to users.")
    await send_result_to_users(final_success_msg)

async def open_order_with_price_check(symbol: str, entry_price_from_signal: float, stop_loss_from_signal: float | None, targets_from_signal: list[float], is_4h_conditional_sl_from_parser: bool):
    """
    تستخرج البيانات من الإشارة وتستدعي الدالة المركزية لوضع الأوامر.
    """
    if stop_loss_from_signal is None:
        await send_result_to_users(f"⛔ لا يمكن فتح صفقة `{symbol}` بدون وقف خسارة في وضع الأوامر الجديد.")
        return
    
    logger.info(f"Signal processed for {symbol}. Calling place_trade_orders.")
    
    # استدعاء الدالة الجديدة مع تمرير المبلغ الثابت
    await place_trade_orders(
        symbol_key=symbol,
        entry_price=entry_price_from_signal,
        targets=targets_from_signal,
        stop_loss_price=stop_loss_from_signal,
        amount_usdt=fixed_trade_amount,
        is_4h_conditional=is_4h_conditional_sl_from_parser
    )

# إرسال الرسائل إلى المستخدمين
async def send_result_to_users(result):
    for chat_id in ADMIN_USER_IDS_LIST:
        try:
            await bot.send_message(chat_id, result, parse_mode="Markdown")
        except TelegramAPIError as e:
            logging.error(f"❌ خطأ في إرسال الرسالة للمستخدم {chat_id}: {e}")
        except Exception as e:
            logging.error(f"❌ خطأ غير متوقع في إرسال الرسالة: {e}")

#@client_telegram.on(events.NewMessage(chats=BOT_USERNAMES))
async def new_signal_handler(event):
    if not bot_active:
        return
        
    message = event.message.message
    
    result = await parse_new_channel_signal(message)
    if result is None:
        result = await parse_new_format_signal(message)
        if result is None:
            await parse_novo_signal(message)
            if result is None:
                photo_data = None
                if event.message.photo:
                    photo_data = await event.message.download_media(bytes)
                await parse_buy_signal(message, photo_data)

# دالة لجلب الأصول الموجودة في المحفظة
async def get_owned_assets(client):
    try:
        account_info = await asyncio.to_thread(client.get_account)
        balances = account_info['balances']
        owned_assets = [b for b in balances if float(b['free']) > 0 or float(b['locked']) > 0]

        if not owned_assets:
            return "⚠️ لا توجد أصول في المحفظة."

        assets_data = []

        for asset in owned_assets:
            asset_name = asset['asset']
            total_amount = float(asset['free']) + float(asset['locked'])

            # جلب سعر العملة الحالي
            try:
                symbol = f"{asset_name}USDT"
                price_info = await asyncio.to_thread(client.get_symbol_ticker, symbol=symbol)
                current_price = float(price_info['price'])
            except Exception as e:
                continue  # تجاوز العملات التي لا تحتوي على زوج USDT

            # حساب القيمة بالدولار
            total_value = total_amount * current_price
            if total_value < 1:
                continue  # تجاهل العملات التي قيمتها أقل من 1 دولار

            # جلب آخر عمليات الشراء التي تتعلق بالكمية المتبقية فقط
            trades = await asyncio.to_thread(client.get_my_trades, symbol=symbol)
            trades = [t for t in trades if t['isBuyer']]

            remaining_qty = total_amount
            filtered_trades = []
            for trade in reversed(trades):  # البدء من أحدث معاملة
                trade_qty = float(trade['qty'])
                if remaining_qty <= 0:
                    break
                if trade_qty <= remaining_qty:
                    filtered_trades.append(trade)
                    remaining_qty -= trade_qty
                else:
                    trade['qty'] = remaining_qty  # أخذ جزء من الصفقة فقط
                    filtered_trades.append(trade)

            if not filtered_trades:
                continue  # تجاوز العملات التي لا توجد لها بيانات شراء

            # حساب متوسط سعر الشراء بناءً على الكمية المتبقية فقط
            total_cost = sum(float(t['price']) * float(t['qty']) for t in filtered_trades)
            total_qty = sum(float(t['qty']) for t in filtered_trades)
            avg_price = total_cost / total_qty if total_qty > 0 else 0

            # حساب نسبة التغير
            price_change = ((current_price - avg_price) / avg_price) * 100 if avg_price > 0 else 0

            # حساب مدة الاحتفاظ بناءً على أحدث عملية شراء للكمية المتبقية
            last_trade_time = max(int(t['time']) for t in filtered_trades) / 1000
            hold_duration = datetime.now(timezone.utc) - datetime.fromtimestamp(last_trade_time, timezone.utc)
            hold_days = hold_duration.days
            hold_hours = hold_duration.seconds // 3600

            # تخزين البيانات
            assets_data.append({
                'symbol': asset_name,
                'quantity': total_amount,
                'total_value': total_value,
                'avg_price': avg_price,
                'current_price': current_price,
                'price_change': price_change,
                'hold_duration': f"{hold_days} يوم، {hold_hours} ساعة"
            })

        if not assets_data:
            return "⚠️ لا توجد أصول ذات قيمة في المحفظة."

        return assets_data

    except Exception as e:
        return f"⚠️ حدث خطأ أثناء جلب الأصول: {str(e)}"

def split_message(message_text, max_length=4096):
    """
    تقسيم الرسالة إلى أجزاء أصغر بحيث لا يتجاوز طول كل جزء 4096 حرفًا.
    """
    return [message_text[i:i + max_length] for i in range(0, len(message_text), max_length)]

@dp.message_handler(lambda message: message.text == "📝 عرض الأصول")
async def show_owned_assets(message: types.Message):
    if message.chat.id not in ADMIN_USER_IDS_LIST:
        await message.answer("🚫 عذرًا، لا تملك صلاحية استخدام هذا البوت.")
        return
        
    loading_message = await message.answer("⏳ جارٍ جلب بيانات محفظتك...")

    try:
        client = create_client()
        assets_data = await get_owned_assets(client)
        
        # نحذف رسالة التحميل فور الحصول على البيانات
        await bot.delete_message(chat_id=message.chat.id, message_id=loading_message.message_id)

        if isinstance(assets_data, str):
            await message.answer(assets_data)
        else:
            # إنشاء معرف فريد لهذه الجلسة وتخزين البيانات في الكاش
            cache_id = str(int(time.time()))
            assets_display_cache[cache_id] = assets_data

            message_text = "📋 **الأصول في المحفظة:**\n\n"
            keyboard = InlineKeyboardMarkup(row_width=1)
            
            for asset in assets_data:
                symbol = asset.get('symbol', 'N/A')
                quantity = asset.get('quantity', 0)
                total_value = asset.get('total_value', 0)
                avg_price = asset.get('avg_price', 0)
                current_price = asset.get('current_price', 0)
                price_change = asset.get('price_change', 0)
                hold_duration = asset.get('hold_duration', 'N/A')
                
                # تحديد الإيموجي للتقدم (ربح أو خسارة)
                progress_emoji = "🟢" if price_change >= 0 else "🔴"
                
                # --- [ بداية التنسيق الجديد مع الإيموجي ] ---
                message_text += (
                    f"🛒 **العملة: `{symbol}`**\n"
                    f"   - 💰 القيمة الحالية: `{total_value:.2f}$`\n"
                    f"   - 📈 سعر الشراء: `{avg_price:.8f}$`\n"
                    f"   - 📊 السعر الحالي: `{current_price:.8f}$`\n"
                    f"   - {progress_emoji} التقدم: `{price_change:+.2f}%`\n"
                    f"   - 📦 الكمية: `{quantity:.8f}`\n"
                    f"   - ⏳ مدة الاحتفاظ: {hold_duration}\n"
                    f"--------------------\n"
                )
                # --- [ نهاية التنسيق الجديد ] ---
                
                sell_button = InlineKeyboardButton(text=f"❌ بيع {symbol}", callback_data=f"sell_asset:{symbol}")
                keyboard.add(sell_button)
            
            # تمرير المعرف الفريد في callback_data لزر بيع الكل
            sell_all_button = InlineKeyboardButton(
                text="⏹️ بيع جميع الأصول المعروضة",
                callback_data=f"sell_all_displayed:{cache_id}"
            )
            keyboard.add(sell_all_button)
            
            for part in split_message(message_text, max_length=4000):
                is_last_part = (part == split_message(message_text, max_length=4000)[-1])
                await message.answer(
                    part, 
                    parse_mode="Markdown", 
                    reply_markup=keyboard if is_last_part else None
                )
    
    except Exception as e:
        # تأكد من حذف رسالة التحميل حتى في حالة الخطأ
        try:
            await bot.delete_message(chat_id=message.chat.id, message_id=loading_message.message_id)
        except Exception:
            pass
        await message.answer(f"⚠️ حدث خطأ أثناء عرض الأصول: {str(e)}")
        logging.error(f"Error in show_owned_assets: {traceback.format_exc()}")

# معالجة الضغط على زر "بيع"
@dp.callback_query_handler(lambda c: c.data.startswith("sell_asset:"))
async def sell_specific_asset(callback_query: types.CallbackQuery):
    try:
        _, symbol = callback_query.data.split(":")
        
        # بيع العملة بسعر السوق الحالي
        result = await sell_asset(create_client(), symbol)
        
        # إعلام المستخدم بالنتيجة
        await bot.answer_callback_query(callback_query.id, text=result)
        await bot.send_message(callback_query.message.chat.id, result)
    except Exception as e:
        await bot.answer_callback_query(callback_query.id, text=f"⚠️ حدث خطأ: {str(e)}")
        logging.error(f"Error in sell_specific_asset: {traceback.format_exc()}")

# دالة لبيع عملة بسعر السوق الحالي (برسائل نصية بسيطة للمستخدم)
async def sell_asset(client, symbol):
    symbol_api_format = f"{symbol}USDT"
    symbol_key_format = f"{symbol}/USDT"

    logger.info(f"--- STARTING MANUAL SELL FOR {symbol} ---")

    try:
        # جلب معلومات الصفقة النشطة أولاً (إن وجدت)
        async with trade_lock:
            trade_snapshot = active_trades.get(symbol_key_format)

        # الخطوات التالية تبقى كما هي
        s_info = await get_complete_symbol_info(symbol_api_format, symbol_key_format)
        balance = await retry_blocking_api_call(client.get_asset_balance, asset=symbol)

        if not s_info or not balance or not balance.get('free'):
            return f"⚠️ لا يمكن إكمال البيع: فشل جلب معلومات العملة أو الرصيد لـ {symbol}."

        available_free_balance = float(balance['free'])
        if available_free_balance <= 0:
            return f"⚠️ الكمية المتاحة من {symbol} هي صفر."
        
        precision = s_info['quantity_precision']
        step_size = s_info['step_size']
        factor = 10 ** precision
        truncated_qty = math.floor(available_free_balance * factor) / factor
        final_quantity = round(math.floor(truncated_qty / step_size) * step_size, precision)
        
        if final_quantity <= 0:
            return f"⚠️ الكمية النهائية للبيع لـ {symbol} بعد التعديل هي صفر."
        
        quantity_as_string = f"{final_quantity:.{precision}f}"
        
        # منطق تفسير الأخطاء يبقى كما هو
        sell_order_result = await retry_blocking_api_call(
            client.order_market_sell,
            symbol=symbol_api_format,
            quantity=quantity_as_string,
            symbol_for_log=f"Manual Sell {symbol}"
        )
        
        if sell_order_result and sell_order_result.get('error'):
            error_code = sell_order_result.get('code')
            api_error_message = sell_order_result.get('message', 'فشل غير محدد')
            
            if error_code == -1013:
                min_notional_val = s_info.get('minNotional', 10.0)
                return (f"⛔ فشل بيع {symbol}.\n\n"
                        f"السبب: قيمة الكمية أقل من الحد الأدنى للبيع ({min_notional_val}$).\n\n"
                        f"الحل: استخدم خاصية تحويل الأرصدة الصغيرة إلى BNB في محفظتك على Binance.")
            elif error_code == -2010:
                return (f"⛔ فشل بيع {symbol}.\n\n"
                        f"السبب: رصيد غير كافٍ. قد يكون رصيدك محجوز في أمر آخر.\n\n"
                        f"الحل: تأكد من عدم وجود أوامر مفتوحة لهذه العملة على المنصة، ثم حاول مجددًا.")
            else:
                return f"⛔ فشل أمر البيع لـ {symbol}.\nالسبب من Binance: {api_error_message}"

        if not sell_order_result or sell_order_result.get('status') != 'FILLED':
            logger.error(f"Manual sell for {symbol} failed or did not fill immediately. API Response: {sell_order_result}")
            return f"⛔ فشل أمر البيع لـ {symbol}.\nالسبب: لم يتم تأكيد التنفيذ من Binance."

        # إذا نجح البيع، نستدعي دالة التقرير
        final_price = float(sell_order_result.get('cummulativeQuoteQty')) / float(sell_order_result.get('executedQty'))
        
        # --- [ بداية الإضافة والتصحيح ] ---
        # 1. تعريف متغير جديد لتخزين الكمية المباعة الفعلية من نتيجة الأمر
        executed_qty = float(sell_order_result.get('executedQty'))
        # --- [ نهاية الإضافة والتصحيح ] ---
        
        # إذا كانت الصفقة نشطة (وجدنا لها بيانات)، نرسل تقريراً كاملاً
        if trade_snapshot:
            # --- [ بداية التعديل: استخدام المتغير الصحيح ] ---
            # نستدعي دالة التقرير الكاملة مع 'executed_qty' بدلاً من 'quantity'
            await close_and_report(symbol_key_format, "إغلاق يدوي", final_price, executed_qty)
            # --- [ نهاية التعديل ] ---
            
            # إلغاء مهمة المراقبة الخاصة بهذه الصفقة (يبقى كما هو)
            monitor_task_to_cancel = trade_snapshot.get('monitor_task')
            if monitor_task_to_cancel and not monitor_task_to_cancel.done():
                monitor_task_to_cancel.cancel()
                logger.info(f"Manual Sell: Cancelled monitor task for {symbol_key_format}.")
                
            # تم نقل هذه الرسالة إلى داخل close_and_report، لكن يمكن إبقاؤها هنا إذا أردت
            return f"✅ تم إغلاق الصفقة {symbol} بنجاح."
        else:
            # إذا لم تكن صفقة نشطة، نرجع فقط رسالة بيع بسيطة
            # نستخدم المتغير الجديد 'executed_qty' هنا أيضًا للاتساق
            return f"✅ تم بيع {executed_qty:.8f} من الأصل {symbol} بنجاح (لم تكن صفقة مُراقبة)."

    except Exception as e:
        logger.error(f"Unexpected error during manual sell_asset for {symbol}: {e}", exc_info=True)
        return f"⚠️ خطأ غير متوقع أثناء بيع {symbol}: {str(e)}"

# معالج الضغط على زر "بيع جميع الأصول المعروضة" (يستخدم الكاش)
@dp.callback_query_handler(lambda c: c.data.startswith("sell_all_displayed:"))
async def sell_all_displayed_assets_handler(callback_query: types.CallbackQuery):
    await callback_query.message.delete_reply_markup()
    
    try:
        _, cache_id = callback_query.data.split(":")
        
        # --- [ بداية الإضافة ] ---
        # جلب البيانات من الكاش باستخدام المعرف
        assets_to_sell = assets_display_cache.get(cache_id)
        
        if not assets_to_sell:
            await bot.answer_callback_query(callback_query.id, "البيانات منتهية الصلاحية، يرجى عرض الأصول مجددًا.", show_alert=True)
            return
        # --- [ نهاية الإضافة ] ---

        loading_msg = await callback_query.message.answer(f"⏳ جارٍ محاولة بيع {len(assets_to_sell)} أصول")
        
        results = []
        client = create_client()

        for asset in assets_to_sell:
            symbol = asset['symbol']
            result = await sell_asset(client, symbol)
            results.append(result + '\n------------\n')
            await asyncio.sleep(0.3) # تأخير بسيط بين الأوامر
        
        # حذف البيانات من الكاش بعد استخدامها
        assets_display_cache.pop(cache_id, None)

        final_message = "--- 📊 نتائج بيع الأصول ---\n\n" + "\n".join(results)
        
        await loading_msg.edit_text(final_message, parse_mode="Markdown")
        await bot.answer_callback_query(callback_query.id, text="✅ اكتملت عملية البيع.")
    
    except Exception as e:
        await bot.answer_callback_query(callback_query.id, text=f"⚠️ حدث خطأ: {str(e)}", show_alert=True)
        logging.error(f"Error in sell_all_displayed_assets_handler: {traceback.format_exc()}")

def get_trade_statistics():
    if total_trades == 0:
        return "⚠️ لم يتم تنفيذ أي صفقة حتى الآن."
    
    global net_profit_usdt
    total_profit = sum(trade['profit_loss'] for trade in trade_history if trade['profit_loss'] > 0)
    total_loss = sum(trade['profit_loss'] for trade in trade_history if trade['profit_loss'] < 0)

    net_profit_percent = total_profit + total_loss

    closed_trades = successful_trades + failed_trades
    success_rate = (successful_trades / closed_trades) * 100 if closed_trades > 0 else 0

    profit_emoji = "🟢" if net_profit_usdt >= 0 else "🔴"

    return (
        "📊 إحصائيات التداول الحالية:\n"
        "━━━━━━━━━━━━━━━\n"
        f"📋 إجمالي الصفقات: {total_trades}\n"
        f"✔️ الناجحة: {successful_trades}\n"
        f"❌ الفاشلة: {failed_trades}\n"
        f"⏳ المعلقة: {pending_trades}\n"
        "━━━━━━━━━━━━━━━\n"
        f"📈 نسبة النجاح: {success_rate:.2f}%\n"
        f"💹 الربح الصافي (نسبة): {net_profit_percent:+.2f}%\n"
        f"{profit_emoji} الربح الصافي (USDT): {net_profit_usdt:+.2f}$"
    )

@dp.message_handler(commands=["start"])
async def send_welcome(message: types.Message):
    # تجهيز لوحة المفاتيح المناسبة
    markup = get_main_keyboard(message.chat.id)

    # محاولة استخراج الاسم أو اسم المستخدم
    user_name = message.from_user.first_name or message.from_user.username or "مستخدم"

    if message.chat.id not in ADMIN_USER_IDS_LIST:
        await message.answer(
            f"👋 مرحبًا {user_name}!\n\n"
            "📊 يمكنك متابعة الإحصائيات والصفقات.\n\n"
            "⚠️ إذا كنت مهتمًا بالاشتراك الكامل في البوت، اضغط على زر \"📞 تواصل معنا للاشتراك\".",
            reply_markup=markup
        )
        return

    await message.answer(
        f"👋 مرحبًا {user_name}!\n"
        "🤖 البوت جاهز للاستعمال.",
        reply_markup=markup
    )

@dp.message_handler(filters.Text(equals="📊 إحصائيات الصفقات"))
async def handle_stats(message: types.Message):
    stats = get_trade_statistics()
    await message.answer(stats)

@dp.message_handler(filters.IDFilter(user_id=ADMIN_USER_IDS_LIST))
async def handle_admin_commands(message: types.Message):
    global bot_active

    if message.text == "✅ تشغيل البوت":
        if bot_active:
            await message.answer("🟢 البوت يعمل بالفعل!")
        else:
            bot_active = True
            await message.answer("✅ تم تشغيل البوت بنجاح!")

    elif message.text == "⛔ إيقاف البوت":
        if not bot_active:
            await message.answer("🔴 البوت متوقف بالفعل!")
        else:
            bot_active = False
            await message.answer("⛔ تم إيقاف البوت بنجاح!")

@dp.message_handler()
async def fallback(message: types.Message):
    await message.answer("⚠️ أمر غير معروف أو لا تملك صلاحية استخدام هذا الأمر.")

async def on_startup_polling(dp):
    logger.info("Aiogram polling started. Performing setup...")
    
    # تحميل الإعدادات الأولية من الملفات
    load_halal_symbols()
    load_all_stats()
    
    # تعريف المتغيرات العامة التي سيتم تعديلها
    global client, use_testnet, client_telegram, ACTIVE_BOT_USERNAME, ADMIN_USER_IDS_LIST, bot
    
    client_initialized = False
    
    # --- [ بداية: منطق الاتصال الذكي بـ Binance ] ---
    
    # الخطوة 1: محاولة الاتصال بناءً على الإعداد الأولي
    if use_testnet:
        try:
            logger.info("Attempting to connect to Binance Testnet as per initial setting...")
            client = Client(TESTNET_API_KEY, TESTNET_API_SECRET, testnet=True)
            client_initialized = True
            logger.info("Successfully connected to Binance Testnet.")
            
        except Exception as e_testnet:
            logger.error(f"Failed to connect to Testnet: {e_testnet}", exc_info=False)
            logger.warning("Failover: Testnet connection failed. Automatically switching to Real Account...")
            
            # إرسال رسالة تحذير أولية للمستخدم
            if ADMIN_USER_IDS_LIST:
                failover_message = "⚠️ **فشل الاتصال بالحساب التجريبي!**\n\n" \
                                   "سيحاول البوت الآن التحويل تلقائيًا إلى الحساب الحقيقي..."
                try:
                    await bot.send_message(ADMIN_USER_IDS_LIST[0], failover_message, parse_mode="Markdown")
                except Exception as e_msg:
                    logger.error(f"Failed to send failover message to admin: {e_msg}")
            
            # تفعيل آلية التحويل
            use_testnet = False 
    
    # الخطوة 2: إذا فشل التجريبي أو كان الإعداد حقيقيًا من البداية، اتصل بالحقيقي
    if not client_initialized:
        try:
            logger.info("Attempting to connect to Binance Real Account...")
            client = Client(API_KEY, API_SECRET)
            client_initialized = True
            logger.info("Successfully connected to Binance Real Account.")

            # إذا تم التحويل تلقائيًا، أرسل رسالة تأكيد
            if not use_testnet: # هذا الشرط يتحقق إذا تم التحويل أو كان الإعداد حقيقيًا
                 if ADMIN_USER_IDS_LIST:
                    real_account_msg = "✅ **تم الاتصال بالحساب الحقيقي بنجاح.**\n\nالبوت يعمل الآن على حسابك الحقيقي."
                    try:
                        await bot.send_message(ADMIN_USER_IDS_LIST[0], real_account_msg, parse_mode="Markdown")
                    except Exception as e_msg:
                        logger.error(f"Failed to send real account confirmation message to admin: {e_msg}")

        except BinanceAPIException as e_real:
            log_message = f"CRITICAL: Failed to connect to Real Account. Code: {e_real.code}, Msg: {e_real.message}"
            user_message = "⛔ **فشل الاتصال بالحساب الحقيقي!**\n\n" \
                           f"السبب: خطأ من Binance (Code: {e_real.code}). يرجى التأكد من صلاحية مفاتيح API وقيود الـ IP."
            logger.error(log_message, exc_info=False)
            if ADMIN_USER_IDS_LIST:
                await bot.send_message(ADMIN_USER_IDS_LIST[0], user_message, parse_mode="Markdown")
        
        except Exception as e_real_unexpected:
            logger.error(f"FATAL: An unexpected error occurred during Real Account client init: {e_real_unexpected}", exc_info=True)
            if ADMIN_USER_IDS_LIST:
                await bot.send_message(ADMIN_USER_IDS_LIST[0], "⛔ حدث خطأ فادح وغير متوقع أثناء الاتصال بالحساب الحقيقي.")
    
    # --- [ نهاية: منطق الاتصال الذكي بـ Binance ] ---

    # --- [ بداية: الاتصال بـ Telethon ] ---
    if client_telegram and not client_telegram.is_connected():
        logger.info("Connecting Telethon client...")
        try:
            await client_telegram.start()
            client_telegram.add_event_handler(new_signal_handler, events.NewMessage(chats=ACTIVE_BOT_USERNAME))
            asyncio.create_task(client_telegram.run_until_disconnected())
            logger.info(f"Telethon client connected and listening to: {ACTIVE_BOT_USERNAME}")
            
            # أضف هذا الكود في نفس المكان:
            #@client_telegram.on(events.NewMessage(chats=['testdztestdz']))
            #async def test_specific_channel(event):
            #   logger.info(f"TEST FROM testdztestdz: Chat {event.chat_id}: {event.message.message[:100]}")

            #client_telegram.add_event_handler(test_specific_channel, events.NewMessage(chats=['testdztestdz']))
        
        except Exception as e:
            logger.error(f"Failed to connect Telethon client: {e}", exc_info=True)
            if ADMIN_USER_IDS_LIST:
                await bot.send_message(ADMIN_USER_IDS_LIST[0], "⛔ فشل في الاتصال بـ Telethon. البوت لن يستقبل الإشارات.")
    # --- [ نهاية: الاتصال بـ Telethon ] ---

    # --- [ بداية: تحميل معلومات العملات (فقط إذا نجح الاتصال بأي حساب) ] ---
    if client_initialized:
        logger.info("Attempting to load symbol information...")
        loaded_from_cache = await load_symbol_info_from_cache_file()
        
        initial_load_success = False
        if loaded_from_cache:
            initial_load_success = True
        else:
            logger.info("Cache not available or outdated. Fetching initial symbol info from API...")
            initial_load_success = await initial_symbol_info_load_wrapper()  
        
        await load_and_resume_trades()
        
        if initial_load_success:
            logger.info("Initial symbol information load completed.")
            if ADMIN_USER_IDS_LIST:
                startup_message = "✅ تم تحميل معلومات العملات المستهدفة بنجاح.\nالبوت جاهز الآن."
                # لا نرسل هذه الرسالة إذا كنا قد أرسلنا رسالة تأكيد الحساب الحقيقي للتو
                # يمكنك إضافتها إذا أردت
            
            asyncio.create_task(connect_to_price_server())
            logger.info("Price Server connection task created.")
            #asyncio.create_task(periodic_symbol_info_updater())
            logger.info("Periodic symbol info updater task created.")
                
        else:
            logger.error("Failed to load initial symbol information. Bot may not function correctly for trading.")
            for admin_id in ADMIN_USER_IDS_LIST:
                try:
                    await bot.send_message(admin_id, "⚠️ فشل تحميل معلومات العملات الأولية. البوت قد لا يعمل بشكل صحيح. يرجى مراجعة السجلات.")
                except Exception as e_msg:
                    logger.error(f"Failed to send 'initial load failed' message to admin: {e_msg}")
    
    else:
        logger.critical("Could not establish connection with Binance on any account. Trading features will be disabled.")
    
    logger.info("Startup setup complete. Aiogram will now start polling for updates.")

async def on_shutdown_polling(_):
    logger.info("Aiogram polling is shutting down...")
    # يمكنك إرسال رسالة للمستخدمين المسموح لهم بأن البوت يتوقف
    for user_id in ADMIN_USER_IDS_LIST:
        try:
            await bot.send_message(user_id, "⏳ البوت قيد الايقاف...")
        except Exception:
            pass
    # إغلاق جلسة البوت بشكل آمن
    session = await bot.get_session()
    await session.close()

if __name__ == "__main__":
    if sys.platform == "win32":
        logging.info("Setting event loop policy for Windows: WindowsSelectorEventLoopPolicy")
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    # بدء Aiogram Telegram polling
    from aiogram import executor
    logger.info("Starting Aiogram polling...")
    
    try:
        executor.start_polling(dp, skip_updates=True, on_startup=on_startup_polling, on_shutdown=on_shutdown_polling)
    except Exception as e_poll:
        logger.critical(f"Aiogram polling failed critically: {e_poll}", exc_info=True)
    finally:
        logger.info("Aiogram polling stopped. Cleaning up remaining asyncio tasks.")
        # يمكنك إضافة منطق لإلغاء المهام المعلقة الأخرى هنا إذا لزم الأمر
        # tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        # for task in tasks:
        #     task.cancel()
        # await asyncio.gather(*tasks, return_exceptions=True)
