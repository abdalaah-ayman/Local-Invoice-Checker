# -*- coding: utf-8 -*-
import sys, os, time, math, traceback, random, threading
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ========= إعدادات عامة قابلة للتعديل =========
MAX_WORKERS          = 5            # عدد المتصفحات المتوازية
HEADLESS             = True         # تشغيل المتصفح في الخلفية
WAIT_SECONDS         = 15           # أقصى وقت انتظار لعنصر المبلغ
RETRIES_PER_ACCOUNT  = 3            # عدد المحاولات لكل حساب
SLEEP_BETWEEN_RETRY  = (1.0, 2.5)   # مهلة عشوائية بين المحاولات
MIN_CHUNK_SIZE       = 50           # أقل حجم للمجموعة
CHUNK_DIVISOR        = 10           # نقسم الملف إلى ~ 10 مجموعات
OUTPUT_DIR           = "results"    # مجلد النتائج الجزئية والنهائية
LOG_FILE             = "error_log.txt"  # ملف سجل الأخطاء
CHROMEDRIVER_PATH    = None         # ضع مسار chromedriver لو عايز تخصصه، أو خليه None
# ==============================================

# نضبط إخراج الكونسول لـ UTF-8 على قد ما نقدر
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

log_lock = threading.Lock()

def log_error(msg: str, exc: Exception | None = None):
    """تسجيل أي خطأ في ملف لوج مع الـ traceback بدون ما يعطل البرنامج."""
    with log_lock:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")
            if exc:
                f.write(traceback.format_exc() + "\n")

# ---- تنضيف وتحويل القيم بأمان ----
AR2EN = str.maketrans("٠١٢٣٤٥٦٧٨٩٫٬", "0123456789..")

def to_float_safe(x) -> float:
    """يحّول أي نص/رقم لمبلغ float بأمان (يشيل الفواصل، يحوّل أرقام عربية، الخ)."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return 0.0
    s = str(x).strip()
    if s == "" or s.lower() in {"nan", "none"}:
        return 0.0
    s = s.translate(AR2EN)
    # نشيل كل حاجة غير أرقام ونقطة
    clean = []
    dot_seen = False
    for ch in s:
        if ch.isdigit():
            clean.append(ch)
        elif ch == ".":
            if not dot_seen:
                clean.append(".")
                dot_seen = True
        # نتجاهل أي رموز تانية (مسافات/ريال/الخ)
    s2 = "".join(clean)
    if s2 == "" or s2 == ".":
        return 0.0
    try:
        return float(s2)
    except Exception as e:
        log_error(f"فشل تحويل '{x}' إلى رقم", e)
        return 0.0

def normalize_account(x) -> str | None:
    """يحوّل رقم الحساب لنص أرقام فقط (يشيل .0 وأي رموز)، أو None لو فاضي."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).strip().translate(AR2EN)
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits if digits else None

# ---- إنشاء متصفح كروم مضبوط ----
def build_chrome_options() -> webdriver.ChromeOptions:
    options = webdriver.ChromeOptions()
    if HEADLESS:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--lang=ar")
    # يقلل رسالة "Chrome is being controlled by automated test software"
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    return options

def create_driver() -> webdriver.Chrome:
    options = build_chrome_options()
    try:
        if CHROMEDRIVER_PATH:
            service = Service(CHROMEDRIVER_PATH)
            return webdriver.Chrome(service=service, options=options)
        return webdriver.Chrome(options=options)  # يعتمد على PATH
    except Exception as e:
        # محاولة احتياطية باستخدام webdriver-manager (لو النت متاح)
        try:
            from webdriver_manager.chrome import ChromeDriverManager
            service = Service(ChromeDriverManager().install())
            return webdriver.Chrome(service=service, options=options)
        except Exception as e2:
            log_error("فشل إنشاء المتصفح حتى بعد المحاولة الاحتياطية", e2)
            raise e

# ---- جلب المبلغ من زين بمحاولات متعددة ----
def fetch_amount_once(driver: webdriver.Chrome, account: str) -> float | None:
    """محاولة واحدة لجلب المبلغ."""
    if account.startswith("2"):
        url = f"https://app.sa.zain.com/ar/quickpay?account={account}"
    else:
        url = f"https://app.sa.zain.com/ar/contract-payment?contract={account}"

    driver.get(url)
    # ننتظر لغاية ما يظهر عنصر المبلغ
    try:
        elem = WebDriverWait(driver, WAIT_SECONDS).until(
            EC.presence_of_element_located((By.ID, "customAmount"))
        )
    except TimeoutException:
        return None
    except Exception:
        return None

    try:
        amount_text = elem.get_attribute("value") or ""
        amount = to_float_safe(amount_text)
        return amount
    except Exception as e:
        log_error(f"فشل قراءة قيمة المبلغ للحساب {account}", e)
        return None

def get_amount_from_zain_with_retry(account: str) -> float | None:
    """يحاول يجلب المبلغ بعدة محاولات، كل محاولة بمتصفح جديد لضمان الثبات."""
    for attempt in range(1, RETRIES_PER_ACCOUNT + 1):
        driver = None
        try:
            driver = create_driver()
            amount = fetch_amount_once(driver, account)
            if amount is not None:
                return amount
        except WebDriverException as e:
            log_error(f"WebDriverException (محاولة {attempt}) للحساب {account}", e)
        except Exception as e:
            log_error(f"استثناء غير متوقع (محاولة {attempt}) للحساب {account}", e)
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass
        # انتظار عشوائي بسيط بين المحاولات
        time.sleep(random.uniform(*SLEEP_BETWEEN_RETRY))
    # بعد كل المحاولات
    return None

# ---- مقارنة ----
def compare_amounts(excel_amount: float, zain_amount: float | None) -> str:
    if zain_amount is None:
        return "حصلت مشكلة أثناء الجلب"
    if zain_amount == 0:
        return "مصفر في زين"
    if zain_amount >= excel_amount:
        return "لسه مدفعش (غير مسدد)"
    # zain_amount < excel_amount
    return "سداد جزئي"

# ---- قراءة الإكسل مع مرونة أسماء الأعمدة ----
def detect_columns(df: pd.DataFrame) -> tuple[str, str]:
    # نحاول نلاقي عمود الحساب
    account_aliases = ["رقم الحساب", "الحساب", "Account", "account", "contract", "رقم العقد"]
    amount_aliases  = ["مبلغ المديونية", "المديونية", "amount", "Amount", "المبلغ"]

    def find_col(candidates):
        for c in df.columns:
            c_norm = str(c).strip()
            for a in candidates:
                if c_norm == a or c_norm.lower() == a.lower():
                    return c
        # محاولات مرنة
        for c in df.columns:
            c_norm = str(c).strip().lower()
            for a in candidates:
                if a.lower() in c_norm:
                    return c
        return None

    account_col = find_col(account_aliases)
    amount_col  = find_col(amount_aliases)
    if not account_col or not amount_col:
        raise RuntimeError(
            f"تعذّر العثور على الأعمدة. الأعمدة المتاحة: {list(df.columns)}.\n"
            f"محتاج عمود للحساب وعمود للمبلغ (مثلاً: 'رقم الحساب' و'مبلغ المديونية')."
        )
    return account_col, amount_col

# ---- معالجة مجموعة (Chunk) بترتيب ثابت وإحصائيات تقدم ----
from concurrent.futures import ThreadPoolExecutor, as_completed

def process_chunk(df_chunk: pd.DataFrame, chunk_id: int, total_accounts: int,
                  processed_counter: dict, start_time: float,
                  account_col: str, amount_col: str) -> pd.DataFrame:
    n = len(df_chunk)
    if n == 0:
        return df_chunk

    # نجهز مصفوفات بنفس ترتيب الصفوف
    zain_amounts = [None] * n
    results      = [""]   * n

    # نحول الصفوف لقائمة سجلات عشان نضمن نفس الترتيب
    records = df_chunk.to_dict("records")

    # نجهّز التنفيذ المتوازي
    futures = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for pos, rec in enumerate(records):
            account_raw = rec.get(account_col)
            acc = normalize_account(account_raw)
            excel_amount = to_float_safe(rec.get(amount_col))
            # نخزن التفاصيل مع الـ future
            fut = executor.submit(get_amount_from_zain_with_retry, acc) if acc else None
            futures[fut] = (pos, acc, excel_amount)

        # نستقبل النتائج
        for fut in as_completed([f for f in futures.keys() if f is not None]):
            pos, acc, excel_amount = futures[fut]
            zain_amount = None
            try:
                zain_amount = fut.result()
            except Exception as e:
                log_error(f"فشل future للحساب {acc}", e)
                zain_amount = None

            zain_amounts[pos] = zain_amount
            results[pos] = compare_amounts(excel_amount, zain_amount)

            # تحديث تقدّم عام
            processed_counter["done"] += 1
            done = processed_counter["done"]
            elapsed = time.time() - start_time
            avg = elapsed / done if done else 0.0
            remain = total_accounts - done
            eta = int(remain * avg)
            print(f"[Chunk {chunk_id}] تم: {done}/{total_accounts} | باقي: {remain} | تقدير الوقت المتبقي ~ {eta} ثانية")

    # نكتب الأعمدة الجديدة بنفس ترتيب الصفوف
    df_chunk["المبلغ من زين"] = zain_amounts
    df_chunk["النتيجة"]       = results

    # ملف جزئي
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    part_file = os.path.join(OUTPUT_DIR, f"contracts_results_part{chunk_id}.xlsx")
    try:
        df_chunk.to_excel(part_file, index=False)
        print(f"تم إنشاء الملف الجزئي: {part_file}")
    except Exception as e:
        log_error(f"فشل حفظ الملف الجزئي للمجموعة {chunk_id}", e)

    return df_chunk

# ---- البرنامج الرئيسي ----
def process_excel_parallel(filename: str):
    t0 = time.time()
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    try:
        df = pd.read_excel(filename)
    except Exception as e:
        raise RuntimeError(f"فشل قراءة ملف الإكسل '{filename}'") from e

    account_col, amount_col = detect_columns(df)

    # تنظيف الأعمدة (لا يغيّر القيم الأصلية—هنستخدم التحويل أثناء القراءة)
    total_accounts = len(df)
    if total_accounts == 0:
        print("الملف فاضي.")
        return

    # تقسيم ذكي
    chunk_size = max(MIN_CHUNK_SIZE, total_accounts // CHUNK_DIVISOR or MIN_CHUNK_SIZE)
    num_chunks = math.ceil(total_accounts / chunk_size)

    print(f"إجمالي الحسابات: {total_accounts}")
    print(f"هيتعمل تقسيم إلى {num_chunks} مجموعة (حجم كل مجموعة ~ {chunk_size})")
    print(f"تشغيل متوازي بعدد متصفحات: {MAX_WORKERS}")
    print("=" * 60)

    processed_counter = {"done": 0}
    all_chunks = []

    for cid in range(num_chunks):
        start = cid * chunk_size
        end   = min((cid + 1) * chunk_size, total_accounts)
        df_chunk = df.iloc[start:end].copy()

        print(f"\nبدء المجموعة {cid+1}/{num_chunks} من الصف {start} إلى {end-1}")
        chunk_result = process_chunk(
            df_chunk=df_chunk,
            chunk_id=cid + 1,
            total_accounts=total_accounts,
            processed_counter=processed_counter,
            start_time=t0,
            account_col=account_col,
            amount_col=amount_col,
        )
        all_chunks.append(chunk_result)

    # دمج وحفظ نهائي
    try:
        final_df = pd.concat(all_chunks, ignore_index=True)
    except Exception as e:
        log_error("فشل دمج النتائج النهائية", e)
        # نعمل دمج بديل لو في مشكلة
        final_df = pd.DataFrame()
        for ch in all_chunks:
            try:
                final_df = pd.concat([final_df, ch], ignore_index=True)
            except Exception:
                pass

    final_xlsx = os.path.join(OUTPUT_DIR, "contracts_results_final.xlsx")
    final_csv  = os.path.join(OUTPUT_DIR, "contracts_results_final.csv")

    try:
        final_df.to_excel(final_xlsx, index=False)
    except Exception as e:
        log_error("فشل حفظ Excel النهائي", e)

    try:
        final_df.to_csv(final_csv, index=False, encoding="utf-8-sig")
    except Exception as e:
        log_error("فشل حفظ CSV النهائي", e)

    # ملخّص
    total_time = int(time.time() - t0)
    print("\n" + "=" * 60)
    print("تمّ التنفيذ بالكامل بدون توقف.")
    print(f"الوقت الكلي: {total_time} ثانية")
    print(f"النتائج النهائية: {final_xlsx}")
    print(f"نسخة CSV:       {final_csv}")
    print(f"سجل الأخطاء (لو موجود): {LOG_FILE}")
    print("=" * 60)

# ---- تشغيل ----
if __name__ == "__main__":
    process_excel_parallel("contracts.xlsx")