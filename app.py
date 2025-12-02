# -*- coding: utf-8 -*-
# --- HỆ THỐNG BHXH CHUYÊN NGHIỆP (PHIÊN BẢN GOOGLE GEMINI FREE + AUTO FIX) ---
import streamlit as st
import streamlit.components.v1 as components 
import pandas as pd
import sqlite3
import unidecode
import time
import os
import zipfile
import glob
import requests
from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta

# --- THƯ VIỆN AI (GOOGLE GEMINI) ---
try:
    import google.generativeai as genai
except ImportError:
    st.error("Chưa cài đặt thư viện Google AI. Vui lòng thêm 'google-generativeai' vào requirements.txt")
    st.stop()

# --- CẤU HÌNH ỨNG DỤNG ---
st.set_page_config(
    page_title="Hệ thống BHXH Việt Nam - AI Support",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==============================================================================
# 🔑 CẤU HÌNH BẢO MẬT (GOOGLE API)
# ==============================================================================
def configure_google_ai():
    # Lấy API Key từ Secrets (Tên biến: GOOGLE_API_KEY)
    api_key = st.secrets.get("GOOGLE_API_KEY")
    if not api_key:
        return False
    genai.configure(api_key=api_key)
    return True

ZALO_PHONE_NUMBER = "0986053006"
OWM_API_KEY = "3ec0c3bf9ff1be61e3c94060a1037713" 
NEWS_API_KEY = "39779fb4a0634d8fbfb86e2668d955e0"

# CÁC HẰNG SỐ BHXH
CHUAN_NGHEO = 1500000 
LUONG_CO_SO = 2340000 
MAX_MUC_DONG = 20 * LUONG_CO_SO 
TY_LE_DONG = 0.22 
HO_TRO_NGHEO = 0.50     
HO_TRO_CAN_NGHEO = 0.40 
HO_TRO_DAN_TOC = 0.30   
HO_TRO_KHAC = 0.20      

# Tên file dữ liệu
EXCEL_FILE = 'aaa.xlsb'
DB_FILE = 'bhxh_data.db'
ZIP_PART_PREFIX = 'bhxh_data.zip.' 

# ==============================================================================
# 🎨 GIAO DIỆN & CSS
# ==============================================================================
BHXH_BLUE = "#005b96"
BHXH_LIGHT_BLUE = "#e6f2ff"

st.markdown(f"""
    <style>
    .main {{ background-color: #f8f9fa; }}
    h1, h2, h3 {{ color: {BHXH_BLUE} !important; font-family: 'Arial', sans-serif; }}
    
    /* Chatbot Style */
    .stChatMessage {{ background-color: white; border-radius: 10px; padding: 10px; box-shadow: 0 1px 2px rgba(0,0,0,0.1); }}
    
    /* Marquee */
    .marquee-container {{ width: 100%; overflow: hidden; white-space: nowrap; background-color: {BHXH_LIGHT_BLUE}; padding: 10px 0; border-radius: 5px; border: 1px solid #dcdcdc; margin-bottom: 20px; }}
    .marquee-text {{ display: inline-block; padding-left: 100%; animation: marquee 25s linear infinite; color: {BHXH_BLUE}; font-weight: bold; font-size: 1.2rem; text-transform: uppercase; }}
    @keyframes marquee {{ 0% {{ transform: translate(0, 0); }} 100% {{ transform: translate(-100%, 0); }} }}

    /* News & Widget */
    .news-card {{ background-color: white; padding: 15px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 15px; border-left: 5px solid {BHXH_BLUE}; transition: transform 0.2s; }}
    .news-card:hover {{ transform: translateY(-3px); box-shadow: 0 4px 8px rgba(0,0,0,0.15); }}
    .news-title {{ font-weight: bold; color: {BHXH_BLUE}; font-size: 1.1em; text-decoration: none; }}
    .news-meta {{ font-size: 0.85em; color: #666; margin-top: 5px; }}
    .weather-widget {{ background: linear-gradient(135deg, #005b96 0%, #0082c8 100%); color: white; padding: 15px; border-radius: 10px; margin-bottom: 20px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); text-align: center; }}
    .weather-temp {{ font-size: 2.5em; font-weight: bold; margin: 0; }}
    
    /* Sidebar & Button */
    [data-testid="stSidebar"] {{ background-color: {BHXH_LIGHT_BLUE}; border-right: 1px solid #ddd; }}
    .stButton>button {{ background-color: {BHXH_BLUE}; color: white; border-radius: 5px; border: none; padding: 0.5rem 1rem; transition: all 0.3s; }}
    .stButton>button:hover {{ background-color: #004470; box-shadow: 0 4px 8px rgba(0,0,0,0.2); }}
    </style>
""", unsafe_allow_html=True)

# --- HEADER ---
def render_header():
    c1, c2 = st.columns([1.5, 8.5])
    with c1:
        try: st.image("https://upload.wikimedia.org/wikipedia/vi/thumb/a/a2/Logo_BHXH_VN.png/300px-Logo_BHXH_VN.png", width=100)
        except: st.warning("Logo Error") 
    with c2:
        st.markdown(f"""<div style="padding-top: 10px;"><h1 style="margin:0; font-size: 2rem;">HỆ THỐNG TRA CỨU & TÍNH TOÁN BHXH</h1><p style="margin:0; color: #666;">Cổng thông tin tiện ích hỗ trợ người dân (Tích hợp Google AI)</p></div>""", unsafe_allow_html=True)
    st.markdown(f"""<div class="marquee-container"><div class="marquee-text">BẢO HIỂM XÃ HỘI VIỆT NAM - TẤT CẢ VÌ AN SINH XÃ HỘI, VÌ NGƯỜI THAM GIA BHXH, BHYT &#127483;&#127475;</div></div>""", unsafe_allow_html=True)

def render_zalo_widget():
    st.markdown(f"""<style>.z{{position:fixed;bottom:20px;right:20px;width:60px;height:60px;z-index:9999;animation:s 3s infinite}}@keyframes s{{0%,100%{{transform:rotate(0deg)}}10%,30%{{transform:rotate(10deg)}}20%,40%{{transform:rotate(-10deg)}}}}</style><a href="https://zalo.me/{ZALO_PHONE_NUMBER}" target="_blank" class="z"><img src="https://upload.wikimedia.org/wikipedia/commons/thumb/9/91/Icon_of_Zalo.svg/1200px-Icon_of_Zalo.svg.png" width="100%"></a>""", unsafe_allow_html=True)

# --- 1. CHỨC NĂNG AI: CHATBOT (GEMINI AUTO-FALLBACK) ---
def render_chatbot_ai():
    st.subheader("🤖 Trợ lý AI Chuyên gia BHXH (Google Gemini)")
    st.caption("Hỏi đáp mọi vấn đề về Luật BHXH, BHYT, chế độ thai sản, ốm đau, hưu trí...")

    if not configure_google_ai():
        st.warning("⚠️ Chưa cấu hình API Key. Vui lòng vào Settings -> Secrets trên Streamlit Cloud để thêm 'GOOGLE_API_KEY'.")
        st.markdown("[Lấy Key miễn phí tại đây](https://aistudio.google.com/app/apikey)")
        return

    # Khởi tạo lịch sử chat
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Hiển thị lịch sử
    for message in st.session_state.messages:
        role = "user" if message["role"] == "user" else "assistant"
        with st.chat_message(role):
            st.markdown(message["content"])

    if prompt := st.chat_input("Nhập câu hỏi..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"): st.markdown(prompt)

        with st.chat_message("assistant"):
            message_placeholder = st.empty()
            full_response = ""
            
            # --- CƠ CHẾ TỰ ĐỘNG CHUYỂN MODEL KHI LỖI ---
            # Danh sách ưu tiên: Flash (nhanh) -> Pro (ổn định) -> 1.5 Pro (mạnh)
            models_to_try = ['gemini-1.5-flash', 'gemini-pro', 'gemini-1.5-pro']
            success = False
            last_error = ""

            for model_name in models_to_try:
                try:
                    # Cấu hình model
                    model = genai.GenerativeModel(model_name)
                    
                    # Tạo prompt context
                    system_prompt = "Bạn là chuyên gia tư vấn BHXH/BHYT Việt Nam. Trả lời ngắn gọn, chính xác, thân thiện."
                    full_prompt = f"{system_prompt}\n\nUser: {prompt}"
                    
                    # Gọi API (Stream)
                    response = model.generate_content(full_prompt, stream=True)
                    
                    for chunk in response:
                        if chunk.text:
                            full_response += chunk.text
                            message_placeholder.markdown(full_response + "▌")
                    
                    # Nếu chạy đến đây mà không lỗi thì thoát vòng lặp
                    success = True
                    break 
                except Exception as e:
                    last_error = str(e)
                    continue # Thử model tiếp theo
            
            if success:
                message_placeholder.markdown(full_response)
                st.session_state.messages.append({"role": "model", "content": full_response})
            else:
                st.error(f"⚠️ Hệ thống AI đang bận hoặc lỗi kết nối. Vui lòng thử lại sau giây lát.\nChi tiết lỗi: {last_error}")

# --- 2. CHỨC NĂNG AI: VIẾT BÀI TUYÊN TRUYỀN (GEMINI AUTO-FALLBACK) ---
def render_content_creator():
    st.subheader("✍️ AI Viết Bài Tuyên Truyền (Google Gemini)")
    
    if not configure_google_ai():
        st.warning("⚠️ Chưa cấu hình GOOGLE_API_KEY.")
        return

    c1, c2 = st.columns([1, 1])
    with c1:
        topic = st.text_input("Chủ đề bài viết:", placeholder="Ví dụ: Lợi ích BHYT học sinh sinh viên")
        target_audience = st.selectbox("Đối tượng:", ["Người lao động", "Học sinh sinh viên", "Người nội trợ", "Doanh nghiệp", "Toàn dân"])
    with c2:
        platform = st.selectbox("Nền tảng:", ["Facebook", "Zalo", "Website"])
        tone = st.select_slider("Giọng văn:", options=["Nghiêm túc", "Vừa phải", "Hài hước/Bắt trend"], value="Vừa phải")

    if st.button("🚀 Tạo nội dung ngay", type="primary"):
        if not topic:
            st.warning("Vui lòng nhập chủ đề.")
            return
        
        try:
            with st.spinner("AI đang viết bài..."):
                # --- CƠ CHẾ TỰ ĐỘNG CHUYỂN MODEL ---
                models_to_try = ['gemini-1.5-flash', 'gemini-pro', 'gemini-1.5-pro']
                result_text = ""
                success = False
                
                prompt_content = f"""
                Đóng vai chuyên viên truyền thông BHXH Việt Nam.
                Viết bài tuyên truyền về: {topic}.
                Đối tượng: {target_audience}.
                Nền tảng: {platform}.
                Giọng văn: {tone}.
                Yêu cầu: Hấp dẫn, nhiều emoji, có hashtag, kêu gọi hành động.
                """

                for model_name in models_to_try:
                    try:
                        model = genai.GenerativeModel(model_name)
                        response = model.generate_content(prompt_content)
                        result_text = response.text
                        success = True
                        break
                    except:
                        continue
                
                if success:
                    st.success("Đã tạo xong!")
                    st.text_area("Nội dung:", value=result_text, height=400)
                else:
                    st.error("⚠️ Không thể kết nối đến máy chủ AI lúc này. Vui lòng thử lại.")
                    
        except Exception as e:
            st.error(f"Lỗi: {e}")

# --- TIỆN ÍCH ---
def render_clock():
    components.html("""<!DOCTYPE html><html><head><style>body{margin:0;font-family:'Arial',sans-serif;background-color:transparent}.clock-container{background-color:#004470;color:white;padding:15px;border-radius:10px;text-align:center;border:2px solid #e6f2ff;box-shadow:0 2px 5px rgba(0,0,0,0.1);display:flex;flex-direction:column;justify-content:center;height:100px}.clock-title{font-size:12px;color:#ccc;margin-bottom:5px;text-transform:uppercase;letter-spacing:1px}.clock-time{font-size:32px;font-weight:bold;letter-spacing:2px;line-height:1;font-family:'Courier New',monospace}.clock-date{font-size:14px;margin-top:5px;color:#ddd;font-weight:bold}</style></head><body><div class="clock-container"><div class="clock-title">GIỜ VIỆT NAM (GMT+7)</div><div id="digital-clock" class="clock-time">00:00:00</div><div id="date-display" class="clock-date">dd/mm/yyyy</div></div><script>function updateClock(){const now=new Date();const optionsTime={timeZone:'Asia/Ho_Chi_Minh',hour12:false,hour:'2-digit',minute:'2-digit',second:'2-digit'};const optionsDate={timeZone:'Asia/Ho_Chi_Minh',day:'2-digit',month:'2-digit',year:'numeric'};try{const timeString=now.toLocaleTimeString('en-GB',optionsTime);const dateString=now.toLocaleDateString('en-GB',optionsDate);document.getElementById('digital-clock').innerHTML=timeString;document.getElementById('date-display').innerHTML=dateString}catch(e){document.getElementById('digital-clock').innerHTML="Loading..."}}setInterval(updateClock,1000);updateClock();</script></body></html>""", height=140)

def render_calculator_widget():
    st.markdown("### 🧮 Máy tính cá nhân")
    components.html("""<!DOCTYPE html><html><head><style>body{margin:0;font-family:sans-serif}.calc-container{background-color:#f8f9fa;padding:10px;border-radius:10px;border:1px solid #ddd;box-shadow:0 2px 4px rgba(0,0,0,0.1)}.calc-display{width:93%;height:40px;background:#fff;border:2px solid #ccc;margin-bottom:10px;text-align:right;padding:5px 10px;font-size:24px;font-family:'Courier New',monospace;border-radius:5px;color:#333;font-weight:bold}.calc-row{display:flex;justify-content:space-between;margin-bottom:8px}.calc-btn{width:23%;padding:12px 0;font-weight:bold;font-size:18px;border:1px solid #ccc;border-radius:5px;cursor:pointer;background-color:white;color:#333;transition:background-color 0.2s}.calc-btn:hover{background-color:#f0f0f0}.calc-btn:active{transform:translateY(2px)}.calc-btn.op{background-color:#005b96;color:white;border-color:#005b96}.calc-btn.clear{background-color:#d9534f;color:white;border-color:#d9534f}.calc-btn.equal{background-color:#5cb85c;color:white;border-color:#5cb85c}</style></head><body><div class="calc-container"><input type="text" id="calc-display" class="calc-display" readonly><div class="calc-row"><button class="calc-btn clear" onclick="clearDisplay()">C</button><button class="calc-btn op" onclick="appendOp('/')">÷</button><button class="calc-btn op" onclick="appendOp('*')">×</button><button class="calc-btn" onclick="backspace()">⌫</button></div><div class="calc-row"><button class="calc-btn" onclick="appendNum('7')">7</button><button class="calc-btn" onclick="appendNum('8')">8</button><button class="calc-btn" onclick="appendNum('9')">9</button><button class="calc-btn op" onclick="appendOp('-')">-</button></div><div class="calc-row"><button class="calc-btn" onclick="appendNum('4')">4</button><button class="calc-btn" onclick="appendNum('5')">5</button><button class="calc-btn" onclick="appendNum('6')">6</button><button class="calc-btn op" onclick="appendOp('+')">+</button></div><div class="calc-row"><button class="calc-btn" onclick="appendNum('1')">1</button><button class="calc-btn" onclick="appendNum('2')">2</button><button class="calc-btn" onclick="appendNum('3')">3</button><button class="calc-btn equal" onclick="calculate()" style="height:auto;grid-row:span 2;">=</button></div><div class="calc-row" style="margin-bottom:0"><button class="calc-btn" onclick="appendNum('0')" style="width:48%">0</button><button class="calc-btn" onclick="appendNum('.')">.</button><div style="width:23%"></div></div></div><script>const display=document.getElementById('calc-display');function appendNum(num){display.value+=num}function appendOp(op){display.value+=op}function clearDisplay(){display.value=''}function backspace(){display.value=display.value.slice(0,-1)}function calculate(){try{display.value=eval(display.value)}catch(e){display.value='Error';setTimeout(()=>display.value='',1500)}}</script></body></html>""", height=320)

@st.cache_data(ttl=900)
def get_weather_data():
    mock_data = {"main": {"temp": 26, "humidity": 78}, "weather": [{"description": "nắng nhẹ (dự báo)", "icon": "02d"}], "wind": {"speed": 3.5}, "cod": 200}
    try:
        lat, lon = "12.4468", "107.6247"
        url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={OWM_API_KEY}&units=metric&lang=vi"
        response = requests.get(url, timeout=3)
        return response.json() if response.status_code == 200 else mock_data
    except: return mock_data

def render_weather_widget():
    data = get_weather_data()
    if isinstance(data, dict) and 'main' in data:
        temp = int(data['main']['temp'])
        desc = data['weather'][0]['description']
        icon_code = data['weather'][0]['icon']
        humidity = data['main']['humidity']
        wind = data['wind']['speed']
        icon_url = f"https://openweathermap.org/img/wn/{icon_code}@2x.png"
        st.markdown(f"""<div class="weather-widget"><div style="font-weight:bold;margin-bottom:5px;">&#128205; Huyện Đắk Mil</div><div style="font-size:0.8em;margin-bottom:10px;">Tỉnh Đắk Nông</div><div style="display:flex;align-items:center;justify-content:center;"><img src="{icon_url}" width="60"><p class="weather-temp">{temp}°C</p></div><p class="weather-desc">{desc}</p><div class="weather-info">&#128167; Độ ẩm: {humidity}% | &#127788; Gió: {wind} m/s</div></div>""", unsafe_allow_html=True)
    else: st.warning("⚠️ Đang cập nhật...")

# --- DATA ---
def clean_text(text): return unidecode.unidecode(str(text)).lower().replace(' ', '') if pd.notna(text) else ""
def init_data_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn
def check_and_prepare_data():
    if os.path.exists(DB_FILE):
        try:
            conn = init_data_db(); res = conn.execute("SELECT count(*) FROM bhxh").fetchone(); conn.close()
            if res and res[0] > 0: return True, "Sẵn sàng"
        except: os.remove(DB_FILE)
    parts = sorted(glob.glob(f"{ZIP_PART_PREFIX}*"))
    if parts:
        msg = st.empty(); msg.info(f"📦 Đang nối dữ liệu...")
        try:
            with open("bhxh_full.zip", 'wb') as o:
                for p in parts: 
                    with open(p, 'rb') as i: o.write(i.read())
            msg.info("📦 Đang giải nén..."); 
            with zipfile.ZipFile("bhxh_full.zip", 'r') as z: z.extractall()
            if os.path.exists("bhxh_full.zip"): os.remove("bhxh_full.zip")
            msg.empty(); return True, "Restored"
        except Exception as e: return False, str(e)
    if os.path.exists(EXCEL_FILE): return import_excel_to_sqlite()
    return False, "⚠️ Thiếu dữ liệu"
def import_excel_to_sqlite():
    st.warning("⚠️ Đang nạp Excel...")
    conn = init_data_db(); 
    try:
        df = pd.read_excel(EXCEL_FILE, engine='pyxlsb'); 
        df.columns = [unidecode.unidecode(str(c)).strip().replace(' ', '_').lower() for c in df.columns]
        df = df.astype(str).replace(['nan', 'None', 'NaT'], '')
        df['master_search_idx'] = df.apply(lambda x: clean_text(' '.join(x.values)), axis=1)
        for col in df.columns: 
            if col != 'master_search_idx': df[f'idx_{col}'] = df[col].apply(clean_text)
        df.to_sql('bhxh', conn, if_exists='replace', index=False, chunksize=5000)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_master ON bhxh (master_search_idx)")
        conn.close(); return True, "Done"
    except Exception as e: conn.close(); return False, str(e)
@st.cache_data(ttl=3600)
def get_display_columns():
    conn = init_data_db()
    try:
        c = conn.cursor(); c.execute("PRAGMA table_info(bhxh)"); all = [r[1] for r in c.fetchall()]
        return [c for c in all if not c.startswith('idx_') and c != 'master_search_idx' and 'kcb' not in c.lower() and c != 'index']
    except: return []
    finally: conn.close()
def search_data(mode, q):
    conn = init_data_db(); cols = get_display_columns()
    if not cols: return pd.DataFrame()
    sel = ", ".join([f'"{c}"' for c in cols])
    try:
        if mode == 'simple': 
            k = clean_text(q); 
            if not k: return pd.DataFrame()
            return pd.read_sql_query(f'SELECT {sel} FROM bhxh WHERE master_search_idx LIKE ? LIMIT 50', conn, params=(f'%{k}%',))
        else:
            conds, vals = [], []
            for c, v in q.items():
                if v.strip():
                    conds.append(f'idx_{unidecode.unidecode(c).strip().replace(" ", "_").lower()} LIKE ?')
                    vals.append(f'%{clean_text(v)}%')
            if not conds: return pd.DataFrame()
            return pd.read_sql_query(f'SELECT {sel} FROM bhxh WHERE {" AND ".join(conds)} LIMIT 50', conn, params=tuple(vals))
    except: return pd.DataFrame()
    finally: conn.close()

# --- OTHER TOOLS ---
def format_vnd(value): return f"{int(value):,} VNĐ".replace(",", ".")
def render_calculator():
    st.subheader("🧮 Tính Mức Đóng BHXH Tự Nguyện")
    col_inp, col_info = st.columns([2, 1])
    with col_inp:
        income = st.slider("Mức thu nhập lựa chọn:", min_value=CHUAN_NGHEO, max_value=MAX_MUC_DONG, value=CHUAN_NGHEO, step=50000, format="%d")
        st.info(f"Thu nhập chọn đóng: **{format_vnd(income)}**")
        exact_income = st.number_input("Hoặc nhập số chính xác:", min_value=CHUAN_NGHEO, max_value=MAX_MUC_DONG, value=income, step=1000)
        if exact_income != income: income = exact_income
    with col_info: st.info(f"""**Thông số cơ sở:**\n- Chuẩn nghèo: {format_vnd(CHUAN_NGHEO)}\n- Tỷ lệ đóng: 22%\n- Hỗ trợ tối đa: 10 năm""")
    doi_tuong = st.radio("Đối tượng ưu tiên:", ["Khác (Hỗ trợ 20%)", "Hộ nghèo (Hỗ trợ 50%)", "Hộ cận nghèo (Hỗ trợ 40%)", "Dân tộc thiểu số (Hỗ trợ 30%)"], horizontal=True)
    muc_dong_chuan = income * TY_LE_DONG
    if "Hộ nghèo" in doi_tuong: tile_hotro, hs = "50%", HO_TRO_NGHEO
    elif "Hộ cận nghèo" in doi_tuong: tile_hotro, hs = "40%", HO_TRO_CAN_NGHEO
    elif "Dân tộc" in doi_tuong: tile_hotro, hs = "30%", HO_TRO_DAN_TOC
    else: tile_hotro, hs = "20%", HO_TRO_KHAC
    muc_ho_tro = CHUAN_NGHEO * TY_LE_DONG * hs
    so_tien_thuc_dong = muc_dong_chuan - muc_ho_tro
    st.markdown(f"#### 📊 Bảng Chi Tiết (Hỗ trợ: {tile_hotro})")
    data = {"Phương thức": [], "Số tháng": [1, 3, 6, 12], "Tổng đóng (Gốc)": [], "Được hỗ trợ": [], "SỐ TIỀN PHẢI ĐÓNG": []}
    modes = ["Hằng tháng", "3 tháng", "6 tháng", "12 tháng"]
    for i, m in enumerate(data["Số tháng"]):
        data["Phương thức"].append(modes[i])
        data["Tổng đóng (Gốc)"].append(format_vnd(muc_dong_chuan * m))
        data["Được hỗ trợ"].append(format_vnd(muc_ho_tro * m))
        data["SỐ TIỀN PHẢI ĐÓNG"].append(format_vnd(so_tien_thuc_dong * m))
    st.dataframe(pd.DataFrame(data).style.highlight_max(axis=0, subset=["SỐ TIỀN PHẢI ĐÓNG"], color='#dbeeff'), use_container_width=True, hide_index=True)

def render_bhyt_calculator():
    st.subheader("🏥 Tính Tiền BHYT Hộ Gia Đình")
    st.caption(f"Lương cơ sở: **{format_vnd(LUONG_CO_SO)}** | Mức đóng: **4.5%**")
    c1, c2 = st.columns([1, 2])
    with c1:
        num_people = st.number_input("Số người:", min_value=1, max_value=20, value=1)
        base_rate = LUONG_CO_SO * 0.045
        details = []
        for i in range(1, num_people + 1):
            if i==1: r,n=1.0,"100%"
            elif i==2: r,n=0.7,"70%"
            elif i==3: r,n=0.6,"60%"
            elif i==4: r,n=0.5,"50%"
            else: r,n=0.4,"40%"
            c = base_rate * r
            details.append({"TV": f"Người {i}", "Giảm": n, "3 Tháng": format_vnd(c*3), "6 Tháng": format_vnd(c*6), "12 Tháng": format_vnd(c*12)})
    with c2: st.markdown(f"""<div style="background-color:#e6f2ff;padding:15px;border-radius:10px;text-align:center;"><h4 style="color:#005b96;">CHI TIẾT MỨC ĐÓNG</h4></div>""", unsafe_allow_html=True)
    st.dataframe(pd.DataFrame(details), use_container_width=True, hide_index=True)

def render_retirement_calculator():
    st.subheader("👴👵 Tính Tuổi Nghỉ Hưu")
    c1, c2 = st.columns(2)
    with c1: dob = st.date_input("Ngày sinh:", value=date(1970, 1, 1))
    with c2: gender = st.radio("Giới tính:", ["Nam", "Nữ"], horizontal=True)
    if st.button("Tính toán", type="primary"):
        target_years, target_months = 0, 0
        if gender == "Nam":
            if dob < date(1961, 1, 1): target_years = 60
            elif dob >= date(1966, 10, 1): target_years = 62
            else: target_years, target_months = 60, (dob.year - 1960) * 3
        else:
            if dob < date(1966, 1, 1): target_years = 55
            elif dob >= date(1980, 1, 1): target_years = 60
            else: target_years, target_months = 55, (dob.year - 1965) * 4
        add_years = target_months // 12
        final_age_years = target_years + add_years
        final_age_months = target_months % 12
        retire_date = dob + relativedelta(years=final_age_years, months=final_age_months)
        st.success(f"✅ Tuổi nghỉ hưu: {final_age_years} tuổi {final_age_months} tháng")
        st.info(f"📅 Thời điểm: Tháng {retire_date.month}/{retire_date.year}")

def render_statistics():
    st.subheader("📊 Thống Kê")
    conn = init_data_db()
    try:
        c = conn.cursor(); c.execute("PRAGMA table_info(bhxh)"); cols = [r[1] for r in c.fetchall()]
        col_dob = next((c for c in cols if 'ngaysinh' in unidecode.unidecode(c).lower()), None)
        if col_dob:
            df = pd.read_sql_query(f'SELECT "{col_dob}" as dob FROM bhxh', conn)
            df['Year'] = df['dob'].apply(lambda x: int(str(x).split('/')[-1]) if '/' in str(x) else 0)
            df_valid = df[(df['Year'] >= 1900) & (df['Year'] <= 2025)]
            if not df_valid.empty: st.bar_chart(df_valid['Year'].value_counts().sort_index())
            else: st.warning("Không có dữ liệu.")
    finally: conn.close()

@st.cache_data(ttl=1800)
def get_bhxh_news():
    try:
        url = "https://newsapi.org/v2/everything"
        params = {'q': 'bảo hiểm xã hội OR bảo hiểm y tế OR bhxh OR bhyt', 'language': 'vi', 'sortBy': 'publishedAt', 'apiKey': NEWS_API_KEY, 'pageSize': 50}
        response = requests.get(url, params=params, timeout=10)
        return response.json() if response.status_code == 200 else {}
    except: return {}
def render_news():
    st.subheader("📰 Tin Tức BHXH Mới Nhất")
    news_data = get_bhxh_news()
    if news_data.get('status') == 'ok':
        for article in news_data.get('articles', [])[:50]:
            if article.get('title') == '[Removed]': continue
            st.markdown(f"""<div class="news-card"><a href="{article.get('url')}" target="_blank" class="news-title">{article.get('title')}</a><div class="news-meta">📅 {article.get('publishedAt')[:10]} | {article.get('source', {}).get('name')}</div></div>""", unsafe_allow_html=True)
    else: st.info("Tin tức đang cập nhật...")

def render_search(cols):
    st.subheader("🔍 Tra Cứu Thông Tin")
    t1, t2 = st.tabs(["Tra cứu nhanh", "Tra cứu chi tiết"])
    with t1:
        q = st.text_input("Nhập từ khóa (Tên, Năm sinh...):", placeholder="vd: nguyen van a 1990")
        if q:
            df = search_data('simple', q)
            if not df.empty:
                st.success(f"Tìm thấy {len(df)} kết quả")
                st.dataframe(df, use_container_width=True, hide_index=True)
            else: st.warning("Không tìm thấy.")
    with t2:
        defs = ['sobhxh', 'hoten', 'ngaysinh', 'socmnd', 'madoituong', 'maho']
        sel = [c for c in cols if any(x in unidecode.unidecode(c).lower() for x in defs)] or cols[:4] 
        with st.expander("⚙️ Chọn trường tìm kiếm", expanded=False): 
            s = st.multiselect("Chọn các cột:", cols, default=sel)
        inp = {}
        if s:
            num_cols = 4
            cols_layout = st.columns(num_cols)
            for i, n in enumerate(s): 
                with cols_layout[i % num_cols]: inp[n] = st.text_input(f"Nhập {n}", key=f"manual_{n}")
        if st.button("🔍 Tìm kiếm chi tiết", type="primary"):
            v = {k: val for k, val in inp.items() if val.strip()}
            if v:
                df = search_data('manual', v)
                if not df.empty:
                    st.success(f"Tìm thấy {len(df)} kết quả")
                    st.dataframe(df, use_container_width=True, hide_index=True)
                else: st.warning("Không tìm thấy.")
            else: st.warning("Vui lòng nhập thông tin.")

# --- MAIN ---
def main():
    render_header()
    render_zalo_widget()
    ok, msg = check_and_prepare_data()
    if not ok: st.error(msg); return
    
    content_col, widget_col = st.columns([3, 1])
    
    with st.sidebar:
        st.title("MENU CHỨC NĂNG")
        
        # Nhóm tính năng AI
        st.markdown("---")
        st.caption("✨ TÍNH NĂNG AI MỚI")
        if st.button("🤖 Chatbot AI Support", use_container_width=True): st.session_state['page'] = 'chatbot'
        if st.button("✍️ Viết bài Tuyên truyền", use_container_width=True): st.session_state['page'] = 'content_writer'
        
        # Nhóm tính năng cơ bản
        st.markdown("---")
        st.caption("🔎 TRA CỨU & TIỆN ÍCH")
        if st.button("🔍 Tra cứu CSDL", use_container_width=True): st.session_state['page'] = 'search'
        if st.button("🔥 Tin tức BHXH (HOT)", use_container_width=True): st.session_state['page'] = 'news'
        
        # Nhóm tính toán
        st.markdown("---")
        st.caption("🧮 CÔNG CỤ TÍNH TOÁN")
        if st.button("Tính BHXH Tự Nguyện", use_container_width=True): st.session_state['page'] = 'calc'
        if st.button("Tính BHYT Hộ Gia Đình", use_container_width=True): st.session_state['page'] = 'bhyt'
        if st.button("Tính Tuổi Nghỉ Hưu", use_container_width=True): st.session_state['page'] = 'retirement'
        
        # Thống kê
        st.markdown("---")
        if st.button("📊 Thống kê Dữ liệu", use_container_width=True): st.session_state['page'] = 'stats'

    with widget_col:
        st.markdown("### 🛠️ Tiện ích")
        render_clock()
        render_weather_widget()
        render_calculator_widget()

    with content_col:
        if 'page' not in st.session_state: st.session_state['page'] = 'search'
        p = st.session_state['page']
        
        if p == 'search': 
            cols = get_display_columns()
            if cols: render_search(cols)
        elif p == 'chatbot': render_chatbot_ai()
        elif p == 'content_writer': render_content_creator()
        elif p == 'news': render_news()
        elif p == 'calc': render_calculator()
        elif p == 'bhyt': render_bhyt_calculator()
        elif p == 'retirement': render_retirement_calculator()
        elif p == 'stats': render_statistics()

if __name__ == '__main__':
    main()
