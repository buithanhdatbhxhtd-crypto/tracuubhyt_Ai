# --- BƯỚC 1: CƯỠNG CHẾ CÀI ĐẶT THƯ VIỆN MỚI NHẤT ---
# Đoạn này sẽ chạy đầu tiên để đảm bảo server có thư viện hỗ trợ Gemini 1.5
import subprocess
import sys
import time

def install_libs():
    try:
        import google.generativeai as genai
        # Kiểm tra version, nếu cũ thì update
        import pkg_resources
        ver = pkg_resources.get_distribution("google-generativeai").version
        if ver < "0.7.2": # Yêu cầu bản mới hỗ trợ Flash
            print(f"Phát hiện bản cũ {ver}, đang nâng cấp...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "google-generativeai"])
            print("Đã nâng cấp xong!")
    except:
        # Nếu chưa có hoặc lỗi, cài mới luôn
        subprocess.check_call([sys.executable, "-m", "pip", "install", "google-generativeai", "pandas", "pyxlsb", "unidecode", "google-cloud-firestore"])

install_libs()

# --- BƯỚC 2: IMPORT THƯ VIỆN ---
import streamlit as st
import pandas as pd
import sqlite3
import hashlib
import datetime
import unidecode
import os
import zipfile
import glob
import google.generativeai as genai

# Thử import Firestore (Cloud)
try:
    from google.cloud import firestore
    from google.oauth2 import service_account
    CLOUD_AVAILABLE = True
except:
    CLOUD_AVAILABLE = False

# --- CẤU HÌNH ỨNG DỤNG ---
st.set_page_config(
    page_title="Hệ thống BHXH Chuyên Nghiệp",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==============================================================================
# 🔑 CẤU HÌNH HỆ THỐNG
# ==============================================================================
# Thay API Key của bạn vào đây
HARDCODED_API_KEY = "AIzaSyCw8kpB4mr_rw9IAh3-UOoaQfB8y_x16NE" 
ZALO_PHONE_NUMBER = "0986053006" 

# HẰNG SỐ TÍNH TOÁN 2025
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
USER_DB_LOCAL = 'users_local.db'

# --- 1. KẾT NỐI DATABASE (CLOUD & LOCAL) ---
@st.cache_resource
def get_firestore_db():
    if not CLOUD_AVAILABLE: return None
    try:
        # Ưu tiên lấy từ Secrets
        if "gcp_service_account" in st.secrets:
            key_dict = dict(st.secrets["gcp_service_account"])
            creds = service_account.Credentials.from_service_account_info(key_dict)
            return firestore.Client(credentials=creds, project=key_dict["project_id"])
    except: return None
    return None

def init_local_db():
    conn = sqlite3.connect(USER_DB_LOCAL, check_same_thread=False)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT, role TEXT)''')
    # Tạo admin mặc định nếu chưa có
    c.execute("SELECT * FROM users WHERE username = 'admin'")
    if not c.fetchone():
        c.execute("INSERT INTO users VALUES (?, ?, ?)", ('admin', make_hashes('admin123'), 'admin'))
    conn.commit()
    return conn

def make_hashes(password):
    return hashlib.sha256(str.encode(password)).hexdigest()

# --- QUẢN LÝ USER ---
def create_user(username, password, role):
    db = get_firestore_db()
    if db: # Lưu Cloud
        doc_ref = db.collection("users").document(username)
        if doc_ref.get().exists: return False 
        doc_ref.set({"password": make_hashes(password), "role": role, "created_at": datetime.datetime.now()})
        return True
    else: # Lưu Local
        try:
            conn = init_local_db()
            conn.execute("INSERT INTO users VALUES (?, ?, ?)", (username, make_hashes(password), role))
            conn.commit(); conn.close()
            return True
        except: return False

def verify_login(username, password):
    db = get_firestore_db()
    if db: # Check Cloud
        doc = db.collection("users").document(username).get()
        if doc.exists and doc.to_dict()["password"] == make_hashes(password):
            return doc.to_dict()["role"]
    
    # Check Local
    conn = init_local_db()
    res = conn.execute("SELECT role FROM users WHERE username=? AND password=?", (username, make_hashes(password))).fetchone()
    conn.close()
    if res: return res[0]
    return None

def update_password(username, new_password):
    db = get_firestore_db()
    success = False
    if db: 
        try: 
            db.collection("users").document(username).update({"password": make_hashes(new_password)})
            success = True
        except: pass
    
    try:
        conn = init_local_db()
        conn.execute("UPDATE users SET password=? WHERE username=?", (make_hashes(new_password), username))
        conn.commit(); conn.close()
        success = True
    except: pass
    return success

def delete_user(username):
    db = get_firestore_db()
    if db: db.collection("users").document(username).delete()
    
    conn = init_local_db()
    conn.execute("DELETE FROM users WHERE username=?", (username,))
    conn.commit(); conn.close()
    return True

def get_all_users():
    data = []
    db = get_firestore_db()
    if db:
        try: 
            docs = db.collection("users").stream()
            for doc in docs: data.append({"source": "Cloud", "username": doc.id, **doc.to_dict()})
        except: pass
    
    if not data: # Fallback local
        conn = init_local_db()
        df = pd.read_sql("SELECT * FROM users", conn)
        conn.close()
        if not df.empty:
            df['source'] = 'Local'
            return df
            
    return pd.DataFrame(data)

# --- LOGS ---
def log_action(username, action, details=""):
    try:
        db = get_firestore_db()
        if db:
            # Giờ VN = UTC + 7
            vn_time = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=7)))
            db.collection("logs").add({
                "timestamp": vn_time.strftime("%Y-%m-%d %H:%M:%S"),
                "sort_time": firestore.SERVER_TIMESTAMP,
                "username": username, "action": action, "details": str(details)
            })
    except: pass

def get_logs(limit=100):
    db = get_firestore_db()
    if not db: return pd.DataFrame()
    try:
        logs_ref = db.collection("logs").order_by("sort_time", direction=firestore.Query.DESCENDING).limit(limit)
        data = []
        for doc in logs_ref.stream():
            d = doc.to_dict()
            data.append({
                "Thời gian (VN)": d.get("timestamp"),
                "Người dùng": d.get("username"),
                "Hành động": d.get("action"),
                "Chi tiết": d.get("details")
            })
        return pd.DataFrame(data)
    except: return pd.DataFrame()

def init_admin_account():
    if "admin_checked" not in st.session_state:
        # Kiểm tra cả Cloud và Local
        if verify_login("admin", "admin123") is None:
            create_user("admin", "admin123", "admin")
        st.session_state["admin_checked"] = True

def render_zalo_widget():
    st.markdown(f"""
    <style>
    .z{{position:fixed;bottom:20px;right:20px;width:60px;height:60px;z-index:9999;animation:s 3s infinite}}
    @keyframes s{{0%,100%{{transform:rotate(0deg)}}10%,30%{{transform:rotate(10deg)}}20%,40%{{transform:rotate(-10deg)}}}}
    </style>
    <a href="https://zalo.me/{ZALO_PHONE_NUMBER}" target="_blank" class="z" title="Chat Zalo">
        <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/9/91/Icon_of_Zalo.svg/1200px-Icon_of_Zalo.svg.png" width="100%">
    </a>
    """, unsafe_allow_html=True)

# --- 2. HỆ THỐNG AI (FIX LỖI) ---
def configure_ai():
    # Ưu tiên lấy key từ code trước cho chắc ăn
    key = HARDCODED_API_KEY 
    if not key: key = st.secrets.get("GOOGLE_API_KEY")
    if key: 
        genai.configure(api_key=key)
        return True
    return False

@st.cache_resource(show_spinner=False)
def get_working_model():
    """Hàm kiểm tra model nào hoạt động"""
    # Danh sách ưu tiên
    models = ["gemini-1.5-flash", "gemini-1.5-pro", "gemini-1.0-pro", "gemini-pro"]
    
    for m in models:
        try:
            # Test thử 1 lệnh
            test_model = genai.GenerativeModel(m)
            test_model.generate_content("Test")
            return m # Nếu chạy được thì trả về tên model
        except:
            continue
    return None

def get_ai_response(prompt, role_desc="", stream=False):
    if not configure_ai(): return "⚠️ Lỗi: Chưa có API Key."
    
    model_name = get_working_model()
    if not model_name:
        return "⚠️ Hệ thống AI đang bảo trì hoặc Key bị lỗi. Vui lòng thử lại sau."
    
    full_prompt = f"{role_desc}\n\n{prompt}" if role_desc else prompt
    try:
        model = genai.GenerativeModel(model_name)
        if stream: return model.generate_content(full_prompt, stream=True)
        return model.generate_content(full_prompt).text
    except Exception as e:
        return f"⚠️ Lỗi khi gọi AI ({model_name}): {str(e)}"

# --- 3. XỬ LÝ DỮ LIỆU ---
def clean(text): return unidecode.unidecode(str(text)).lower().replace(' ', '') if pd.notna(text) else ""

def init_data_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def check_data():
    if os.path.exists(DB_FILE):
        try: 
            c=init_data_db(); c.execute("SELECT count(*) FROM bhxh"); c.close(); return True, "OK"
        except: os.remove(DB_FILE)
    
    parts = sorted(glob.glob(f"{ZIP_PART_PREFIX}*"))
    if parts:
        msg = st.empty(); msg.info(f"📦 Đang nối dữ liệu ({len(parts)} phần)...")
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
    return False, "⚠️ Không tìm thấy dữ liệu. Vui lòng upload file."

def import_excel_to_sqlite():
    st.warning("⚠️ Đang nạp Excel (Chậm).")
    conn = init_data_db(); msg = st.empty(); bar = st.progress(0)
    try:
        msg.info("⏳ Đang xử lý..."); df = pd.read_excel(EXCEL_FILE, engine='pyxlsb'); bar.progress(30)
        df.columns = [unidecode.unidecode(str(c)).strip().replace(' ', '_').lower() for c in df.columns]
        df = df.astype(str).replace(['nan','None'], '')
        df['idx']=df.apply(lambda x: clean(' '.join(x.values)), axis=1)
        for c in df.columns: 
            if c!='idx': df[f'i_{c}']=df[c].apply(clean)
        bar.progress(80)
        df.to_sql('bhxh', conn, if_exists='replace', index=False, chunksize=5000)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_m ON bhxh (idx)")
        bar.progress(100); msg.empty(); bar.empty(); conn.close(); return True, "Done"
    except Exception as e: conn.close(); return False, str(e)

@st.cache_data(ttl=3600)
def get_display_columns():
    c=init_data_db(); 
    try: return [r[1] for r in c.execute("PRAGMA table_info(bhxh)") if not r[1].startswith('i_') and r[1] not in ['idx','index'] and 'kcb' not in r[1]]
    except: return []
    finally: c.close()

def search(mode, q):
    c=init_data_db(); cols=get_display_columns()
    if not cols: return pd.DataFrame()
    sel = ", ".join([f'"{x}"' for x in cols])
    try:
        if mode=='ai': 
            k=clean(q); return pd.read_sql(f'SELECT {sel} FROM bhxh WHERE idx LIKE ? LIMIT 50', c, params=(f'%{k}%',))
        else:
            conds, vals = [], []
            for k,v in q.items():
                if v.strip(): conds.append(f'i_{clean(k)} LIKE ?'); vals.append(f'%{clean(v)}%')
            if not conds: return pd.DataFrame()
            return pd.read_sql(f'SELECT {sel} FROM bhxh WHERE {" AND ".join(conds)} LIMIT 50', c, params=tuple(vals))
    except: return pd.DataFrame()
    finally: c.close()

# --- TÍNH TOÁN BHXH ---
def format_vnd(v): return f"{int(v):,} VNĐ".replace(",", ".")

def render_calculator():
    st.markdown("## 🧮 Tính Mức Đóng BHXH Tự Nguyện (Luật 2025)")
    
    c1, c2 = st.columns([2, 1])
    with c1:
        inc = st.slider("Chọn mức thu nhập:", CHUAN_NGHEO, MAX_MUC_DONG, CHUAN_NGHEO, 50000, format="%d")
        exc = st.number_input("Hoặc nhập số cụ thể:", CHUAN_NGHEO, MAX_MUC_DONG, inc, 1000)
        if exc != inc: inc = exc
    
    with c2: 
        st.info(f"""
        **Thông số 2025:**
        - Chuẩn nghèo: {format_vnd(CHUAN_NGHEO)}
        - Tỷ lệ đóng: 22%
        """)
    
    dt = st.radio("Chọn đối tượng:", ["Khác (20%)", "Hộ nghèo (50%)", "Hộ cận nghèo (40%)", "Dân tộc (30%)"], horizontal=True)
    
    # Tính toán
    base = inc * TY_LE_DONG
    if "nghèo" in dt and "cận" not in dt: supp=base*HO_TRO_NGHEO; l="50%"
    elif "cận" in dt: supp=base*HO_TRO_CAN_NGHEO; l="40%"
    elif "Dân tộc" in dt: supp=base*HO_TRO_DAN_TOC; l="30%"
    else: supp=base*HO_TRO_KHAC; l="20%"
    
    final = base - supp
    
    st.markdown(f"### 📊 Kết Quả Chi Tiết (Hỗ trợ: {l})")
    data = {"Kỳ hạn": ["1 tháng", "3 tháng", "6 tháng", "12 tháng"], "Tháng": [1,3,6,12], "Tổng mức đóng": [], "Nhà nước hỗ trợ": [], "SỐ TIỀN PHẢI ĐÓNG": []}
    
    for m in data["Tháng"]:
        data["Tổng mức đóng"].append(format_vnd(base*m))
        data["Nhà nước hỗ trợ"].append(format_vnd(supp*m))
        data["SỐ TIỀN PHẢI ĐÓNG"].append(format_vnd(final*m))
    
    df_cal = pd.DataFrame(data)
    st.dataframe(df_cal.style.highlight_max(axis=0, subset=["SỐ TIỀN PHẢI ĐÓNG"], color='#d4edda'), use_container_width=True, hide_index=True)

# --- GIAO DIỆN CHÍNH & CSS ---
def inject_custom_css():
    st.markdown("""
    <style>
        .stApp {background-color: #f4f8fb; background-image: url("https://upload.wikimedia.org/wikipedia/vi/thumb/9/93/Logo_BHXH_Vi%E1%BB%87t_Nam.svg/1200px-Logo_BHXH_Vi%E1%BB%87t_Nam.svg.png"); background-repeat: no-repeat; background-position: center center; background-size: 40%; background-blend-mode: overlay;}
        .stApp::before {content: ""; position: absolute; top: 0; left: 0; width: 100%; height: 100%; background: rgba(255, 255, 255, 0.9); z-index: -1;}
        div[data-testid="stVerticalBlock"] > div {background: rgba(255, 255, 255, 0.85); backdrop-filter: blur(10px); border-radius: 15px; border: 1px solid rgba(255,255,255,0.5); padding: 15px; box-shadow: 0 4px 15px rgba(0,84,166,0.1);}
        h1, h2, h3 {color: #0054a6 !important; font-family: 'Arial', sans-serif;}
        section[data-testid="stSidebar"] {background: linear-gradient(180deg, #0054a6 0%, #003366 100%);}
        section[data-testid="stSidebar"] p, section[data-testid="stSidebar"] span, section[data-testid="stSidebar"] div {color: white !important;}
        .stButton>button {background: linear-gradient(90deg, #0054a6, #0078d4); color: white; border: none; border-radius: 8px;}
        .stButton>button:hover {transform: translateY(-2px); box-shadow: 0 4px 8px rgba(0,0,0,0.2);}
    </style>
    """, unsafe_allow_html=True)

def login():
    st.markdown("<br><br><h1 style='text-align: center;'>🔐 ĐĂNG NHẬP HỆ THỐNG</h1>", unsafe_allow_html=True)
    if not get_firestore_db(): st.toast("⚠️ Đang chạy chế độ Offline (Dữ liệu lưu trên máy)", icon="ℹ️")
    render_zalo_widget()
    c1,c2,c3=st.columns([1,2,1])
    with c2:
        with st.form("l"):
            u=st.text_input("Tên đăng nhập"); p=st.text_input("Mật khẩu", type='password')
            if st.form_submit_button("Đăng nhập", use_container_width=True):
                r=verify_login(u,p)
                if r: st.session_state.update({'logged_in':True,'username':u,'role':r}); log_action(u,"Login"); st.rerun()
                else: st.error("Sai thông tin")

def page_pass():
    st.markdown("## 🔒 Đổi Mật Khẩu")
    with st.form("p"):
        o=st.text_input("Mật khẩu cũ",type='password'); n=st.text_input("Mật khẩu mới",type='password'); c=st.text_input("Nhập lại mật khẩu mới",type='password')
        if st.form_submit_button("Xác nhận đổi"):
            u=st.session_state['username']
            if verify_login(u,o):
                if n==c and len(n)>=6:
                    if update_password(u,n): st.success("Thành công! Vui lòng đăng nhập lại."); log_action(u,"ChangePass"); time.sleep(1); st.session_state['logged_in']=False; st.rerun()
                    else: st.error("Lỗi hệ thống")
                else: st.warning("Mật khẩu mới không khớp hoặc quá ngắn")
            else: st.error("Mật khẩu cũ không đúng")

def page_search(cols):
    st.markdown("## 🔍 Tra Cứu Dữ Liệu")
    t1,t2=st.tabs(["Tra cứu Nhanh (AI)","Tra cứu Chi tiết"])
    with t1:
        q=st.text_input("Nhập từ khóa (Tên, Số thẻ...):", placeholder="Ví dụ: nguyen van a 1990")
        if q:
            log_action(st.session_state['username'],"Search AI",q)
            df=search('ai',q)
            if not df.empty:
                st.success(f"Tìm thấy {len(df)} kết quả"); st.dataframe(df,use_container_width=True,hide_index=True)
                if len(df)==1:
                    with st.expander("✨ AI Phân tích hồ sơ"): 
                        st.write(get_ai_response(f"Dữ liệu hồ sơ: {df.iloc[0].to_dict()}", "Bạn là chuyên gia BHXH. Hãy tóm tắt quyền lợi."))
            else: st.warning("Không tìm thấy kết quả nào.")
    with t2:
        defs=['sobhxh','hoten','ngaysinh','socmnd']; sel=[c for c in cols if any(x in clean(c) for x in defs)] or cols[:4]
        s=st.multiselect("Chọn cột tìm kiếm:",cols,default=sel); inp={}
        if s:
            c=st.columns(4)
            for i,n in enumerate(s): inp[n]=c[i%4].text_input(f"Nhập {n}")
        if st.button("🔍 Tìm kiếm ngay"):
            v={k:v for k,v in inp.items() if v.strip()}
            if v:
                log_action(st.session_state['username'],"Search Manual",str(v))
                df=search('manual',v)
                if not df.empty: st.success(f"Tìm thấy {len(df)} kết quả"); st.dataframe(df,use_container_width=True,hide_index=True)
                else: st.warning("Không tìm thấy.")
            else: st.warning("Vui lòng nhập ít nhất một trường thông tin.")

def page_chat():
    st.markdown("## 🤖 Trợ Lý Ảo BHXH")
    # Chỉ kiểm tra model 1 lần khi vào trang này
    if 'ai_model_ok' not in st.session_state:
        with st.spinner("Đang kết nối AI..."):
            if get_working_model(): st.session_state['ai_model_ok'] = True
            else: st.error("Không kết nối được AI. Vui lòng kiểm tra API Key.")

    if "msg" not in st.session_state: st.session_state.msg=[{"role":"model","content":"Xin chào! Tôi là trợ lý ảo chuyên về BHXH/BHYT. Tôi có thể giúp gì cho bạn hôm nay?"}]
    for m in st.session_state.msg: st.chat_message(m["role"]).markdown(m["content"])
    
    if p:=st.chat_input("Nhập câu hỏi..."):
        log_action(st.session_state['username'],"Chatbot",p)
        st.session_state.msg.append({"role":"user","content":p}); st.chat_message("user").markdown(p)
        with st.chat_message("model"):
            ph=st.empty(); res=""; 
            s=get_ai_response(p,"Bạn là chuyên gia tư vấn BHXH Việt Nam.",True)
            try:
                if isinstance(s,str): ph.markdown(s); res=s
                else:
                    for c in s: 
                        if c.text: res+=c.text; ph.markdown(res+"▌")
                    ph.markdown(res)
            except: ph.markdown(res)
            st.session_state.msg.append({"role":"model","content":res})

def page_content():
    st.markdown("## ✍️ Sáng Tạo Nội Dung")
    t=st.text_input("Nhập chủ đề bài viết:"); 
    if st.button("✨ Tạo nội dung ngay") and t:
        log_action(st.session_state['username'],"Content",t)
        with st.spinner("AI đang viết bài..."): 
            st.session_state['txt']=get_ai_response(f"Viết bài tuyên truyền về: {t}", "Bạn là chuyên viên truyền thông BHXH.")
    if 'txt' in st.session_state: st.text_area("Kết quả:",value=st.session_state['txt'],height=400)

def page_admin():
    st.markdown("## 🛠️ Quản Trị Hệ Thống")
    t1,t2=st.tabs(["Người dùng","Nhật ký hoạt động"])
    with t1:
        st.dataframe(get_all_users(),use_container_width=True)
        c1,c2,c3=st.columns(3)
        with c1:
            with st.popover("➕ Thêm User"):
                with st.form("a"):
                    u=st.text_input("Username"); p=st.text_input("Password"); r=st.selectbox("Role",["user","admin"])
                    if st.form_submit_button("Tạo"):
                        if create_user(u,p,r): st.success("Đã tạo!"); log_action(st.session_state['username'],"Add",u); time.sleep(1); st.rerun()
                        else: st.error("Tên đã tồn tại")
        with c2:
            with st.popover("🗑️ Xóa User"):
                d=st.text_input("Username cần xóa:")
                if st.button("Xóa vĩnh viễn"):
                    if d!="admin" and delete_user(d): st.success("Đã xóa!"); log_action(st.session_state['username'],"Del",d); time.sleep(1); st.rerun()
                    else: st.error("Lỗi")
        with c3:
            with st.popover("🔄 Reset Mật khẩu"):
                rs=st.text_input("Username cần reset:")
                if st.button("Reset về 123456"):
                    if update_password(rs,"123456"): st.success("Đã reset!"); log_action(st.session_state['username'],"Reset",rs)
                    else: st.error("Lỗi")
    with t2:
        if st.button("Tải lại nhật ký"): st.rerun()
        st.dataframe(get_logs(200),use_container_width=True)

def main():
    inject_custom_css() 
    init_admin_account()
    if 'logged_in' not in st.session_state: st.session_state.update({'logged_in':False,'page':'search'})
    render_zalo_widget()
    ok, msg = check_data()
    if not ok: st.error(msg); return
    
    if not st.session_state['logged_in']: render_login()
    else:
        with st.sidebar:
            # Logo BHXH
            st.image("https://upload.wikimedia.org/wikipedia/vi/9/93/Logo_BHXH_Vi%E1%BB%87t_Nam.svg", width=100)
            st.markdown(f"### Xin chào, {st.session_state['username']}!")
            st.markdown("---")
            if st.button("🔍 Tra cứu dữ liệu",use_container_width=True): st.session_state['page']='search'
            if st.button("🧮 Tính BHXH Tự nguyện",use_container_width=True): st.session_state['page']='calc'
            if st.button("🤖 Chatbot Tư vấn AI",use_container_width=True): st.session_state['page']='chat'
            if st.button("✍️ Tạo Nội dung",use_container_width=True): st.session_state['page']='content'
            st.markdown("---")
            if st.button("🔒 Đổi Mật khẩu",use_container_width=True): st.session_state['page']='pass'
            if st.session_state['role']=='admin':
                if st.button("🛠️ Quản trị hệ thống",use_container_width=True): st.session_state['page']='admin'
            st.markdown("---")
            if st.button("Đăng xuất",use_container_width=True):
                log_action(st.session_state['username'],"Logout"); st.session_state['logged_in']=False; st.rerun()
        
        p=st.session_state['page']; cols=get_cols()
        if p=='search': page_search(cols)
        elif p=='calc': render_calculator()
        elif p=='chat': page_chat()
        elif p=='content': page_content()
        elif p=='pass': page_pass()
        elif p=='admin': page_admin()

if __name__ == '__main__': main()
