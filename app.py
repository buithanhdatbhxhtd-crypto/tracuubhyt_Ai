import streamlit as st
import pandas as pd
import sqlite3
import hashlib
import datetime
import unidecode
import google.generativeai as genai
import time

# --- CẤU HÌNH ỨNG DỤNG ---
st.set_page_config(
    page_title="Hệ thống BHXH Thông Minh",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Tên file dữ liệu (Cần đảm bảo file này nằm cùng thư mục)
DATA_FILE = 'aaa.xlsb'

# --- 1. CÁC HÀM XỬ LÝ HỆ THỐNG & DATABASE ---
def init_db():
    """Khởi tạo cơ sở dữ liệu user và log"""
    conn = sqlite3.connect('users.db', check_same_thread=False)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (username TEXT PRIMARY KEY, password TEXT, role TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS logs
                 (timestamp TEXT, username TEXT, action TEXT, details TEXT)''')
    
    # Tạo admin mặc định (pass: admin123)
    c.execute("SELECT * FROM users WHERE username = 'admin'")
    if not c.fetchone():
        c.execute("INSERT INTO users VALUES (?, ?, ?)", 
                  ('admin', hashlib.sha256(str.encode('admin123')).hexdigest(), 'admin'))
    conn.commit()
    return conn

def make_hashes(password):
    return hashlib.sha256(str.encode(password)).hexdigest()

def log_action(username, action, details=""):
    try:
        conn = init_db()
        c = conn.cursor()
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("INSERT INTO logs VALUES (?, ?, ?, ?)", (ts, username, action, details))
        conn.commit()
        conn.close()
    except:
        pass

def configure_gemini():
    """Cấu hình API Key an toàn, tránh lỗi SecretNotFoundError"""
    api_key = None
    # 1. Thử lấy từ Secrets (Streamlit Cloud)
    try:
        api_key = st.secrets["GOOGLE_API_KEY"]
    except:
        pass # Bỏ qua nếu chạy local không có secrets
    
    # 2. Nếu không có, lấy từ Session State (Người dùng nhập tay)
    if not api_key:
        api_key = st.session_state.get('user_api_key', '')

    if api_key:
        genai.configure(api_key=api_key)
        return True
    return False

# --- 2. XỬ LÝ DỮ LIỆU (NÂNG CẤP TÌM KIẾM KHÔNG DẤU CÁCH) ---
@st.cache_data(show_spinner="Đang tối ưu hóa dữ liệu tìm kiếm...", ttl=3600)
def load_data_master():
    """
    Hàm nạp dữ liệu chỉ chạy 1 lần duy nhất.
    Tạo sẵn các cột chỉ mục (Index) đã XÓA KHOẢNG TRẮNG để tìm kiếm siêu linh hoạt.
    """
    try:
        start_time = time.time()
        # Đọc file với engine pyxlsb
        df = pd.read_excel(DATA_FILE, engine='pyxlsb')
        
        # Chuẩn hóa tên cột
        df.columns = [str(c).strip() for c in df.columns]
        
        # Chuyển đổi dữ liệu sang string và xử lý NaN
        df = df.astype(str)
        df.replace(['nan', 'None', 'NaT'], '', inplace=True)

        # --- TỐI ƯU TÌM KIẾM (BÍ QUYẾT Ở ĐÂY) ---
        # Helper function: Chuyển về không dấu VÀ xóa hết khoảng trắng
        def clean_text(text):
            if not text: return ''
            return unidecode.unidecode(text).lower().replace(' ', '')

        # 1. Tạo bản sao "sạch" cho TỪNG CỘT để tìm thủ công
        # Ví dụ: 'Nguyen Van A' -> 'nguyenvana'
        for col in df.columns:
            df[f'idx_{col}'] = df[col].apply(clean_text)

        # 2. Tạo "Siêu chỉ mục" (Master Index) cho tìm kiếm AI
        # Gộp hết thông tin -> Xóa dấu -> Xóa khoảng trắng
        # Ví dụ: Tên 'A', sinh '1990' -> 'a1990'
        df['Master_Search'] = df[[c for c in df.columns if not c.startswith('idx_')]].apply(
            lambda row: ' '.join(row.values.astype(str)), axis=1
        )
        df['Master_Search'] = df['Master_Search'].apply(clean_text)

        return df, f"Đã xử lý {len(df)} hồ sơ ({round(time.time() - start_time, 2)}s)"
    
    except FileNotFoundError:
        return None, "Lỗi: Không tìm thấy file dữ liệu."
    except Exception as e:
        return None, f"Lỗi nghiêm trọng: {str(e)}"

# --- 3. CÁC HÀM TRA CỨU (LOGIC MỚI) ---

def manual_search_logic(df, filters):
    """Tra cứu thủ công: So khớp trên dữ liệu đã xóa khoảng trắng"""
    if not filters:
        return pd.DataFrame()
    
    result_df = df.copy()
    
    for col, value in filters.items():
        if value:
            # Xử lý từ khóa tìm kiếm: Xóa dấu + Xóa khoảng trắng
            value_clean = unidecode.unidecode(value).lower().replace(' ', '')
            
            # Tìm trên cột index tương ứng (đã xóa khoảng trắng lúc nạp file)
            idx_col = f'idx_{col}'
            result_df = result_df[result_df[idx_col].str.contains(value_clean, na=False)]
            
            if result_df.empty:
                break
    
    return result_df

def ai_search_logic(df, keyword):
    """Tra cứu AI: So khớp trên cột tổng hợp đã xóa khoảng trắng"""
    if not keyword:
        return pd.DataFrame()
    
    # Xử lý từ khóa: Xóa dấu + Xóa khoảng trắng
    keyword_clean = unidecode.unidecode(keyword).lower().replace(' ', '')
    
    # Tìm trên cột Master_Search
    return df[df['Master_Search'].str.contains(keyword_clean, na=False)]

# --- 4. GIAO DIỆN NGƯỜI DÙNG ---

def render_login():
    st.markdown("<h2 style='text-align: center;'>🔐 Đăng Nhập Hệ Thống BHXH</h2>", unsafe_allow_html=True)
    
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        with st.form("login_form"):
            username = st.text_input("Tên đăng nhập")
            password = st.text_input("Mật khẩu", type='password')
            submitted = st.form_submit_button("Đăng nhập", use_container_width=True)
            
            if submitted:
                conn = init_db()
                c = conn.cursor()
                c.execute('SELECT * FROM users WHERE username = ? AND password = ?', 
                          (username, make_hashes(password)))
                data = c.fetchall()
                conn.close()
                
                if data:
                    st.session_state['logged_in'] = True
                    st.session_state['username'] = username
                    st.session_state['role'] = data[0][2]
                    log_action(username, "Login", "Success")
                    st.rerun()
                else:
                    st.error("Sai tên đăng nhập hoặc mật khẩu.")

def render_manual_search(df, cols_to_show):
    st.subheader("📋 Tra Cứu Thủ Công (Chính Xác)")
    st.info("💡 Mẹo: Bạn có thể viết liền không dấu (vd: 'nguyenvana', '01011990') hệ thống vẫn hiểu.")
    
    # Chọn các cột muốn dùng để lọc
    with st.expander("⚙️ Cấu hình bộ lọc", expanded=True):
        default_options = []
        # Tự động gợi ý các cột quan trọng
        for c in ['hoten', 'ngaysinh', 'socmnd', 'cccd', 'mabhxh', 'mathe']:
            for col in cols_to_show:
                if c in unidecode.unidecode(col).lower():
                    default_options.append(col)
        
        if not default_options: default_options = cols_to_show[:3]

        selected_filters = st.multiselect("Chọn tiêu chí tìm kiếm:", cols_to_show, default=default_options)

    # Tạo các ô input
    user_inputs = {}
    if selected_filters:
        cols = st.columns(len(selected_filters))
        for i, col_name in enumerate(selected_filters):
            user_inputs[col_name] = cols[i].text_input(f"Nhập {col_name}", key=f"search_{col_name}")

    if st.button("🔍 Tìm kiếm", type="primary"):
        valid_filters = {k: v for k, v in user_inputs.items() if v.strip() != ""}
        
        if valid_filters:
            results = manual_search_logic(df, valid_filters)
            st.success(f"Tìm thấy **{len(results)}** kết quả.")
            if not results.empty:
                st.dataframe(results[cols_to_show], use_container_width=True)
                log_action(st.session_state['username'], "Manual Search", str(valid_filters))
        else:
            st.warning("Vui lòng nhập ít nhất một thông tin.")

def render_ai_search(df, cols_to_show, is_ai_ready):
    st.subheader("🤖 Tra Cứu Thông Minh (Smart Search)")
    st.info("💡 Nhập bất cứ gì bạn nhớ: Tên dính liền, ngày sinh, số thẻ... (vd: 'nguyenthite1990')")
    
    query = st.text_input("Nhập từ khóa tìm kiếm:", placeholder="Ví dụ: nguyen van a 1990, 03001234...")
    
    if query:
        results = ai_search_logic(df, query)
        count = len(results)
        
        if count > 0:
            st.success(f"Tìm thấy **{count}** hồ sơ khớp với '{query}'")
            st.dataframe(results[cols_to_show], use_container_width=True)
            
            if count <= 3:
                st.markdown("---")
                st.markdown("### 💡 Phân tích từ AI")
                if not is_ai_ready:
                    st.warning("Chưa có API Key. Vui lòng nhập key ở menu bên trái để AI phân tích.")
                else:
                    for idx, row in results[cols_to_show].iterrows():
                        with st.expander(f"Chi tiết hồ sơ: {row.iloc[0]}", expanded=True):
                            with st.spinner("AI đang đọc dữ liệu..."):
                                try:
                                    model = genai.GenerativeModel('gemini-pro')
                                    prompt = f"""
                                    Dữ liệu BHXH: {row.to_dict()}
                                    Yêu cầu:
                                    1. Tóm tắt thông tin người này (Họ tên, tuổi, nơi ĐKCB).
                                    2. Giải thích các mã số/mức hưởng nếu có.
                                    3. Trả lời ngắn gọn bằng tiếng Việt.
                                    """
                                    response = model.generate_content(prompt)
                                    st.write(response.text)
                                except Exception as e:
                                    st.error(f"Lỗi AI: {e}")
            log_action(st.session_state['username'], "AI Search", query)
        else:
            st.warning("Không tìm thấy dữ liệu phù hợp.")

def render_admin():
    st.header("Quản Trị Hệ Thống")
    conn = init_db()
    
    tab1, tab2 = st.tabs(["Người dùng", "Nhật ký"])
    
    with tab1:
        c = conn.cursor()
        c.execute("SELECT username, role FROM users")
        st.dataframe(pd.DataFrame(c.fetchall(), columns=['User', 'Role']), use_container_width=True)
        
        with st.form("new_user"):
            st.write("Thêm người dùng mới")
            u = st.text_input("Username")
            p = st.text_input("Password", type="password")
            r = st.selectbox("Role", ["user", "admin"])
            if st.form_submit_button("Tạo"):
                try:
                    c.execute("INSERT INTO users VALUES (?,?,?)", (u, make_hashes(p), r))
                    conn.commit()
                    st.success("Tạo thành công!")
                    st.rerun()
                except:
                    st.error("Tên đăng nhập đã tồn tại.")
                    
    with tab2:
        if st.button("Xóa nhật ký"):
            c = conn.cursor()
            c.execute("DELETE FROM logs")
            conn.commit()
            st.rerun()
        
        c = conn.cursor()
        c.execute("SELECT * FROM logs ORDER BY timestamp DESC LIMIT 100")
        st.dataframe(pd.DataFrame(c.fetchall(), columns=['Time', 'User', 'Action', 'Details']), use_container_width=True)
    
    conn.close()

# --- 5. HÀM MAIN ---
def main():
    if 'logged_in' not in st.session_state:
        st.session_state['logged_in'] = False

    if not st.session_state['logged_in']:
        render_login()
    else:
        with st.sidebar:
            st.image("https://cdn-icons-png.flaticon.com/512/2303/2303512.png", width=50)
            st.title("Menu Điều Khiển")
            st.write(f"Xin chào, **{st.session_state['username']}**")
            
            with st.popover("Cấu hình AI Key"):
                key = st.text_input("Nhập Google Gemini API Key", type="password", value=st.session_state.get('user_api_key',''))
                if key:
                    st.session_state['user_api_key'] = key
                    st.success("Đã lưu key!")
            
            st.divider()
            
            # Logic điều hướng
            if 'page' not in st.session_state: st.session_state['page'] = 'search_manual'
            
            if st.button("📋 Tra cứu Thủ công", use_container_width=True):
                st.session_state['page'] = 'search_manual'
                st.rerun()
                
            if st.button("🤖 Tra cứu AI", use_container_width=True):
                st.session_state['page'] = 'search_ai'
                st.rerun()
                
            if st.session_state['role'] == 'admin':
                if st.button("🛠️ Quản trị", use_container_width=True):
                    st.session_state['page'] = 'admin'
                    st.rerun()
            
            st.divider()
            if st.button("Đăng xuất", use_container_width=True):
                log_action(st.session_state['username'], "Logout")
                st.session_state['logged_in'] = False
                st.rerun()

        # Nạp dữ liệu
        df, status_msg = load_data_master()
        
        if df is None:
            st.error(f"⚠️ {status_msg}. Vui lòng kiểm tra file 'aaa.xlsb'.")
            return

        cols_to_show = [c for c in df.columns if not c.startswith('idx_') and c != 'Master_Search']
        is_ai_ready = configure_gemini()

        # Điều hướng nội dung
        page = st.session_state['page']
        if page == 'search_manual':
            render_manual_search(df, cols_to_show)
        elif page == 'search_ai':
            render_ai_search(df, cols_to_show, is_ai_ready)
        elif page == 'admin':
            render_admin()

if __name__ == '__main__':
    init_db()
    main()
