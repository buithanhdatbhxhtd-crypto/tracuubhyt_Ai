import pandas as pd
import sqlite3
import unidecode
import time
import os

# Cấu hình tên file
INPUT_FILE = 'aaa.xlsb'
OUTPUT_DB = 'bhxh_data.db'

def clean_text(text):
    """Hàm làm sạch văn bản giống hệt trên App"""
    if pd.isna(text) or str(text).lower() == 'nan' or str(text).strip() == '': 
        return ""
    return unidecode.unidecode(str(text)).lower().replace(' ', '')

def convert_data():
    print(f"🚀 Bắt đầu xử lý file '{INPUT_FILE}'...")
    print("⚠️ Lưu ý: Với 2 triệu dòng, quá trình này có thể mất 5-10 phút và ngốn nhiều RAM. Hãy kiên nhẫn!")
    
    start_time = time.time()
    
    try:
        # 1. Đọc file Excel (Tốn RAM nhất ở bước này)
        print("--- Đang đọc file Excel (Vui lòng đợi)...")
        df = pd.read_excel(INPUT_FILE, engine='pyxlsb')
        print(f"--- Đã đọc xong {len(df)} dòng. Đang xử lý dữ liệu...")

        # 2. Chuẩn hóa tên cột
        df.columns = [unidecode.unidecode(str(c)).strip().replace(' ', '_').replace('.','').lower() for c in df.columns]
        
        # 3. Chuyển đổi dữ liệu sang string
        df = df.astype(str)
        df.replace(['nan', 'None', 'NaT', '<NA>'], '', inplace=True)

        # 4. Tạo cột Index tìm kiếm (Tối ưu tốc độ)
        print("--- Đang tạo chỉ mục tìm kiếm thông minh...")
        
        # Cột Master Index: Gộp tất cả các trường lại
        df['master_search_idx'] = df.apply(lambda x: clean_text(' '.join(x.values)), axis=1)
        
        # Cột Index riêng cho từng trường
        for col in df.columns:
            if col != 'master_search_idx' and not col.startswith('idx_'):
                df[f'idx_{col}'] = df[col].apply(clean_text)

        # 5. Lưu vào SQLite
        print("--- Đang ghi vào Database...")
        conn = sqlite3.connect(OUTPUT_DB)
        df.to_sql('bhxh', conn, if_exists='replace', index=False, chunksize=10000)
        
        # 6. Tạo Index SQL
        print("--- Đang tối ưu hóa Database (Indexing)...")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_master ON bhxh (master_search_idx)")
        conn.commit()
        conn.close()

        print(f"✅ THÀNH CÔNG! File '{OUTPUT_DB}' đã được tạo.")
        print(f"⏱️ Thời gian xử lý: {round(time.time() - start_time, 2)} giây")
        print("👉 BƯỚC TIẾP THEO: Hãy nén file này thành 'bhxh_data.zip' và upload lên GitHub.")

    except Exception as e:
        print(f"❌ LỖI: {e}")
        print("Gợi ý: Nếu lỗi tràn bộ nhớ (Memory Error), hãy thử tắt bớt các phần mềm khác trên máy tính.")

if __name__ == "__main__":
    if os.path.exists(INPUT_FILE):
        convert_data()
    else:
        print(f"❌ Không tìm thấy file '{INPUT_FILE}' trong thư mục này.")
