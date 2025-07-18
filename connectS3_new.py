import boto3
from botocore.client import Config
import geopandas as gpd
from shapely.geometry import MultiPolygon, Polygon
from shapely.ops import transform
import psycopg2
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# -------------------- Config --------------------
BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
S3_PREFIX = 'Fire/y2025/50_burnt/Level_01/'
LOCAL_DIR = './temp_shp/'
TABLE_NAME = 'disaster.burnt_scar_1day_new'

os.makedirs(LOCAL_DIR, exist_ok=True)

# -------------------- S3 Client (MinIO) --------------------
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('S3_ACCESS_KEY'),
    aws_secret_access_key=os.getenv('S3_SECRET_KEY'),
    endpoint_url=os.getenv('S3_ENDPOINT_URL'),
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

# -------------------- PostgreSQL Connection --------------------
conn = psycopg2.connect(
    dbname=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT')
)
cursor = conn.cursor()

# -------------------- Function: Check file_name --------------------
def file_name_exists(cursor, table_name, file_name):
    query = f"SELECT EXISTS(SELECT 1 FROM {table_name} WHERE file_name = %s)"
    cursor.execute(query, (file_name,))
    return cursor.fetchone()[0]

# -------------------- Function: Insert to table --------------------
def insert_data_to_table(cursor, table_name, data):
    insert_query = f"""
    INSERT INTO {table_name} (
        objectid, sat, date, daynumb, aream2, lat, long, shape_leng, shape_area, geom, file_name
    ) VALUES %s
    """
    execute_values(cursor, insert_query, data)

# -------------------- Function: Convert 3D to 2D --------------------
def to_2d(geom):
    return transform(lambda x, y, *_: (x, y), geom)

# -------------------- Download & Process Each .shp File --------------------
response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=S3_PREFIX)

if 'Contents' not in response:
    print("❌ ไม่พบไฟล์ใน path ที่ระบุ")
else:
    shp_files = set()
    # 1. ดาวน์โหลดไฟล์ทั้งหมดที่เกี่ยวข้องกับ .shp
    for obj in response['Contents']:
        key = obj['Key']
        filename = os.path.basename(key)

        if filename.endswith(('.shp', '.shx', '.dbf', '.prj')):
            local_path = os.path.join(LOCAL_DIR, filename)
            s3_client.download_file(BUCKET_NAME, key, local_path)
            print(f"✅ Downloaded: {filename}")
            if filename.endswith('.shp'):
                shp_files.add(os.path.splitext(filename)[0])

    # 2. ประมวลผลแต่ละชุดไฟล์ .shp
    for shp_base in shp_files:
        file_name = shp_base
        shp_path = os.path.join(LOCAL_DIR, f"{shp_base}.shp")

        if file_name_exists(cursor, TABLE_NAME, file_name):
            print(f"⏭️ Skipping {file_name} (already in DB)")
            continue

        try:
            gdf = gpd.read_file(shp_path)
        except Exception as e:
            print(f"❌ Failed to read {shp_path}: {e}")
            continue

        # กรองเฉพาะคอลัมน์ที่ต้องการ
        try:
            gdf_filtered = gdf[['OBJECTID', 'Sat', 'FireDate', 'DayNumb', 'AreaM2',
                                'lat', 'long', 'Shape_Leng', 'Shape_Area', 'geometry']].copy()
        except KeyError as e:
            print(f"❌ Missing expected columns in {file_name}: {e}")
            continue

        gdf_filtered['file_name'] = file_name

        # แปลง geometry → MultiPolygon + 2D
        gdf_filtered['geometry'] = gdf_filtered['geometry'].apply(
            lambda geom: MultiPolygon([geom]) if isinstance(geom, Polygon) else geom
        )
        gdf_filtered['geometry'] = gdf_filtered['geometry'].apply(to_2d)

        # เตรียมข้อมูลสำหรับ insert
        data_to_insert = [
            (
                row['OBJECTID'],
                row['Sat'],
                row['FireDate'],
                row['DayNumb'],
                row['AreaM2'],
                row['lat'],
                row['long'],
                row['Shape_Leng'],
                row['Shape_Area'],
                f"SRID=4326;{row['geometry'].wkt}",
                row['file_name']
            )
            for _, row in gdf_filtered.iterrows()
        ]

        try:
            insert_data_to_table(cursor, TABLE_NAME, data_to_insert)
            print(f"✅ Inserted data from {file_name}.shp")
        except Exception as e:
            print(f"❌ Insert failed for {file_name}: {e}")
            conn.rollback()

# ปิดการเชื่อมต่อ
conn.commit()
cursor.close()
conn.close()
