import requests
import xml.etree.ElementTree as ET
import psycopg2
from psycopg2.extras import RealDictCursor

# ฟังก์ชันสำหรับการแปลงค่าที่ปลอดภัยจาก string เป็น float


def safe_float(value):
    if value and value.strip():  # ตรวจสอบว่าค่าไม่ใช่ None และไม่เป็นเพียงช่องว่าง
        try:
            return float(value.strip())  # พยายามแปลงเป็น float
        except ValueError:
            return None  # คืนค่า None ถ้าแปลงไม่ได้
    return None  # คืนค่า None ถ้าค่าเป็น None หรือมีแต่ช่องว่าง


# URL ของ API
url = "https://data.tmd.go.th/api/WeatherToday/V2/?uid=api&ukey=api12345"

# ดึงข้อมูล XML จาก API
response = requests.get(url)
if response.status_code == 200:
    xml_data = response.content  # ข้อมูล XML ในรูปแบบ binary
    # แปลง XML เป็น ElementTree
    root = ET.fromstring(xml_data)

    # เชื่อมต่อกับ PostgreSQL
    conn = psycopg2.connect(
        dbname="othersource",   # ชื่อฐานข้อมูล
        user="gi.joke",       # ชื่อผู้ใช้
        password="Tawatcha1@2021",  # รหัสผ่าน
        host="172.27.154.25",       # โฮสต์

    )
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # ดึงข้อมูล Header
header = root.find(".//Header")

header_data = {
    "title": header.findtext("Title"),
    "description": header.findtext("Description"),
    "uri": header.findtext("Uri"),
    "last_build_date": header.findtext("LastBuildDate"),
    "copyright": header.findtext("CopyRight"),
    "generator": header.findtext("Generator"),
    "status": header.findtext("status")
}

# ดึงข้อมูลจาก XML และทำการ INSERT ลงในตารางใหม่
for station in root.findall(".//Station"):
    observation = station.find("Observation")

    combined_data = {
        **header_data,  # รวมข้อมูลจาก Header
        "station_number": station.findtext("WmoStationNumber"),
        "station_name_thai": station.findtext("StationNameThai"),
        "station_name_english": station.findtext("StationNameEnglish"),
        "province": station.findtext("Province"),
        "latitude": safe_float(station.find("Latitude").text),
        "longitude": safe_float(station.find("Longitude").text),
        "observation_datetime": observation.findtext("DateTime"),
        "mean_sea_level_pressure": safe_float(observation.find("MeanSeaLevelPressure").text),
        "temperature": safe_float(observation.find("Temperature").text),
        "max_temperature": safe_float(observation.find("MaxTemperature").text),
        "diff_from_max_temperature": safe_float(observation.find("DifferentFromMaxTemperature").text),
        "min_temperature": safe_float(observation.find("MinTemperature").text),
        "diff_from_min_temperature": safe_float(observation.find("DifferentFromMinTemperature").text),
        "relative_humidity": safe_float(observation.find("RelativeHumidity").text),
        "wind_direction": safe_float(observation.find("WindDirection").text),
        "wind_speed": safe_float(observation.find("WindSpeed").text),
        "rainfall": safe_float(observation.find("Rainfall").text)
    }

    # เตรียมคำสั่ง SQL สำหรับการ INSERT ข้อมูล
    sql = """
    INSERT INTO tmd.weathertoday (
        title, description, uri, last_build_date, copyright, generator, status,
        station_number, station_name_thai, station_name_english, province,
        latitude, longitude, observation_datetime, mean_sea_level_pressure,
        temperature, max_temperature, diff_from_max_temperature, min_temperature,
        diff_from_min_temperature, relative_humidity, wind_direction, wind_speed, rainfall
    ) VALUES (
        %(title)s, %(description)s, %(uri)s, %(last_build_date)s, %(copyright)s,
        %(generator)s, %(status)s, %(station_number)s, %(station_name_thai)s,
        %(station_name_english)s, %(province)s, %(latitude)s, %(longitude)s,
        %(observation_datetime)s, %(mean_sea_level_pressure)s, %(temperature)s,
        %(max_temperature)s, %(diff_from_max_temperature)s, %(min_temperature)s,
        %(diff_from_min_temperature)s, %(relative_humidity)s, %(wind_direction)s,
        %(wind_speed)s, %(rainfall)s
    )
    ON CONFLICT (station_number, observation_datetime)
    DO NOTHING;
    """

    # ทำการ INSERT ข้อมูล
    cur.execute(sql, combined_data)

    # Commit การเปลี่ยนแปลง
conn.commit()

# Debug: แสดงข้อมูลที่กำลังจะ Insert
print("Inserted data:", combined_data)