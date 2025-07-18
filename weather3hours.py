import requests
import xml.etree.ElementTree as ET
import psycopg2


def safe_float(value, default=0.0):
    try:
        return float(value.strip()) if value.strip() else default
    except ValueError:
        return default


# URL ของ API
url = "https://data.tmd.go.th/api/Weather3Hours/V2/?uid=api&ukey=api12345"

# ดึงข้อมูล XML จาก API
response = requests.get(url)
if response.status_code == 200:
    xml_data = response.content
    root = ET.fromstring(xml_data)

    # ดึงข้อมูล Header
    header_element = root.find("Header")
    header_data = {
        "title": header_element.findtext("Title"),
        "description": header_element.findtext("Description"),
        "uri": header_element.findtext("Uri"),
        "last_build_date": header_element.findtext("LastBuildDate"),
        "copyright": header_element.findtext("CopyRight"),
        "generator": header_element.findtext("Generator"),
        "status": header_element.findtext("status"),
    }

    # ดึงข้อมูล Stations
    rows = []
    for station_element in root.findall(".//Station"):
        observation_element = station_element.find("Observation")
        row = {
            **header_data,  # รวมข้อมูล Header
            "wmo_station_number": station_element.findtext("WmoStationNumber"),
            "station_name_thai": station_element.findtext("StationNameThai"),
            "station_name_english": station_element.findtext("StationNameEnglish"),
            "province": station_element.findtext("Province"),
            "latitude": safe_float(station_element.findtext("Latitude", "0.0")),
            "longitude": safe_float(station_element.findtext("Longitude", "0.0")),
            "datetime": observation_element.findtext("DateTime"),
            "station_pressure": safe_float(observation_element.findtext("StationPressure", "0.0")),
            "mean_sea_level_pressure": safe_float(observation_element.findtext("MeanSeaLevelPressure", "0.0")),
            "minimum_temperature": safe_float(observation_element.findtext("MinimumTemperature", "0.0")),
            "air_temperature": safe_float(observation_element.findtext("AirTemperature", "0.0")),
            "dew_point": safe_float(observation_element.findtext("DewPoint", "0.0")),
            "relative_humidity": safe_float(observation_element.findtext("RelativeHumidity", "0.0")),
            "vapor_pressure": safe_float(observation_element.findtext("VaporPressure", "0.0")),
            "land_visibility": safe_float(observation_element.findtext("LandVisibility", "0.0")),
            "wind_direction": safe_float(observation_element.findtext("WindDirection", "0.0")),
            "wind_speed": safe_float(observation_element.findtext("WindSpeed", "0.0")),
            "rainfall": safe_float(observation_element.findtext("Rainfall", "0.0")),
            "rainfall_24hr": safe_float(observation_element.findtext("Rainfall24Hr", "0.0")),
        }
        rows.append(row)

    # เชื่อมต่อ PostgreSQL
    conn = psycopg2.connect(
        dbname="othersource",    # ใส่ชื่อฐานข้อมูลของคุณ
        user="gi.joke",          # ใส่ชื่อผู้ใช้
        password="Tawatcha1@2021",      # ใส่รหัสผ่าน
        host="172.27.154.25",              # ใส่โฮสต์
        port="5432"               # ใส่พอร์ต
    )
    cur = conn.cursor()

    # แทรกข้อมูลทั้งหมด
    insert_query = """
        INSERT INTO tmd.weather3hours (
            title, description, uri, last_build_date, copyright, generator, status,
            wmo_station_number, station_name_thai, station_name_english, province, latitude, longitude, datetime,
            station_pressure, mean_sea_level_pressure, minimum_temperature, air_temperature, dew_point,
            relative_humidity, vapor_pressure, land_visibility, wind_direction, wind_speed, rainfall, rainfall_24hr, geom
        ) VALUES (
            %(title)s, %(description)s, %(uri)s, %(last_build_date)s, %(copyright)s, %(generator)s, %(status)s,
            %(wmo_station_number)s, %(station_name_thai)s, %(station_name_english)s, %(province)s, %(latitude)s, %(longitude)s, %(datetime)s,
            %(station_pressure)s, %(mean_sea_level_pressure)s, %(minimum_temperature)s, %(air_temperature)s, %(dew_point)s,
            %(relative_humidity)s, %(vapor_pressure)s, %(land_visibility)s, %(wind_direction)s, %(wind_speed)s, %(rainfall)s, %(rainfall_24hr)s,
            ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326)
        )
    """
    cur.executemany(insert_query, rows)

    # Commit และปิดการเชื่อมต่อ
    conn.commit()
    cur.close()
    conn.close()

    print("ข้อมูลถูกบันทึกเรียบร้อยแล้ว!")
else:
    print(f"Error fetching data: {response.status_code}")
